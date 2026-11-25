//! Scope `blobs`: small tenant blobs copy between hubs when missing.
//!
//! Content-addressed: a blob is `(owner_sub, sha256)`, stored under
//! `tenants/<owner>/sha256=<hex>` on both hubs, and the receiver re-hashes the
//! bytes before storing. The `raw.tenant_blobs` index row moves WITH the bytes
//! (written only after the bytes landed), carrying `home_hub` = the hub the
//! upload happened on. Only blobs of at most `LUMID_HUB_SMALL_MAX` bytes ever
//! copy, in either direction; a larger blob stays where it was written.
//!
//! Both directions are driven from the entrance hub: it pushes its own blobs
//! (`POST /sync/blobs/have` → `PUT /sync/blobs/:owner/:sha` for the missing)
//! and pulls the peer's (`GET /sync/outbox?scope=blobs` → `GET
//! /sync/blobs/:owner/:sha` for what it lacks).

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::Response;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use object_store::{path::Path as ObjPath, Attribute, Attributes};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::handlers::me_blobs::tenant_key;
use crate::state::AppState;

use super::registry::Cursor;
use super::{record, require_hub_peer};

/// Cursor key the blob scope uses inside a [`Cursor`].
pub const CURSOR_KEY: &str = "raw.tenant_blobs";

/// One index row on the wire.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlobIndexRow {
    pub owner_sub: String,
    pub blob_sha256: String,
    pub content_type: String,
    pub size_bytes: i64,
    #[serde(default)]
    pub suggested_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub home_hub: String,
}

pub(crate) fn owner_ok(sub: &str) -> bool {
    !sub.is_empty()
        && sub.len() <= 128
        && sub
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub(crate) fn sha_ok(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Blob sync is on when blob storage is configured and the cap is non-zero.
pub fn enabled(st: &AppState) -> bool {
    !st.settings.blob_root.is_empty() && st.settings.hub_small_max > 0
}

fn cap(st: &AppState) -> i64 {
    st.settings.hub_small_max.min(i64::MAX as u64) as i64
}

/// This hub's own blobs (≤ cap) past the cursor, oldest first.
pub async fn collect_index(
    st: &AppState,
    cursor: &Cursor,
    limit: i64,
) -> ApiResult<(Vec<BlobIndexRow>, Cursor, bool)> {
    let mut next = cursor.clone();
    if !enabled(st) {
        return Ok((Vec::new(), next, false));
    }
    let (since, key) = match cursor.get(CURSOR_KEY) {
        Some((ts, k)) => (Some(*ts), Some(k.clone())),
        None => (None, None),
    };
    let c = st.pool.get().await?;
    let rows = c
        .query(
            "SELECT owner_sub, blob_sha256, content_type, size_bytes, suggested_name, created_at,
                    owner_sub || '/' || blob_sha256 AS k
               FROM raw.tenant_blobs t
              WHERE COALESCE(t.home_hub, $1) = $1 AND t.size_bytes <= $5
                AND ($2::timestamptz IS NULL
                     OR (t.created_at, t.owner_sub || '/' || t.blob_sha256) > ($2::timestamptz, $3::text))
              ORDER BY t.created_at, t.owner_sub || '/' || t.blob_sha256 LIMIT $4",
            &[&st.settings.hub_id, &since, &key, &limit, &cap(st)],
        )
        .await?;
    let more = rows.len() as i64 >= limit;
    if let Some(last) = rows.last() {
        next.insert(CURSOR_KEY.into(), (last.get("created_at"), last.get("k")));
    }
    let out = rows
        .iter()
        .map(|r| BlobIndexRow {
            owner_sub: r.get("owner_sub"),
            blob_sha256: r.get("blob_sha256"),
            content_type: r.get("content_type"),
            size_bytes: r.get("size_bytes"),
            suggested_name: r.get("suggested_name"),
            created_at: r.get("created_at"),
            home_hub: st.settings.hub_id.clone(),
        })
        .collect();
    Ok((out, next, more))
}

#[derive(Deserialize, Serialize)]
pub struct HaveReq {
    pub items: Vec<BlobKey>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlobKey {
    pub owner_sub: String,
    pub blob_sha256: String,
}

#[derive(Deserialize, Serialize)]
pub struct HaveResp {
    pub missing: Vec<BlobKey>,
}

/// Which of `items` this hub has no index row for.
pub async fn missing_of(st: &AppState, items: &[BlobKey]) -> ApiResult<Vec<BlobKey>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let owners: Vec<&str> = items.iter().map(|i| i.owner_sub.as_str()).collect();
    let shas: Vec<&str> = items.iter().map(|i| i.blob_sha256.as_str()).collect();
    let c = st.pool.get().await?;
    let have: std::collections::HashSet<(String, String)> = c
        .query(
            "SELECT t.owner_sub, t.blob_sha256 FROM raw.tenant_blobs t
               JOIN unnest($1::text[], $2::text[]) AS k(o, s)
                 ON t.owner_sub = k.o AND t.blob_sha256 = k.s",
            &[&owners, &shas],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    Ok(items
        .iter()
        .filter(|i| !have.contains(&(i.owner_sub.clone(), i.blob_sha256.clone())))
        .cloned()
        .collect())
}

/// `POST /sync/blobs/have` — the peer asks which of its blobs we lack.
pub async fn post_have(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(req): Json<HaveReq>,
) -> ApiResult<Json<HaveResp>> {
    require_hub_peer(&st.settings, &identity)?;
    if !enabled(&st) {
        return Err(ApiError::Unavailable("blob sync is off on this hub".into()));
    }
    if req.items.len() > 5000 {
        return Err(ApiError::BadRequest("at most 5000 items".into()));
    }
    if req
        .items
        .iter()
        .any(|i| !owner_ok(&i.owner_sub) || !sha_ok(&i.blob_sha256))
    {
        return Err(ApiError::BadRequest("bad owner_sub or blob_sha256".into()));
    }
    Ok(Json(HaveResp {
        missing: missing_of(&st, &req.items).await?,
    }))
}

/// Store verified bytes + the index row. `Ok(true)` when newly stored.
/// Shared by the inbound PUT and the pull side.
pub async fn store(st: &AppState, row: &BlobIndexRow, body: Bytes) -> ApiResult<bool> {
    if !owner_ok(&row.owner_sub) || !sha_ok(&row.blob_sha256) {
        return Err(ApiError::BadRequest("bad owner_sub or blob_sha256".into()));
    }
    if body.len() as i64 > cap(st) || row.size_bytes > cap(st) {
        return Err(ApiError::BadRequest(format!(
            "blob larger than LUMID_HUB_SMALL_MAX ({} bytes)",
            st.settings.hub_small_max
        )));
    }
    if body.len() as i64 != row.size_bytes {
        return Err(ApiError::BadRequest(
            "size does not match the index row".into(),
        ));
    }
    let sha = hex::encode(Sha256::digest(&body));
    if sha != row.blob_sha256 {
        return Err(ApiError::BadRequest(
            "sha256 does not match the bytes".into(),
        ));
    }
    let c = st.pool.get().await?;
    if c.query_opt(
        "SELECT 1 FROM raw.tenant_blobs WHERE owner_sub = $1 AND blob_sha256 = $2",
        &[&row.owner_sub, &row.blob_sha256],
    )
    .await?
    .is_some()
    {
        return Ok(false);
    }
    let key = tenant_key(&row.owner_sub, &row.blob_sha256);
    let content_type = crate::handlers::blobs::valid_content_type(&row.content_type)
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let attrs = Attributes::from_iter([(Attribute::ContentType, content_type.clone())]);
    crate::objstore::put_with_optional_attrs(
        &st.blob_store,
        &ObjPath::from(key.as_str()),
        body,
        attrs,
    )
    .await
    .map_err(|e| ApiError::Internal(anyhow::anyhow!("hub blob put: {e}")))?;
    let n = c
        .execute(
            "INSERT INTO raw.tenant_blobs
               (owner_sub, blob_sha256, storage_key, content_type, size_bytes, suggested_name,
                created_at, home_hub)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (owner_sub, blob_sha256) DO NOTHING",
            &[
                &row.owner_sub,
                &row.blob_sha256,
                &key,
                &content_type,
                &row.size_bytes,
                &row.suggested_name,
                &row.created_at,
                &row.home_hub,
            ],
        )
        .await?;
    Ok(n == 1)
}

/// The index row rides in one header, hex-encoded JSON (the name may be
/// non-ASCII, which a raw header cannot carry).
pub const INDEX_HEADER: &str = "x-lumid-blob-index";

/// `PUT /sync/blobs/:owner/:sha` — receive one of the peer's blobs.
pub async fn put_blob(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((owner, sha)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<serde_json::Value>> {
    let peer = require_hub_peer(&st.settings, &identity)?;
    if !enabled(&st) {
        return Err(ApiError::Unavailable("blob sync is off on this hub".into()));
    }
    let row: BlobIndexRow = headers
        .get(INDEX_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| hex::decode(h).ok())
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| ApiError::BadRequest(format!("missing or bad {INDEX_HEADER}")))?;
    if row.owner_sub != owner || row.blob_sha256 != sha {
        return Err(ApiError::BadRequest(
            "index row does not match the path".into(),
        ));
    }
    if row.home_hub != peer {
        return Err(ApiError::Forbidden(
            "a peer sends only blobs uploaded on it".into(),
        ));
    }
    match store(&st, &row, body).await {
        Ok(stored) => {
            record(&st.pool, &peer, "blobs", "inbound", Ok(stored as i64), None).await;
            Ok(Json(serde_json::json!({"stored": stored})))
        }
        Err(e) => {
            record(
                &st.pool,
                &peer,
                "blobs",
                "inbound",
                Err(&e.to_string()),
                None,
            )
            .await;
            Err(e)
        }
    }
}

/// `GET /sync/blobs/:owner/:sha` — serve one of THIS hub's own small blobs.
pub async fn get_blob(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((owner, sha)): Path<(String, String)>,
) -> ApiResult<Response> {
    require_hub_peer(&st.settings, &identity)?;
    if !enabled(&st) {
        return Err(ApiError::Unavailable("blob sync is off on this hub".into()));
    }
    if !owner_ok(&owner) || !sha_ok(&sha) {
        return Err(ApiError::BadRequest("bad owner_sub or blob_sha256".into()));
    }
    let c = st.pool.get().await?;
    let r = c
        .query_opt(
            "SELECT storage_key, content_type, size_bytes FROM raw.tenant_blobs
              WHERE owner_sub = $1 AND blob_sha256 = $2
                AND COALESCE(home_hub, $3) = $3 AND size_bytes <= $4",
            &[&owner, &sha, &st.settings.hub_id, &cap(&st)],
        )
        .await?
        .ok_or_else(|| ApiError::NotFound("no such small blob owned by this hub".into()))?;
    let key: String = r.get(0);
    let ct: String = r.get(1);
    let got = st
        .blob_store
        .get(&ObjPath::from(key.as_str()))
        .await
        .map_err(|e| ApiError::NotFound(format!("blob bytes: {e}")))?;
    Response::builder()
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&ct)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        )
        .body(Body::from_stream(got.into_stream()))
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("build blob response: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validators() {
        assert!(owner_ok("fc61eecf-5766-46af-a2d3-e9016b4b3f7b"));
        assert!(!owner_ok("a/b"));
        assert!(!owner_ok(""));
        assert!(sha_ok(&"ab".repeat(32)));
        assert!(!sha_ok(&"AB".repeat(32)));
        assert!(!sha_ok("abc"));
    }

    #[test]
    fn index_row_roundtrips_through_the_header_encoding() {
        let row = BlobIndexRow {
            owner_sub: "u1".into(),
            blob_sha256: "ab".repeat(32),
            content_type: "application/octet-stream".into(),
            size_bytes: 3,
            suggested_name: Some("données.npz".into()),
            created_at: Utc::now(),
            home_hub: "home".into(),
        };
        let h = hex::encode(serde_json::to_vec(&row).unwrap());
        assert!(HeaderValue::from_str(&h).is_ok());
        let back: BlobIndexRow = serde_json::from_slice(&hex::decode(h).unwrap()).unwrap();
        assert_eq!(back, row);
    }
}
