//! Tenant-private blobs — `POST /me/blobs`, `GET /me/blobs`, `GET /me/blobs/{sha256}`.
//!
//! A user uploads a binary once (a model, a calibration `.npz`, a parquet
//! file) and references it from later work — a FlowMesh Python node, a
//! notebook — by its sha256. Unlike `POST /ingest/blob`, the object is the
//! CALLER's:
//!
//! * Stored under `tenants/<sub>/sha256=<hex>` and recorded in
//!   `raw.tenant_blobs` keyed `(owner_sub, blob_sha256)`. `raw.blobs` is keyed
//!   on sha256 ALONE, so a private upload deduped against it would hand one
//!   tenant's key to another tenant uploading the same bytes; this table
//!   dedups within a tenant only.
//! * Readable only by its owner (or an admin / local key): here by sha256, and
//!   through `GET /blobs/tenants/<sub>/…`, which `blobs::serve_blob` gates on
//!   the same rule. `GET /blobs` listing hides the `tenants/` prefix from
//!   everyone else.
//! * Never federated: `/me/blobs` and `/blobs/tenants/` are local-only in
//!   shadow mode (`federation::is_local_only`). The shadow's forward cache is
//!   keyed by path, not caller, so forwarding a `/me/*` read could serve one
//!   tenant's response to another; and the bytes live where they were written.

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::Response;
use axum::{Extension, Json};
use deadpool_postgres::Pool;
use object_store::{path::Path as ObjPath, Attribute, Attributes, Error as ObjError};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// Key prefix every tenant-private object lives under.
pub const TENANT_PREFIX: &str = "tenants";

/// Idempotent DDL, applied at boot when blob storage is configured.
pub const TENANT_BLOBS_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.tenant_blobs (
  owner_sub       text        NOT NULL,
  blob_sha256     text        NOT NULL,
  storage_key     text        NOT NULL,
  content_type    text        NOT NULL,
  size_bytes      bigint      NOT NULL,
  suggested_name  text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_sub, blob_sha256)
);";

/// Create `raw.tenant_blobs` (idempotent).
pub async fn migrate(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client.batch_execute(TENANT_BLOBS_DDL).await?;
    Ok(())
}

/// A `sub` is used verbatim as a key segment, so it must be one. lum.id subs
/// are UUIDs; anything with a separator or traversal shape is refused rather
/// than escaped.
fn owner_segment(sub: &str) -> Option<&str> {
    let ok = !sub.is_empty()
        && sub.len() <= 128
        && sub
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    ok.then_some(sub)
}

/// The object key for `owner`'s copy of `sha` (hex).
pub fn tenant_key(owner: &str, sha: &str) -> String {
    format!("{TENANT_PREFIX}/{owner}/sha256={sha}")
}

/// `Some(owner)` when `key` is a tenant-private object key.
pub fn tenant_owner_of(key: &str) -> Option<&str> {
    let rest = key.strip_prefix(TENANT_PREFIX)?.strip_prefix('/')?;
    let owner = rest.split('/').next()?;
    (!owner.is_empty()).then_some(owner)
}

/// Admins and local keys may read any tenant's blobs; everyone else only their own.
pub fn may_read_tenant(identity: &Identity, owner: &str) -> bool {
    identity.role == "local"
        || matches!(identity.role.as_str(), "admin" | "super_admin")
        || identity.sub == owner
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

#[derive(Serialize)]
pub struct TenantBlob {
    pub blob_sha256: String,
    /// Stable reference for later use (e.g. a FlowMesh `blob` input).
    pub uri: String,
    pub storage_key: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub suggested_name: Option<String>,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct UploadResponse {
    #[serde(flatten)]
    pub blob: TenantBlob,
    pub already_existed: bool,
}

fn uri_for(sha: &str) -> String {
    format!("lumid://blob/sha256/{sha}")
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// POST /me/blobs — raw body; `Content-Type` and optional `X-Filename`.
pub async fn upload(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<UploadResponse>> {
    if st.settings.blob_root.is_empty() {
        return Err(ApiError::Unavailable("blob storage not configured".into()));
    }
    let owner = owner_segment(&identity.sub)
        .ok_or_else(|| ApiError::Forbidden("this credential has no usable owner id".into()))?
        .to_string();
    if body.is_empty() {
        return Err(ApiError::BadRequest(
            "empty body — no bytes to store".into(),
        ));
    }
    if (body.len() as u64) > st.settings.blob_max_bytes {
        return Err(ApiError::BadRequest(format!(
            "blob too large ({} > {} bytes)",
            body.len(),
            st.settings.blob_max_bytes
        )));
    }
    let sha = hex::encode(Sha256::digest(&body));
    let content_type = header_str(&headers, "content-type")
        .and_then(crate::handlers::blobs::valid_content_type)
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let name = header_str(&headers, "x-filename").map(|s| s.chars().take(255).collect::<String>());
    let key = tenant_key(&owner, &sha);

    let client = st.pool.get().await?;
    if let Some(row) = client
        .query_opt(
            "SELECT storage_key, content_type, size_bytes, suggested_name, created_at::text \
               FROM raw.tenant_blobs WHERE owner_sub = $1 AND blob_sha256 = $2",
            &[&owner, &sha],
        )
        .await?
    {
        return Ok(Json(UploadResponse {
            blob: TenantBlob {
                uri: uri_for(&sha),
                blob_sha256: sha,
                storage_key: row.get(0),
                content_type: row.get(1),
                size_bytes: row.get(2),
                suggested_name: row.get(3),
                created_at: row.get(4),
            },
            already_existed: true,
        }));
    }

    let size = body.len() as i64;
    let attrs = Attributes::from_iter([(Attribute::ContentType, content_type.clone())]);
    crate::objstore::put_with_optional_attrs(
        &st.blob_store,
        &ObjPath::from(key.as_str()),
        body,
        attrs,
    )
    .await
    .map_err(|e| ApiError::Internal(anyhow::anyhow!("tenant blob put: {e}")))?;

    // ON CONFLICT: a concurrent identical upload by the same owner wrote the
    // same bytes to the same key — keep whichever row landed first.
    let row = client
        .query_one(
            "INSERT INTO raw.tenant_blobs \
               (owner_sub, blob_sha256, storage_key, content_type, size_bytes, suggested_name) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (owner_sub, blob_sha256) DO UPDATE SET owner_sub = EXCLUDED.owner_sub \
             RETURNING storage_key, content_type, size_bytes, suggested_name, created_at::text",
            &[&owner, &sha, &key, &content_type, &size, &name],
        )
        .await?;
    Ok(Json(UploadResponse {
        blob: TenantBlob {
            uri: uri_for(&sha),
            blob_sha256: sha,
            storage_key: row.get(0),
            content_type: row.get(1),
            size_bytes: row.get(2),
            suggested_name: row.get(3),
            created_at: row.get(4),
        },
        already_existed: false,
    }))
}

#[derive(Serialize)]
pub struct ListResponse {
    pub blobs: Vec<TenantBlob>,
}

/// GET /me/blobs — the caller's uploads, newest first.
pub async fn list(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<ListResponse>> {
    let owner = owner_segment(&identity.sub)
        .ok_or_else(|| ApiError::Forbidden("this credential has no usable owner id".into()))?;
    let client = st.pool.get().await?;
    let rows = client
        .query(
            "SELECT blob_sha256, storage_key, content_type, size_bytes, suggested_name, created_at::text \
               FROM raw.tenant_blobs WHERE owner_sub = $1 ORDER BY created_at DESC LIMIT 1000",
            &[&owner],
        )
        .await?;
    let blobs = rows
        .iter()
        .map(|r| {
            let sha: String = r.get(0);
            TenantBlob {
                uri: uri_for(&sha),
                blob_sha256: sha,
                storage_key: r.get(1),
                content_type: r.get(2),
                size_bytes: r.get(3),
                suggested_name: r.get(4),
                created_at: r.get(5),
            }
        })
        .collect();
    Ok(Json(ListResponse { blobs }))
}

/// GET /me/blobs/{sha256} — the caller's own copy, by content hash. 404 (not
/// 403) for a hash the caller never uploaded, so a probe cannot learn that
/// another tenant holds those bytes.
pub async fn fetch(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(sha): Path<String>,
) -> ApiResult<Response> {
    let owner = owner_segment(&identity.sub)
        .ok_or_else(|| ApiError::Forbidden("this credential has no usable owner id".into()))?;
    let sha = sha.to_ascii_lowercase();
    if !is_sha256_hex(&sha) {
        return Err(ApiError::BadRequest("expected a 64-char hex sha256".into()));
    }
    let client = st.pool.get().await?;
    let row = client
        .query_opt(
            "SELECT storage_key, content_type, suggested_name FROM raw.tenant_blobs \
              WHERE owner_sub = $1 AND blob_sha256 = $2",
            &[&owner, &sha],
        )
        .await?
        .ok_or_else(|| ApiError::NotFound("no such blob for this account".into()))?;
    let key: String = row.get(0);
    let ct: String = row.get(1);
    let got = match st.blob_store.get(&ObjPath::from(key.as_str())).await {
        Ok(r) => r,
        Err(ObjError::NotFound { .. }) => {
            return Err(ApiError::NotFound("blob bytes missing".into()))
        }
        Err(e) => return Err(ApiError::Internal(anyhow::anyhow!("tenant blob get: {e}"))),
    };
    // Always a download: these are user-supplied bytes served from our origin.
    Response::builder()
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&ct)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        )
        .header(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        )
        .header(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment"),
        )
        .header(
            "x-blob-sha256",
            HeaderValue::from_str(&sha).unwrap_or_else(|_| HeaderValue::from_static("")),
        )
        .body(Body::from_stream(got.into_stream()))
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("build blob response: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(sub: &str, role: &str) -> Identity {
        Identity {
            sub: sub.into(),
            role: role.into(),
            email: None,
            active: true,
            scopes: vec![],
        }
    }

    #[test]
    fn owner_segment_accepts_ids_refuses_paths() {
        assert_eq!(
            owner_segment("fc61eecf-5766-46af-a2d3-e9016b4b3f7b"),
            Some("fc61eecf-5766-46af-a2d3-e9016b4b3f7b")
        );
        for bad in ["", "a/b", "..", "a.b", "a b", "x\0"] {
            assert_eq!(owner_segment(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn tenant_key_roundtrips_owner() {
        let k = tenant_key("u-1", &"a".repeat(64));
        assert_eq!(k, format!("tenants/u-1/sha256={}", "a".repeat(64)));
        assert_eq!(tenant_owner_of(&k), Some("u-1"));
        assert_eq!(tenant_owner_of("tenantsX/u-1/x"), None);
        assert_eq!(tenant_owner_of("blob/sha256=abc"), None);
        assert_eq!(tenant_owner_of("tenants/"), None);
    }

    #[test]
    fn read_rule_owner_or_admin_only() {
        assert!(may_read_tenant(&ident("u-1", "user"), "u-1"));
        assert!(!may_read_tenant(&ident("u-2", "user"), "u-1"));
        assert!(may_read_tenant(&ident("x", "admin"), "u-1"));
        assert!(may_read_tenant(&ident("x", "super_admin"), "u-1"));
        assert!(may_read_tenant(&ident("x", "local"), "u-1"));
    }

    #[test]
    fn sha_validation() {
        assert!(is_sha256_hex(&"0f".repeat(32)));
        assert!(!is_sha256_hex(&"0F".repeat(32)));
        assert!(!is_sha256_hex("abc"));
    }
}
