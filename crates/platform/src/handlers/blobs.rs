//! Blob serving (read side) — port of `api/routes/blobs.py`.
//!
//! GET /blobs/{key:path}
//!   Serve the bytes at `<settings.blob_root>/{key}`. Content-Type is read from
//!   `raw.blobs.content_type` (DB-truth, not file-extension guessing), falling
//!   back to `application/octet-stream`. 404 if the file is missing; 400 on any
//!   path-traversal attempt that escapes blob_root; 503 if blob_root unset.
//!
//! GET /blobs?prefix=…
//!   List objects by prefix (flat or delimiter-bounded). See `list_blobs`.
//!
//! `legacy_storage_alias` — a generic blob-by-path handler that 302-redirects
//!   to `/blobs/{path}`. The platform names no path for it; an app mounts it at
//!   whatever legacy/compat URL it needs (e.g. an old `/storage/v1/object/<x>/{path}`
//!   route) so pre-migration `storage_url` values keep resolving.

use std::path::{Component, Path as FsPath};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjPath, Error as ObjError};
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::queries::blobs as q;
use crate::state::AppState;

/// Public wrapper around `sanitize_key` that returns an `ApiError::BadRequest`
/// for traversal/empty keys instead of `None`. Used by the retrieval pipeline's
/// `storage_get` op, which needs the same path-traversal guard the HTTP handler applies.
pub fn sanitize_blob_key(key: &str) -> Result<String, ApiError> {
    sanitize_key(key).ok_or_else(|| ApiError::BadRequest("invalid key".into()))
}

/// Lexically validate a blob key, rejecting any traversal (`..`, absolute or
/// prefix components) and normalizing the remaining `/`-separated segments.
/// Object keys are opaque (no filesystem resolution), so this is a pure
/// sanitization step — the FS-specific `canonicalize` symlink guard is gone.
/// Returns `None` for traversal attempts AND for empty keys (fetch requires a key).
fn sanitize_key(key: &str) -> Option<String> {
    let mut parts: Vec<&str> = Vec::new();
    for comp in FsPath::new(key).components() {
        match comp {
            Component::Normal(c) => parts.push(c.to_str()?),
            Component::CurDir => {}
            // Any `..`, root `/`, or `C:`-style prefix is a traversal attempt.
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// Validate a listing prefix: same traversal rules as `sanitize_key`, but an
/// empty prefix is allowed (means "list from root").
pub fn sanitize_prefix(prefix: &str) -> Result<Option<ObjPath>, ApiError> {
    if prefix.is_empty() {
        return Ok(None);
    }
    let clean =
        sanitize_key(prefix).ok_or_else(|| ApiError::BadRequest("invalid prefix".into()))?;
    Ok(Some(ObjPath::from(clean.as_str())))
}

/// Cap `limit` to [1, 10000]; absent → 1000.
pub fn clamp_limit(opt: Option<usize>) -> usize {
    match opt {
        None => 1000,
        Some(n) => n.clamp(1, 10_000),
    }
}

// ── query params ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ListParams {
    pub prefix: Option<String>,
    /// When present (any non-empty value), use `list_with_delimiter` for
    /// folder-style results instead of a flat recursive listing.
    pub delimiter: Option<String>,
    pub limit: Option<usize>,
}

// ── response types ───────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct BlobItem {
    pub key: String,
    pub size: usize,
    pub last_modified: String, // RFC 3339
}

#[derive(Serialize)]
pub struct ListBlobsResponse {
    pub objects: Vec<BlobItem>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
}

// ── retrieval-prefix filter ───────────────────────────────────────────────────

/// Returns `true` when `key` falls under the retrieval-output prefix and should
/// be hidden from general listing results.
///
/// Security: don't enumerate other runs' materialized outputs via `GET /blobs`.
/// Fetch-by-key (`GET /blobs/<key>`) intentionally stays open — run IDs are
/// unguessable UUIDs and are only handed to the run's own requester.
///
/// Matching is path-segment–bounded so `retrievals-archive/x` is NOT hidden
/// when `retrieval_prefix` is `"retrievals"`.
pub fn is_hidden_key(key: &str, retrieval_prefix: &str) -> bool {
    if retrieval_prefix.is_empty() {
        return false;
    }
    // Exact match OR starts with "<prefix>/"
    key == retrieval_prefix || key.starts_with(&format!("{retrieval_prefix}/"))
}

/// Returns `true` when a `common_prefixes` entry should be hidden.
///
/// object_store may return the prefix with or without a trailing `/`; we
/// normalise by stripping it before comparing.
///
/// Security: a delimiter-based listing for `?prefix=retrievals&delimiter=/`
/// returns `common_prefixes` like `retrievals/<run_id>/`, which are nested UNDER
/// the retrieval namespace — not equal to it.  Matching only the exact prefix
/// would leak run IDs.  We therefore hide anything that equals OR is nested
/// under the retrieval prefix (segment-bounded to avoid hiding siblings like
/// `retrievals-archive/`).
pub fn is_hidden_prefix(common_prefix: &str, retrieval_prefix: &str) -> bool {
    if retrieval_prefix.is_empty() {
        return false;
    }
    let norm = common_prefix.trim_end_matches('/');
    // Hide the retrieval prefix itself AND any run-ID sub-prefixes beneath it,
    // but NOT segment siblings like "retrievals-archive/".
    norm == retrieval_prefix || norm.starts_with(&format!("{retrieval_prefix}/"))
}

// ── handler ──────────────────────────────────────────────────────────────────

pub async fn list_blobs(
    State(st): State<AppState>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<ListBlobsResponse>> {
    if st.settings.blob_root.is_empty() {
        return Err(ApiError::Unavailable("blob storage not configured".into()));
    }

    let prefix_path =
        sanitize_prefix(params.prefix.as_deref().unwrap_or(""))?;
    let limit = clamp_limit(params.limit);
    let use_delimiter = params.delimiter.as_deref().is_some_and(|d| !d.is_empty());

    if use_delimiter {
        let result = st
            .blob_store
            .list_with_delimiter(prefix_path.as_ref())
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("blob list: {e}")))?;

        // Filter hidden prefixes BEFORE limit/truncated accounting so the
        // visible count is correct.
        let rp = &st.settings.retrieval_prefix;
        let visible_objects: Vec<BlobItem> = result
            .objects
            .into_iter()
            .filter(|m| !is_hidden_key(&m.location.to_string(), rp))
            .map(|m| BlobItem {
                key: m.location.to_string(),
                size: m.size,
                last_modified: m.last_modified.to_rfc3339(),
            })
            .collect();
        let visible_prefixes: Vec<String> = result
            .common_prefixes
            .into_iter()
            .map(|p| p.to_string())
            .filter(|p| !is_hidden_prefix(p, rp))
            .collect();

        let truncated = visible_objects.len() > limit || visible_prefixes.len() > limit;
        let objects: Vec<BlobItem> = visible_objects.into_iter().take(limit).collect();
        let common_prefixes: Vec<String> = visible_prefixes.into_iter().take(limit).collect();

        Ok(Json(ListBlobsResponse {
            objects,
            common_prefixes,
            truncated,
        }))
    } else {
        // Flat recursive listing — filter hidden keys, then collect up to
        // limit+1 to detect truncation.
        let rp = &st.settings.retrieval_prefix;
        let mut objects: Vec<BlobItem> = Vec::with_capacity(limit + 1);
        let mut stream = st.blob_store.list(prefix_path.as_ref());
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("blob list: {e}")))?
        {
            let key = meta.location.to_string();
            // Security: hide retrieval-output prefix from enumeration.
            if is_hidden_key(&key, rp) {
                continue;
            }
            objects.push(BlobItem {
                key,
                size: meta.size,
                last_modified: meta.last_modified.to_rfc3339(),
            });
            if objects.len() > limit {
                break;
            }
        }
        let truncated = objects.len() > limit;
        if truncated {
            objects.truncate(limit);
        }
        Ok(Json(ListBlobsResponse {
            objects,
            common_prefixes: vec![],
            truncated,
        }))
    }
}

pub async fn serve_blob(
    State(st): State<AppState>,
    Path(key): Path<String>,
) -> ApiResult<Response> {
    if st.settings.blob_root.is_empty() {
        return Err(ApiError::Unavailable("blob storage not configured".into()));
    }
    let key = sanitize_key(&key).ok_or_else(|| ApiError::BadRequest("invalid key".into()))?;

    // Fetch from the object store (localfs default, or S3/MinIO).
    let got = match st.blob_store.get(&ObjPath::from(key.as_str())).await {
        Ok(r) => r,
        Err(ObjError::NotFound { .. }) => return Err(ApiError::NotFound("blob not found".into())),
        Err(e) => return Err(ApiError::Internal(anyhow::anyhow!("blob get: {e}"))),
    };

    let ct = q::content_type_for_key(&st.pool, &key)
        .await?
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Stream rather than buffering (blobs can be up to 100 MB).
    let resp = (
        [(header::CONTENT_TYPE, ct)],
        Body::from_stream(got.into_stream()),
    )
        .into_response();
    Ok(resp)
}

/// Back-compat alias → 302 redirect to `/blobs/{path}`.
pub async fn legacy_storage_alias(Path(path): Path<String>) -> Response {
    let location = format!("/blobs/{path}");
    (
        StatusCode::FOUND,
        [(header::LOCATION, location)],
    )
        .into_response()
}
