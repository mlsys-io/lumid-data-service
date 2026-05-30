//! Blob serving (read side) — port of `api/routes/blobs.py`.
//!
//! GET /blobs/{key:path}
//!   Serve the bytes at `<settings.blob_root>/{key}`. Content-Type is read from
//!   `raw.blobs.content_type` (DB-truth, not file-extension guessing), falling
//!   back to `application/octet-stream`. 404 if the file is missing; 400 on any
//!   path-traversal attempt that escapes blob_root; 503 if blob_root unset.
//!
//! GET /storage/v1/object/findata/{path:path}
//!   Back-compat alias — 302-redirects to `/blobs/{path}` so legacy
//!   `raw.blobs.storage_url` values stay resolvable after the migration.

use std::path::{Component, Path as FsPath};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use object_store::{path::Path as ObjPath, Error as ObjError};

use crate::error::{ApiError, ApiResult};
use crate::queries::blobs as q;
use crate::state::AppState;

/// Lexically validate a blob key, rejecting any traversal (`..`, absolute or
/// prefix components) and normalizing the remaining `/`-separated segments.
/// Object keys are opaque (no filesystem resolution), so this is a pure
/// sanitization step — the FS-specific `canonicalize` symlink guard is gone.
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
