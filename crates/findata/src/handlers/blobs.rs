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

use std::path::{Component, Path as FsPath, PathBuf};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::{ApiError, ApiResult};
use crate::queries::blobs as q;
use crate::state::AppState;

/// Resolve `root/key` and confine it to `root`, rejecting any traversal
/// (`..`, absolute components). Mirrors the Python `os.path.realpath` guard
/// without touching the filesystem (lexical normalization).
fn safe_join(root: &str, key: &str) -> Option<PathBuf> {
    let root = FsPath::new(root);
    let mut out = root.to_path_buf();
    for comp in FsPath::new(key).components() {
        match comp {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            // Any `..`, root `/`, or `C:`-style prefix is a traversal attempt.
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    // Belt-and-suspenders: the result must still live under root.
    if out.starts_with(root) {
        Some(out)
    } else {
        None
    }
}

pub async fn serve_blob(
    State(st): State<AppState>,
    Path(key): Path<String>,
) -> ApiResult<Response> {
    if st.settings.blob_root.is_empty() {
        return Err(ApiError::Unavailable("blob storage not configured".into()));
    }
    let abs_path = safe_join(&st.settings.blob_root, &key)
        .ok_or_else(|| ApiError::BadRequest("invalid key".into()))?;

    let meta = tokio::fs::metadata(&abs_path).await;
    let is_file = matches!(meta, Ok(m) if m.is_file());
    if !is_file {
        return Err(ApiError::NotFound("blob not found".into()));
    }

    let bytes = tokio::fs::read(&abs_path)
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))?;

    let ct = q::content_type_for_key(&st.pool, &key)
        .await?
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let resp = (
        [(header::CONTENT_TYPE, ct)],
        Body::from(bytes),
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
