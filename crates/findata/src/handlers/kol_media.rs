//! KOL media serving + metadata — port of `api/routes/kol_media.py`.
//!
//! The bulk downloader mirrors Twitter CDN images to disk under
//! `settings.kol_media_root` (`img/<aa>/<filename>`). This module exposes:
//!
//!   * `GET /kols/media`          — info/status JSON (whether serving is enabled,
//!     the configured root, etc.).
//!   * `GET /kols/media/by-url`   — resolve a Twitter CDN URL to its local mirror
//!     path and **302-redirect** there if cached, else 302 back to the original
//!     CDN URL so callers always get one fetchable URL.
//!   * `GET /kols/media/{rel}`    — serve the mirrored file directly from disk
//!     (`kol_media_root + rel`) with a path-traversal guard. Empty root → disabled
//!     (404), mirroring the Python StaticFiles mount being skipped.

use std::path::{Component, Path as FsPath, PathBuf};

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// `GET /kols/media` — status/info JSON. Reports whether serving is enabled and
/// whether the configured root directory exists on disk.
pub async fn info(State(st): State<AppState>) -> ApiResult<Json<Value>> {
    let root = &st.settings.kol_media_root;
    let configured = !root.is_empty();
    let root_exists = configured && FsPath::new(root).is_dir();
    let enabled = configured && root_exists;
    Ok(Json(json!({
        "enabled": enabled,
        "root": root,
        "root_configured": configured,
        "root_exists": root_exists,
        "by_url": "/kols/media/by-url?u=<twitter-cdn-url>",
        "serve_path": "/kols/media/<rel>",
    })))
}

/// Map a `https://pbs.twimg.com/...` URL to its local relative path under the
/// mirror root, using the same convention as the downloader. Returns `None` for
/// non-Twitter-CDN URLs or URLs without a resolvable filename.
fn url_to_local_path(twitter_url: &str) -> Option<String> {
    if !twitter_url.contains("pbs.twimg.com") {
        return None;
    }
    // Filename is the last URL segment, possibly with a `?format=jpg…` query.
    let mut tail = twitter_url.rsplit('/').next().unwrap_or("").to_string();
    if let Some(qpos) = tail.find('?') {
        let (name, query) = tail.split_at(qpos);
        let query = &query[1..];
        let mut name = name.to_string();
        let ext_from_query = query
            .split('&')
            .find_map(|piece| piece.strip_prefix("format="));
        if !name.contains('.') {
            if let Some(ext) = ext_from_query {
                name = format!("{name}.{ext}");
            }
        }
        tail = name;
    }
    if !tail.contains('.') {
        return None;
    }
    let base = tail.split('.').next().unwrap_or("");
    if base.is_empty() {
        return None;
    }
    let prefix = if base.len() >= 2 { &base[..2] } else { "x_" };
    let prefix = prefix.to_lowercase();
    Some(format!("img/{prefix}/{tail}"))
}

#[derive(Deserialize)]
pub struct ByUrlParams {
    /// The Twitter CDN URL to resolve.
    pub u: String,
}

/// `GET /kols/media/by-url` — 302 to the local mirror if present, else 302 back
/// to the original CDN URL.
pub async fn by_url(State(st): State<AppState>, Query(p): Query<ByUrlParams>) -> Redirect {
    let root = &st.settings.kol_media_root;
    if !root.is_empty() {
        if let Some(rel) = url_to_local_path(&p.u) {
            let full = FsPath::new(root).join(&rel);
            if full.exists() {
                return Redirect::temporary(&format!("/kols/media/{rel}"));
            }
        }
    }
    // Fallback: send the caller back to the original CDN.
    Redirect::temporary(&p.u)
}

/// Reject any relative path that escapes the mirror root (`..`, absolute, or
/// drive-prefixed components). Returns the safe joined path on success.
fn safe_join(root: &str, rel: &str) -> Option<PathBuf> {
    let rel_path = FsPath::new(rel);
    for comp in rel_path.components() {
        match comp {
            Component::Normal(_) => {}
            // Anything that isn't a plain filename component is a traversal risk.
            _ => return None,
        }
    }
    Some(FsPath::new(root).join(rel_path))
}

/// Guess a content-type from the file extension (covers the mirrored image set).
fn content_type_for(path: &FsPath) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("mp4") => "video/mp4",
        Some("bmp") => "image/bmp",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    }
}

/// `GET /kols/media/{rel}` — serve the mirrored file from disk. Empty root →
/// 404 (serving disabled). Path-traversal attempts → 404. Missing file → 404.
pub async fn serve(State(st): State<AppState>, Path(rel): Path<String>) -> ApiResult<Response> {
    let root = &st.settings.kol_media_root;
    if root.is_empty() {
        return Err(ApiError::NotFound("kol media serving is disabled".into()));
    }
    let full =
        safe_join(root, &rel).ok_or_else(|| ApiError::NotFound("not found".into()))?;
    let meta = match tokio::fs::metadata(&full).await {
        Ok(m) if m.is_file() => m,
        _ => return Err(ApiError::NotFound("not found".into())),
    };
    let bytes = tokio::fs::read(&full)
        .await
        .map_err(|_| ApiError::NotFound("not found".into()))?;
    let ct = content_type_for(&full);
    let resp = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, ct.to_string()),
            (header::CONTENT_LENGTH, meta.len().to_string()),
        ],
        bytes,
    )
        .into_response();
    Ok(resp)
}
