//! Blob domain — content-type lookup. Port of
//! api/routes/blobs.py `_content_type_for_key`.

use deadpool_postgres::Pool;

use crate::error::ApiResult;

/// Look up `raw.blobs.content_type` by reconstructing the sha256 from the key
/// suffix (`.../sha256=<64-hex>`). Returns None when the key has no usable
/// `sha256=` segment or no matching row — caller falls back to
/// `application/octet-stream`.
pub async fn content_type_for_key(pool: &Pool, key: &str) -> ApiResult<Option<String>> {
    let Some(sha) = key.rsplit_once("sha256=").map(|(_, s)| s) else {
        return Ok(None);
    };
    if sha.len() != 64 {
        return Ok(None);
    }
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT content_type FROM raw.blobs WHERE blob_sha256 = $1",
            &[&sha],
        )
        .await?;
    Ok(row.and_then(|r| r.get::<_, Option<String>>("content_type")))
}

/// Minimal `mimetypes.guess_type()`-parity extension → MIME lookup. Last-resort
/// fallback for `GET /blobs/{key}` when neither the object-store metadata nor
/// `raw.blobs` has a usable content-type (e.g. a legacy extensionless
/// `sha256=` CAS key with no recorded row). Deliberately narrow — it only
/// needs to cover keys that carry a real extension; content-addressed keys
/// never will, so they must be resolved upstream instead.
pub fn guess_from_extension(key: &str) -> Option<String> {
    let ext = key.rsplit('.').next().unwrap_or(key);
    if ext == key {
        return None;
    }
    let ct = match ext.to_ascii_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "ico" => "image/vnd.microsoft.icon",
        "svg" => "image/svg+xml",
        "tif" | "tiff" => "image/tiff",
        "pdf" => "application/pdf",
        "html" | "htm" => "text/html",
        "txt" => "text/plain",
        "md" => "text/markdown",
        "csv" => "text/csv",
        "json" => "application/json",
        "xml" => "application/xml",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",
        "wasm" => "application/wasm",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "parquet" => "application/vnd.apache.parquet",
        _ => return None,
    };
    Some(ct.to_string())
}
