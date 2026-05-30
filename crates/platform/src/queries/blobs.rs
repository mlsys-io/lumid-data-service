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
