//! Blob-plane ingest — port of `ingest/storage.py` + `ingest/blob_core.py`.
//!
//! sha256(body) → check raw.blobs; if absent, write bytes to the local blob
//! root (atomic tmp→rename under `<root>/<prefix>/sha256=<hex>`), open a run,
//! insert the raw.blobs metadata row, close the run. Idempotent: same sha256 →
//! existing row, no second write.

use std::sync::Arc;

use deadpool_postgres::Pool;
use object_store::{path::Path as ObjPath, ObjectStore, PutPayload};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::Settings;
use crate::error::ApiError;
use crate::write::run;

#[derive(Serialize, Clone)]
pub struct BlobIngestResult {
    pub run_id: String,
    pub blob_sha256: String,
    pub storage_url: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub already_existed: bool,
    pub status: String,
}

impl BlobIngestResult {
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

pub fn is_configured(s: &Settings) -> bool {
    !s.blob_root.is_empty()
}

fn key_prefix_for(content_type: &str) -> &'static str {
    let ct = content_type.split(';').next().unwrap_or("").trim().to_lowercase();
    if ct.starts_with("image/") {
        "images"
    } else if ct == "application/pdf" {
        "pdf"
    } else if ct == "text/html" {
        "html"
    } else if ct == "text/plain" || ct == "text/markdown" {
        "text"
    } else if ct.starts_with("audio/") {
        "audio"
    } else if ct.starts_with("video/") {
        "video"
    } else {
        "blob"
    }
}

fn content_type_for(content_type: Option<&str>, suggested_name: Option<&str>) -> String {
    if let Some(ct) = content_type {
        let ct = ct.trim();
        if !ct.is_empty() {
            return ct.split(';').next().unwrap_or(ct).to_string();
        }
    }
    // Minimal extension → content-type guess (fallback only; the
    // mimetypes.guess_type parity surface is just common web types).
    if let Some(name) = suggested_name {
        let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
        let guessed = match ext.as_str() {
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "svg" => "image/svg+xml",
            "pdf" => "application/pdf",
            "html" | "htm" => "text/html",
            "txt" => "text/plain",
            "md" => "text/markdown",
            "json" => "application/json",
            "csv" => "text/csv",
            "mp4" => "video/mp4",
            "mp3" => "audio/mpeg",
            _ => "",
        };
        if !guessed.is_empty() {
            return guessed.to_string();
        }
    }
    "application/octet-stream".to_string()
}

fn public_url_for(s: &Settings, key: &str) -> String {
    let base = s.blob_public_base_url.trim_end_matches('/');
    if base.is_empty() {
        format!("/blobs/{key}")
    } else {
        format!("{base}/blobs/{key}")
    }
}

async fn lookup_existing(
    pool: &Pool,
    sha: &str,
) -> Result<Option<(String, String, i64)>, ApiError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT storage_url, content_type, size_bytes FROM raw.blobs WHERE blob_sha256 = $1",
            &[&sha],
        )
        .await?;
    Ok(row.map(|r| {
        (
            r.get::<_, String>("storage_url"),
            r.get::<_, String>("content_type"),
            r.get::<_, i64>("size_bytes"),
        )
    }))
}

/// End-to-end blob ingest.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_blob(
    pool: &Pool,
    settings: &Settings,
    blob_store: &Arc<dyn ObjectStore>,
    body: &[u8],
    content_type: Option<&str>,
    suggested_name: Option<&str>,
    metadata: Option<Value>,
    source: &str,
    source_endpoint: &str,
    submitted_by: &str,
    declared_endpoint: Option<&str>,
    user_agent: Option<&str>,
) -> Result<BlobIngestResult, ApiError> {
    if !is_configured(settings) {
        return Err(ApiError::Unavailable(
            "blob storage not configured (set LUMID_BLOB_ROOT)".into(),
        ));
    }
    if body.is_empty() {
        return Err(ApiError::BadRequest("empty body".into()));
    }
    let size = body.len() as i64;
    if (body.len() as u64) > settings.blob_max_bytes {
        return Err(ApiError::BadRequest(format!(
            "blob too large ({} bytes > {})",
            body.len(),
            settings.blob_max_bytes
        )));
    }

    let sha = {
        let mut h = Sha256::new();
        h.update(body);
        hex::encode(h.finalize())
    };

    // 1) Dedup short-circuit.
    if let Some((url, ct, sz)) = lookup_existing(pool, &sha).await? {
        return Ok(BlobIngestResult {
            run_id: String::new(),
            blob_sha256: sha,
            storage_url: url,
            content_type: ct,
            size_bytes: sz,
            already_existed: true,
            status: "ok".into(),
        });
    }

    let ct = content_type_for(content_type, suggested_name);
    let prefix = key_prefix_for(&ct);
    let key = format!("{prefix}/sha256={sha}");

    // 2) Write to the object store (localfs default → identical on-disk layout
    //    at `<blob_root>/<prefix>/sha256=<hex>`; or S3/MinIO when configured).
    blob_store
        .put(&ObjPath::from(key.as_str()), PutPayload::from(body.to_vec()))
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("blob put: {e}")))?;

    let storage_url = public_url_for(settings, &key);

    // 3) Run row + raw.blobs insert.
    let client = pool.get().await?;
    let mut args = json!({
        "target_schema": "raw",
        "target_table": "blobs",
        "mode": "blob",
        "blob_sha256": sha,
        "content_type": ct,
        "size_bytes": size,
    });
    let o = args.as_object_mut().unwrap();
    if let Some(d) = declared_endpoint {
        o.insert("declared_endpoint".into(), json!(d));
    }
    if let Some(ua) = user_agent {
        o.insert("user_agent".into(), json!(ua));
    }
    o.insert("submitted_by".into(), json!(submitted_by));

    let run_id = run::open_run(&client, "ingress:generic", &args, None).await?;
    run::set_submitted_by(&client, &run_id, submitted_by).await?;

    let md = metadata.unwrap_or_else(|| json!({}));
    let insert_res = client
        .execute(
            "INSERT INTO raw.blobs ( \
                 blob_sha256, storage_url, content_type, size_bytes, \
                 suggested_name, metadata, source, source_endpoint, \
                 source_run_id, submitted_by \
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (blob_sha256) DO NOTHING",
            &[
                &sha,
                &storage_url,
                &ct,
                &size,
                &suggested_name,
                &md,
                &source,
                &source_endpoint,
                &run_id,
                &submitted_by,
            ],
        )
        .await;

    match insert_res {
        Ok(_) => {
            let _ = run::close_run(&client, &run_id, "ok", 1, 0, 0, None).await;
            Ok(BlobIngestResult {
                run_id: run_id.to_string(),
                blob_sha256: sha,
                storage_url,
                content_type: ct,
                size_bytes: size,
                already_existed: false,
                status: "ok".into(),
            })
        }
        Err(e) => {
            let msg = format!("{e}");
            let _ = run::close_run(&client, &run_id, "failed", 0, 0, 1, Some(&msg)).await;
            Err(ApiError::BadRequest(format!("failed to record blob: {e}")))
        }
    }
}
