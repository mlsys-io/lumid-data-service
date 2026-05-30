//! Webhook HMAC verification — port of `ingest/webhook_auth.py`.
//!
//! A pre-registered (webhook_id, secret) pair; the caller sends
//! `X-Webhook-Signature: <hex>` (optionally `sha256=<hex>`) = HMAC-SHA256 of
//! the raw body keyed by the secret. We recompute server-side and compare in
//! constant time. The webhook binds to a typed target (adapter mode is out of
//! scope for the Rust build — see report).

use deadpool_postgres::Pool;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::ApiError;

type HmacSha256 = Hmac<Sha256>;

/// Extract the 64-hex digest from `X-Webhook-Signature`, tolerating an optional
/// `sha256=` scheme prefix. Returns the lowercased hex, or None if malformed.
fn parse_sig(header: &str) -> Option<String> {
    let h = header.trim();
    let hexpart = h.strip_prefix("sha256=").unwrap_or(h);
    if hexpart.len() == 64 && hexpart.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(hexpart.to_lowercase())
    } else {
        None
    }
}

#[derive(Clone)]
pub struct WebhookRow {
    pub webhook_id: String,
    pub owner_sub: String,
    pub secret_plain: String,
    pub target_schema: Option<String>,
    pub target_table: Option<String>,
    pub adapter_id: Option<String>,
    pub source_endpoint: Option<String>,
    pub active: bool,
}

async fn fetch_webhook(pool: &Pool, webhook_id: &str) -> Result<Option<WebhookRow>, ApiError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT webhook_id::text, owner_sub, secret_plain, \
                    target_schema, target_table, adapter_id, \
                    source_endpoint, active \
               FROM provenance.webhooks \
              WHERE webhook_id::text = $1",
            &[&webhook_id],
        )
        .await?;
    Ok(row.map(|r| WebhookRow {
        webhook_id: r.get(0),
        owner_sub: r.get(1),
        secret_plain: r.get(2),
        target_schema: r.get(3),
        target_table: r.get(4),
        adapter_id: r.get(5),
        source_endpoint: r.get(6),
        active: r.get(7),
    }))
}

fn verify_signature(secret: &str, body: &[u8], sig_header: &str) -> bool {
    let supplied = match parse_sig(sig_header) {
        Some(s) => s,
        None => return false,
    };
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());
    // hmac's CtOutput compare would need raw bytes; compare hex strings via
    // constant-time byte compare.
    constant_time_eq(expected.as_bytes(), supplied.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Look up + verify. Returns the webhook row or an auth error
/// (404 unknown/inactive, 401 bad signature).
pub async fn authenticate(
    pool: &Pool,
    webhook_id: &str,
    body: &[u8],
    sig_header: &str,
) -> Result<WebhookRow, ApiError> {
    let wh = fetch_webhook(pool, webhook_id).await?;
    let wh = match wh {
        Some(w) if w.active => w,
        _ => return Err(ApiError::NotFound(format!("unknown webhook {webhook_id:?}"))),
    };
    if !verify_signature(&wh.secret_plain, body, sig_header) {
        return Err(ApiError::Unauthorized("invalid webhook signature".into()));
    }
    Ok(wh)
}

/// Increment use_count + bump last_used_at, best-effort (spawned detached).
pub fn stamp_used(pool: Pool, webhook_id: String) {
    tokio::spawn(async move {
        if let Ok(client) = pool.get().await {
            let _ = client
                .execute(
                    "UPDATE provenance.webhooks \
                        SET use_count = use_count + 1, last_used_at = now() \
                      WHERE webhook_id::text = $1",
                    &[&webhook_id],
                )
                .await;
        }
    });
}
