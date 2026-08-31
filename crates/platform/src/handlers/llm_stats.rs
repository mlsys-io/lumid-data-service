//! `GET /admin/llm-backend-stats` — live tok/s + QPS per on-prem GPU backend,
//! computed over a rolling window.
//!
//! Reads straight off `BackendPool`'s in-memory throughput samples
//! (`llm_pool.rs::start_queue_scraper`, which already scrapes every healthy
//! backend's `/metrics` every 5s for engine queue depth — this handler reads
//! the SAME scrape's parsed counters rather than triggering a new one, so a
//! page hitting this endpoint on a fast poll costs nothing extra upstream).
//!
//! Admin-gated the same way as the other `/admin/*` routes in this crate
//! (`ingest::require_admin` — `super_admin` or the local key).

use axum::extract::State;
use axum::{Extension, Json};
use serde::Serialize;
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::error::ApiResult;
use crate::llm_pool::BackendHandle;
use crate::state::AppState;

use super::ingest::require_admin;

/// Static url→human-label map. Nothing in config carries operator-facing
/// backend names today (only bare URLs + a numeric `#tier=`) — this is
/// deliberately a small lookup here rather than a config surface, since the
/// roster is small and changes rarely enough that a hardcoded miss (falling
/// back to the bare host:port) is an acceptable, self-describing degrade
/// rather than something worth a new env var.
fn label_for(url: &str) -> String {
    match url {
        "http://100.117.154.126:4011" => "h100".to_string(),
        "http://100.93.49.42:8090" => "GX10".to_string(),
        "http://100.117.154.126:4001" => "s0-CPU-0".to_string(),
        "http://100.117.154.126:4003" => "s0-CPU-1".to_string(),
        other => other
            .rsplit_once("//")
            .map(|(_, host)| host.to_string())
            .unwrap_or_else(|| other.to_string()),
    }
}

#[derive(Serialize)]
struct BackendStats {
    label: String,
    url: String,
    tier: u32,
    healthy: bool,
    /// `None` (rendered `null`) until at least 2 scrape samples have landed —
    /// distinct from a genuine 0.0, which means "no traffic in the window",
    /// not "no data yet".
    tok_s: Option<f64>,
    qps: Option<f64>,
    /// -1 = unknown/not yet scraped, mirrors `BackendHandle::queue_depth`'s own
    /// convention so this payload doesn't invent a second one.
    queue_depth: i32,
}

fn stats_for(h: &BackendHandle) -> BackendStats {
    let (tok_s, qps) = match h.throughput_rates() {
        Some((t, q)) => (Some(round2(t)), Some(round2(q))),
        None => (None, None),
    };
    BackendStats {
        label: label_for(&h.url),
        url: h.url.clone(),
        tier: h.tier,
        healthy: h.is_healthy(),
        tok_s,
        qps,
        queue_depth: h.queue_depth(),
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

pub async fn llm_backend_stats(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    let backends: Vec<BackendStats> = st.llm_pool.all.iter().map(|h| stats_for(h)).collect();
    Ok(Json(json!({
        "window_seconds": crate::llm_pool::THROUGHPUT_WINDOW_S,
        "backends": backends,
    })))
}
