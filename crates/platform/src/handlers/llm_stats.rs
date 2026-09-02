//! `GET /admin/llm-backend-stats` — live tok/s + QPS per on-prem GPU backend,
//! computed over a rolling window.
//!
//! Reads straight off `BackendPool`'s in-memory throughput samples
//! (`llm_pool.rs::start_queue_scraper`, which already scrapes every healthy
//! backend's `/metrics` every 5s for engine queue depth — this handler reads
//! the SAME scrape's parsed counters rather than triggering a new one, so a
//! page hitting this endpoint on a fast poll costs nothing extra upstream).
//!
//! Scoped to `STATS_MODELS` (`deepseek-v4-flash`, `qwen3.8-27b`) only. The
//! pool also carries two llama.cpp EMBEDDING backends (qwen3-emb-0.6b/4b on
//! s0), which are excluded here — "tok/s of generated text" + "QPS of chat
//! turns" aren't the right measure for an embedding endpoint, and they're
//! registered under different model ids anyway, so `STATS_MODELS` naturally
//! excludes them.
//!
//! `qwen3.8-27b` ADDED 2026-09-02 — it was live (luyao1 RTX 5090, tier=0)
//! but invisible on the `/code` dashboard's on-prem GPU panel from the day it
//! shipped, because this endpoint only ever looked up a single hardcoded
//! model id. Widened from one id to a list rather than special-casing a
//! second lookup, so the next backend added here is one array entry, not
//! another copy of the whole function.
//!
//! Both backend dialects behind these models ARE supported for tok/s
//! (h100/GX10 are vLLM; s0-CPU and luyao1 are llama.cpp with `--metrics`
//! enabled specifically for this — s0-CPU since 2026-08-31, luyao1 from its
//! initial launch 2026-09-01) — see `parse_throughput_counters` in
//! `llm_pool.rs`, which is already dialect-generic and needed no change.
//! QPS is `None` for every llama.cpp leg specifically: llama.cpp's
//! exposition has no cumulative request-completion counter at all (only
//! point-in-time gauges), so there is nothing to compute a rate from — this
//! is a real, permanent gap for that backend family, not a "still warming
//! up" state.
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

/// The model ids this endpoint reports on. See the module doc for why the
/// embedding backends (registered under `qwen3-emb-*`) are excluded rather
/// than shown with a metric that can never populate. Order is display order.
const STATS_MODELS: &[&str] = &["deepseek-v4-flash", "qwen3.8-27b"];

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
        "http://100.73.23.96:8080" => "luyao1".to_string(),
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
    /// `None` for two DIFFERENT reasons the client must not conflate: (a) not
    /// warmed up yet (same as `tok_s: None` — check `tok_s` to tell them
    /// apart: this backend's tok_s is also None), or (b) `tok_s` IS Some but
    /// this backend's dialect (llama.cpp) has no request-completion counter
    /// to compute a rate from at all — see the module doc. RETAINED for
    /// completeness/tooltip detail, but no longer the primary "how loaded is
    /// this backend" figure the UI leads with — see inflight/peak_inflight_5m.
    qps: Option<f64>,
    /// ADDED 2026-09-02: an operator read the dashboard's `29.7 tok/s ·
    /// 0.05 qps` pairing as contradictory — it isn't (qps is completions/s,
    /// and a backend running few, individually SLOW requests generates
    /// tokens steadily while finishing almost nothing per second), but the
    /// UI gave no way to see what was actually meant: how many requests are
    /// running AT ONCE, and whether there was a burst. inflight is this
    /// process's own live in-flight count for the backend (not scraped —
    /// works for every dialect, including llama.cpp, unlike qps).
    inflight: i32,
    /// Highest inflight seen across the same rolling window tok_s/qps are
    /// computed over — answers "bursty in the last 5 min", which a single
    /// current-inflight snapshot on a ~12-15s dashboard poll can miss
    /// entirely between two short spikes. `None` until at least one sample
    /// has landed (mirrors tok_s's own "warming up" convention).
    peak_inflight_5m: Option<i32>,
    /// -1 = unknown/not yet scraped, mirrors `BackendHandle::queue_depth`'s own
    /// convention so this payload doesn't invent a second one.
    queue_depth: i32,
}

fn stats_for(h: &BackendHandle) -> BackendStats {
    let (tok_s, qps) = match h.throughput_rates() {
        Some((t, q)) => (Some(round2(t)), q.map(round2)),
        None => (None, None),
    };
    BackendStats {
        label: label_for(&h.url),
        url: h.url.clone(),
        tier: h.tier,
        healthy: h.is_healthy(),
        tok_s,
        qps,
        inflight: h.inflight(),
        peak_inflight_5m: h.peak_inflight_in_window(),
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
    // Flatten across STATS_MODELS in list order, not a single lookup — a
    // model with no registered backends (removed, or a config-only roster
    // change not yet applied) contributes nothing rather than erroring, same
    // degrade-shape the single-model lookup already had.
    let backends: Vec<BackendStats> = STATS_MODELS
        .iter()
        .filter_map(|m| st.llm_pool.by_model.get(*m))
        .flat_map(|hs| hs.iter().map(|h| stats_for(h)))
        .collect();
    Ok(Json(json!({
        "window_seconds": crate::llm_pool::THROUGHPUT_WINDOW_S,
        "backends": backends,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_for_covers_the_whole_roster() {
        assert_eq!(label_for("http://100.117.154.126:4011"), "h100");
        assert_eq!(label_for("http://100.93.49.42:8090"), "GX10");
        assert_eq!(label_for("http://100.117.154.126:4001"), "s0-CPU-0");
        assert_eq!(label_for("http://100.117.154.126:4003"), "s0-CPU-1");
        assert_eq!(label_for("http://100.73.23.96:8080"), "luyao1");
    }

    #[test]
    fn label_for_degrades_to_host_port_on_a_miss() {
        // An unlabeled backend must still show SOMETHING self-describing
        // rather than panic or an empty string.
        assert_eq!(label_for("http://10.0.0.1:9999"), "10.0.0.1:9999");
    }

    #[test]
    fn stats_models_names_qwen38_alongside_deepseek() {
        // The regression this whole change fixes: qwen3.8-27b was live and
        // serving real traffic but invisible on the dashboard because this
        // list only ever named one model.
        assert!(STATS_MODELS.contains(&"deepseek-v4-flash"));
        assert!(STATS_MODELS.contains(&"qwen3.8-27b"));
        assert_eq!(STATS_MODELS.len(), 2, "a third model added here should extend this assertion too");
    }
}
