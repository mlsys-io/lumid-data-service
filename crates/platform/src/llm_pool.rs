//! Health-aware, least-loaded backend pool for the LLM proxy.
//!
//! Each model maps to a list of backend URLs. Per-URL state:
//! - `inflight`: current in-flight request count (`AtomicI32`)
//! - `healthy`: circuit-breaker latch (`AtomicBool`)
//! - `failures`: consecutive connect failures (`AtomicU32`)
//!
//! `backends_for(model)` returns backends sorted least-loaded-first, healthy
//! before unhealthy, for the caller to iterate as a retry list.
//!
//! `InFlightGuard` is a RAII handle that decrements inflight on drop — callers
//! hold it for the duration of the forwarded request.
//!
//! A background task probes unhealthy backends every `PROBE_INTERVAL_S` seconds
//! via `GET /health`; on a <500 response the circuit closes and the backend
//! re-enters normal rotation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering::Relaxed};
use std::sync::Arc;

const CIRCUIT_OPEN_AFTER: u32 = 3;
const PROBE_INTERVAL_S: u64 = 10;
/// How often to scrape each healthy backend's /metrics for its engine queue
/// depth. Must be short relative to how long a queued request waits (observed
/// 300-570s) but long enough to cost nothing: one cheap GET per backend.
const QUEUE_SCRAPE_INTERVAL_S: u64 = 5;

// ──────────────────────────────────────────── BackendHandle

pub struct BackendHandle {
    pub url: String,
    inflight: AtomicI32,
    healthy: AtomicBool,
    failures: AtomicU32,
    /// Concurrency roof: when in-flight reaches this, the backend is "full" and
    /// is treated as unavailable by the resolver (overflow to OpenRouter when
    /// every local backend is full). 0 disables the roof.
    max_concurrency: u32,
    /// Engine queue depth last read from the backend's `/metrics`
    /// (`vllm:num_requests_waiting`). -1 means "unknown" — never gated.
    queue_depth: AtomicI32,
    /// Queue roof: treat the backend as full once `queue_depth` reaches this.
    /// 0 disables. See `Settings::llm_backend_queue_roof` for why this exists.
    queue_roof: u32,
}

impl BackendHandle {
    fn new(url: String, max_concurrency: u32, queue_roof: u32) -> Self {
        Self {
            url,
            inflight: AtomicI32::new(0),
            healthy: AtomicBool::new(true),
            failures: AtomicU32::new(0),
            max_concurrency,
            // -1 = not yet observed. Until metrics are actually read, the queue
            // roof must never gate a backend: an unreachable /metrics endpoint
            // (non-vLLM backend, scrape blocked) would otherwise silently push
            // all traffic to OpenRouter.
            queue_depth: AtomicI32::new(-1),
            queue_roof,
        }
    }

    /// Engine queue depth last observed, or -1 when unknown.
    pub fn queue_depth(&self) -> i32 {
        self.queue_depth.load(Relaxed)
    }

    /// Record a queue depth scraped from the backend's /metrics.
    pub fn set_queue_depth(&self, n: i32) {
        self.queue_depth.store(n, Relaxed);
    }

    /// Whether the backend's own engine queue is at/over the queue roof.
    /// Unknown depth (-1) never saturates — see `new`.
    pub fn at_queue_roof(&self) -> bool {
        if self.queue_roof == 0 {
            return false;
        }
        let q = self.queue_depth();
        q >= 0 && q >= self.queue_roof as i32
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Relaxed)
    }

    pub fn inflight(&self) -> i32 {
        self.inflight.load(Relaxed)
    }

    /// Whether this backend is saturated, by EITHER signal:
    ///
    ///   - in-flight roof: requests THIS process has issued, or
    ///   - queue roof: the backend engine's own pending queue, scraped from
    ///     /metrics — which is the only signal that sees other clients sharing
    ///     the same GPU.
    ///
    /// The in-flight roof alone was insufficient: it read "room available" while
    /// vLLM held ten queued requests, so turns were admitted into a queue and
    /// waited minutes. A roof must not trip on the circuit being open — it is a
    /// load gate, orthogonal to health.
    pub fn at_roof(&self) -> bool {
        if self.at_queue_roof() {
            return true;
        }
        if self.max_concurrency == 0 {
            return false;
        }
        self.inflight() >= self.max_concurrency as i32
    }

    /// Acquire an in-flight slot. The returned guard decrements on drop.
    pub fn acquire(self: &Arc<Self>) -> InFlightGuard {
        self.inflight.fetch_add(1, Relaxed);
        InFlightGuard(self.clone())
    }

    /// Call after a successful connect (resets failure counter, closes circuit).
    pub fn on_connect_ok(&self) {
        self.failures.store(0, Relaxed);
        if !self.healthy.swap(true, Relaxed) {
            tracing::info!("llm backend {} recovered (circuit closed)", self.url);
        }
    }

    /// Call on a connect-level failure (refused, timeout before first byte).
    /// Opens the circuit after `CIRCUIT_OPEN_AFTER` consecutive failures.
    pub fn on_connect_err(&self) {
        let n = self.failures.fetch_add(1, Relaxed) + 1;
        if n >= CIRCUIT_OPEN_AFTER && self.healthy.swap(false, Relaxed) {
            tracing::warn!(
                "llm backend {} circuit open ({} consecutive failures)",
                self.url,
                n
            );
        }
    }
}

// ──────────────────────────────────────────── InFlightGuard

/// Decrements the backend's in-flight counter when dropped (cancel-safe).
pub struct InFlightGuard(Arc<BackendHandle>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Relaxed);
    }
}

// ──────────────────────────────────────────── BackendPool

pub struct BackendPool {
    /// Model name → ordered backend list (config order; routing sorts at call time).
    pub by_model: HashMap<String, Vec<Arc<BackendHandle>>>,
    /// Primary/default backend (used when no model or model not in `by_model`).
    pub primary: Option<Arc<BackendHandle>>,
    /// All unique handles, for health probing.
    pub all: Vec<Arc<BackendHandle>>,
    /// OpenRouter catch-all URL — forwarded verbatim, not part of the pool.
    pub openrouter_url: String,
}

impl BackendPool {
    pub fn from_settings(s: &crate::config::Settings) -> Self {
        let mut all: Vec<Arc<BackendHandle>> = Vec::new();
        let mut seen: HashMap<String, Arc<BackendHandle>> = HashMap::new();

        let max_concurrency = s.llm_backend_max_concurrency;
        let queue_roof = s.llm_backend_queue_roof;
        let mut intern = |url: &str| -> Arc<BackendHandle> {
            let key = url.trim_end_matches('/').to_string();
            seen.entry(key.clone())
                .or_insert_with(|| {
                    let h = Arc::new(BackendHandle::new(key, max_concurrency, queue_roof));
                    all.push(h.clone());
                    h
                })
                .clone()
        };

        let primary = if !s.llm_backend_url.is_empty() {
            Some(intern(&s.llm_backend_url))
        } else {
            None
        };

        let mut by_model: HashMap<String, Vec<Arc<BackendHandle>>> = HashMap::new();
        for (model, urls) in &s.llm_backends {
            let handles: Vec<_> = urls.iter().map(|u| intern(u)).collect();
            if !handles.is_empty() {
                by_model.insert(model.clone(), handles);
            }
        }

        Self {
            by_model,
            primary,
            all,
            openrouter_url: s.llm_openrouter_url.clone(),
        }
    }

    /// Backends to try for `model`, sorted: healthy-least-loaded first, then
    /// unhealthy (fallback of last resort). Returns empty when nothing is configured.
    pub fn backends_for(&self, model: Option<&str>) -> Vec<Arc<BackendHandle>> {
        let handles = model
            .and_then(|m| self.by_model.get(m))
            .map(|v| v.as_slice())
            .or_else(|| self.primary.as_ref().map(std::slice::from_ref));

        let Some(handles) = handles else {
            return vec![];
        };

        let mut sorted: Vec<Arc<BackendHandle>> = handles.iter().cloned().collect();
        sorted.sort_by_key(|h| {
            // Sort order: healthy < unhealthy; within each, not-at-roof < at-roof;
            // within each tier, fewer in-flight first. A saturated backend is tried
            // last (still a retry candidate), never silently dropped.
            let health = if h.is_healthy() { 0i32 } else { 1_000_000 };
            let roof = if h.at_roof() { 100_000 } else { 0 };
            health + roof + h.inflight()
        });
        sorted
    }

    /// Spawn a background task that re-probes unhealthy backends every `PROBE_INTERVAL_S` s.
    pub fn start_health_prober(self: Arc<Self>, http: reqwest::Client) {
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(PROBE_INTERVAL_S));
            tick.tick().await; // discard the immediate first tick
            loop {
                tick.tick().await;
                for h in &self.all {
                    if !h.is_healthy() {
                        probe_one(&http, h).await;
                    }
                }
            }
        });
    }
}

async fn probe_one(http: &reqwest::Client, h: &BackendHandle) {
    let url = format!("{}/health", h.url);
    match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().as_u16() < 500 => h.on_connect_ok(),
        Ok(r) => {
            tracing::warn!("llm probe {}: HTTP {}", h.url, r.status());
            h.on_connect_err();
        }
        Err(e) => {
            tracing::warn!("llm probe {}: {e}", h.url);
            h.on_connect_err();
        }
    }
}

// ──────────────────────────────────────────── engine-queue scraping

impl BackendPool {
    /// Spawn a background task that scrapes each backend's `/metrics` for its
    /// engine queue depth (`vllm:num_requests_waiting`).
    ///
    /// This is the only signal that sees load from OTHER clients on the same
    /// GPU. `inflight` counts what this process issued; the GB10 is also driven
    /// directly by xpio loops and other apps, so a backend can be ten deep while
    /// this process believes it has room. Scraping the engine closes that gap and
    /// lets the resolver spill to OpenRouter BEFORE queueing, rather than after a
    /// timeout.
    ///
    /// Failure is silent and non-gating: a backend whose /metrics cannot be read
    /// keeps depth -1 and is never blocked by the queue roof.
    pub fn start_queue_scraper(self: Arc<Self>, http: reqwest::Client) {
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(QUEUE_SCRAPE_INTERVAL_S));
            loop {
                tick.tick().await;
                for h in &self.all {
                    if !h.is_healthy() {
                        continue;
                    }
                    let prev = h.queue_depth();
                    match scrape_queue_depth(&http, &h.url).await {
                        Some(n) => {
                            h.set_queue_depth(n);
                            // Log only on a saturation EDGE, so this is quiet in
                            // steady state but visible when spill begins/ends.
                            //
                            // The edge must be the ROOF, not a hardcoded 1. With
                            // queue_roof=4 this warned "treating as saturated, new
                            // requests spill to OpenRouter" every time depth merely
                            // reached 1 -- 15 times in 24h, none of which spilled
                            // anything, because the actual gate (at_queue_roof) is
                            // q >= queue_roof. The log was reporting a fallback that
                            // never happened.
                            let roof = h.queue_roof.max(1) as i32;
                            let was = prev >= roof;
                            let now = n >= roof;
                            if now != was {
                                if now {
                                    tracing::warn!(
                                        "llm backend {} engine queue depth {} — treating as saturated, new requests spill to OpenRouter",
                                        h.url, n
                                    );
                                } else {
                                    tracing::info!(
                                        "llm backend {} engine queue drained — resuming local routing",
                                        h.url
                                    );
                                }
                            }
                        }
                        // Unreachable/unparseable: forget the stale value rather
                        // than gate on it forever.
                        None => h.set_queue_depth(-1),
                    }
                }
            }
        });
    }
}

/// GET `<url>/metrics` and parse `vllm:num_requests_waiting`.
/// Returns None when the endpoint is unreachable or the metric is absent.
async fn scrape_queue_depth(http: &reqwest::Client, base: &str) -> Option<i32> {
    let url = format!("{}/metrics", base);
    let body = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    parse_num_requests_waiting(&body)
}

/// Parse the Prometheus exposition line for `vllm:num_requests_waiting`.
/// Sums across engines/models so a multi-engine backend reports total pressure.
fn parse_num_requests_waiting(body: &str) -> Option<i32> {
    let mut total: f64 = 0.0;
    let mut seen = false;
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') || !line.starts_with("vllm:num_requests_waiting") {
            continue;
        }
        // Skip the _by_reason breakdown: it double-counts the same requests.
        if line.starts_with("vllm:num_requests_waiting_by_reason") {
            continue;
        }
        if let Some(v) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            total += v;
            seen = true;
        }
    }
    if seen {
        Some(total.round() as i32)
    } else {
        None
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;

    const SAMPLE: &str = r#"
# HELP vllm:num_requests_running Number of requests running.
vllm:num_requests_running{engine="0",model_name="deepseek-v4-flash"} 5.0
vllm:num_requests_waiting{engine="0",model_name="deepseek-v4-flash"} 5.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="capacity"} 3.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="deferred"} 2.0
"#;

    #[test]
    fn parses_waiting_and_ignores_by_reason() {
        // 5, not 10: the _by_reason lines break down the SAME requests.
        assert_eq!(parse_num_requests_waiting(SAMPLE), Some(5));
    }

    #[test]
    fn absent_metric_is_none() {
        assert_eq!(parse_num_requests_waiting("# nothing here\n"), None);
    }

    #[test]
    fn unknown_depth_never_saturates() {
        let h = BackendHandle::new("http://x".into(), 8, 1);
        assert_eq!(h.queue_depth(), -1);
        assert!(!h.at_queue_roof(), "unknown depth must not gate");
        assert!(!h.at_roof(), "unknown depth must not saturate the backend");
    }

    #[test]
    fn queue_roof_saturates_even_with_idle_inflight() {
        // The whole point: zero in-flight HERE, but the engine is queued because
        // other clients share the GPU.
        let h = BackendHandle::new("http://x".into(), 8, 1);
        h.set_queue_depth(5);
        assert_eq!(h.inflight(), 0);
        assert!(h.at_roof(), "engine queue must saturate regardless of inflight");
    }

    #[test]
    fn queue_roof_zero_disables() {
        let h = BackendHandle::new("http://x".into(), 8, 0);
        h.set_queue_depth(99);
        assert!(!h.at_queue_roof());
        assert!(!h.at_roof());
    }
}

#[cfg(test)]
mod queue_roof_edge_tests {
    use super::*;

    // The saturation warning must fire on the ROOF, not on any queue at all.
    // With queue_roof=4 the old edge (hardcoded 1) warned "new requests spill to
    // OpenRouter" whenever depth reached 1 — 15 times in 24h of production, none
    // of which spilled anything, because at_queue_roof gates on q >= queue_roof.
    #[test]
    fn saturation_edge_follows_the_roof() {
        let h = BackendHandle::new("http://x".into(), 8, 4);
        for (depth, want) in [(0, false), (1, false), (3, false), (4, true), (9, true)] {
            h.set_queue_depth(depth);
            assert_eq!(
                h.at_queue_roof(),
                want,
                "queue depth {depth} with roof 4 should saturate={want}"
            );
        }
    }

    // roof=0 disables the queue signal entirely; only in-flight applies.
    #[test]
    fn zero_roof_disables_queue_gate() {
        let h = BackendHandle::new("http://x".into(), 8, 0);
        h.set_queue_depth(50);
        assert!(!h.at_queue_roof(), "roof 0 must disable the queue gate");
        assert!(!h.at_roof(), "in-flight is 0, so the backend is not at roof");
    }
}
