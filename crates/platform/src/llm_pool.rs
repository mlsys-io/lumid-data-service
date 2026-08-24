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
        // The primary is a fallback for a request that names NO model, or for a
        // deployment with no OpenRouter catch-all to fall through to.
        //
        // It must NOT swallow an explicitly-named unknown model while a catch-all
        // exists. It did: an unknown id missed `by_model`, fell back to the
        // primary, and `resolve()` then took its `!backends.is_empty()` branch --
        // making the "unknown explicit model -> OpenRouter catch-all" arm
        // unreachable whenever LUMID_LLM_BACKEND_URL is set, which is always.
        // Measured: `z-ai/glm-5.2` and `deepseek/deepseek-v4-flash-0731` both came
        // back as vLLM `NotFoundError` 404s from the LOCAL backend instead of
        // routing to OpenRouter.
        // Gate on having an explicit roster, NOT on OpenRouter being configured:
        // an unknown id must resolve to no backend so resolve() can refuse it
        // outright. A deployment with no roster at all (only LUMID_LLM_BACKEND_URL)
        // still sends every named model to the primary, as it always did.
        let named_unknown = model.is_some()
            && !self.by_model.is_empty()
            && model.map_or(false, |m| !self.by_model.contains_key(m));
        let handles = model
            .and_then(|m| self.by_model.get(m))
            .map(|v| v.as_slice())
            .or_else(|| {
                if named_unknown {
                    None
                } else {
                    self.primary.as_ref().map(std::slice::from_ref)
                }
            });

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
                        // We are not scraping this backend, so its last depth is STALE and
                        // must not gate. `at_roof()` consults `at_queue_roof()` FIRST and is
                        // deliberately orthogonal to health, so a value frozen at >= roof
                        // would spill every request to OpenRouter for as long as the circuit
                        // stayed open -- and because nothing is then sent locally, nothing
                        // calls `on_connect_ok()` to close it. Self-sustaining. Clear to -1
                        // ("unknown, never gate") and let health be the only gate here.
                        h.set_queue_depth(-1);
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

/// Parse the backend's engine queue depth out of its Prometheus exposition.
///
/// Prefers `vllm:num_requests_waiting_by_reason{reason="capacity"}` and falls
/// back to the aggregate `vllm:num_requests_waiting`.
///
/// The distinction is load-bearing at a low roof. The aggregate counts BOTH
/// requests blocked on capacity and requests merely `deferred` by the scheduler
/// -- and this backend runs `--enable-chunked-prefill` with
/// `--long-prefill-token-threshold 1024`, which defers long prefills BY DESIGN.
/// So a single large-context turn can show waiting=1..2 with no congestion at
/// all. Gating on the aggregate would then spill paying traffic to the metered
/// OpenRouter path for a scheduling artifact rather than for real contention.
/// "Capacity" is what the roof is actually meant to mean: someone is blocked
/// because the engine is full.
///
/// Both forms are summed across engines/models so a multi-engine backend
/// reports total pressure. The fallback keeps older vLLM builds (which do not
/// export the by_reason breakdown) working exactly as before.
fn parse_num_requests_waiting(body: &str) -> Option<i32> {
    let mut capacity: f64 = 0.0;
    let mut capacity_seen = false;
    let mut aggregate: f64 = 0.0;
    let mut aggregate_seen = false;

    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') || !line.starts_with("vllm:num_requests_waiting") {
            continue;
        }
        let Some(v) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) else {
            continue;
        };
        if line.starts_with("vllm:num_requests_waiting_by_reason") {
            // Only the capacity reason counts as congestion.
            if line.contains(r#"reason="capacity""#) {
                capacity += v;
                capacity_seen = true;
            }
            continue;
        }
        aggregate += v;
        aggregate_seen = true;
    }

    if capacity_seen {
        Some(capacity.round() as i32)
    } else if aggregate_seen {
        Some(aggregate.round() as i32)
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
    fn prefers_capacity_over_the_aggregate() {
        // CONTRACT CHANGE (2026-08-23). This used to assert Some(5) -- the
        // aggregate -- on the reasoning that the _by_reason lines merely break
        // down the same requests. True, but the breakdown is the point: of
        // those 5, only 3 are blocked on capacity and 2 are `deferred` by
        // chunked prefill, which is normal scheduling and not congestion.
        // Gating on 5 would spill paying traffic to metered OpenRouter for a
        // scheduling artifact. Never 10: the two forms are not added together.
        assert_eq!(parse_num_requests_waiting(SAMPLE), Some(3));
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

    // An InFlightGuard MUST decrement on drop. This is the counter that, when
    // leaked, pins `at_roof()` true forever and sends every request to metered
    // OpenRouter while the local GPU is idle (2026-08-24, ~$40/day). The leak was
    // not here -- the guard is correct -- but in the stream task that HOLDS it;
    // this pins the invariant the fix depends on.
    #[test]
    fn inflight_guard_releases_on_drop() {
        let h = std::sync::Arc::new(BackendHandle::new("http://x".into(), 2, 0));
        assert_eq!(h.inflight(), 0);
        {
            let _a = h.acquire();
            let _b = h.acquire();
            assert_eq!(h.inflight(), 2);
            assert!(h.at_roof(), "two in-flight against max_concurrency 2 is the roof");
        }
        assert_eq!(h.inflight(), 0, "guards must decrement on drop");
        assert!(!h.at_roof(), "a drained backend must leave the roof");
    }

    // A stale queue depth must never outlive the scrape that produced it. When the
    // circuit opens the scraper stops sampling, and a depth frozen at >= roof would
    // keep `at_queue_roof()` true forever -- while nothing is routed locally, so
    // nothing calls on_connect_ok() to close the circuit. Self-sustaining spill.
    #[test]
    fn cleared_depth_releases_the_queue_gate() {
        let h = BackendHandle::new("http://x".into(), 8, 3);
        h.set_queue_depth(5);
        assert!(h.at_queue_roof(), "depth 5 against roof 3 saturates");
        h.set_queue_depth(-1); // what the scraper now does for an unhealthy backend
        assert!(!h.at_queue_roof(), "unknown depth must not gate");
        assert!(!h.at_roof(), "with inflight 0 and depth unknown, not at roof");
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

#[cfg(test)]
mod catchall_tests {
    use super::*;

    /// Build a pool the way from_settings would, without needing a Settings.
    fn pool(known: &[&str], with_primary: bool, openrouter: &str) -> BackendPool {
        let h = Arc::new(BackendHandle::new("http://gb10:8090".into(), 8, 4));
        let mut by_model: HashMap<String, Vec<Arc<BackendHandle>>> = HashMap::new();
        for m in known {
            by_model.insert((*m).to_string(), vec![h.clone()]);
        }
        BackendPool {
            by_model,
            primary: if with_primary { Some(h.clone()) } else { None },
            all: vec![h],
            openrouter_url: openrouter.to_string(),
        }
    }

    // An explicitly-named UNKNOWN model must resolve to NO backend, so resolve()
    // can REFUSE it. It must never reach OpenRouter: an e2e test once caught a
    // nonexistent model id being forwarded to real OpenRouter and billed, so a
    // typo became money (llm-0d342a8 closed that; 70fc036 reopened it by
    // accident; this pins it shut).
    #[test]
    fn named_unknown_model_resolves_to_no_backend() {
        let p = pool(&["deepseek-v4-flash"], true, "https://openrouter.ai/api");
        assert!(
            p.backends_for(Some("z-ai/glm-5.2")).is_empty(),
            "unknown model must resolve to NO local backend so resolve() refuses it"
        );
    }

    // The refusal must not depend on OpenRouter being configured -- otherwise
    // turning the URL off silently changes an unknown id from "refused" to
    // "served by the primary", which is how the roster stops meaning anything.
    #[test]
    fn named_unknown_is_refused_with_or_without_openrouter() {
        for or in ["https://openrouter.ai/api", ""] {
            let p = pool(&["deepseek-v4-flash"], true, or);
            assert!(
                p.backends_for(Some("z-ai/glm-5.2")).is_empty(),
                "unknown model must resolve to no backend (openrouter={or:?})"
            );
        }
    }

    #[test]
    fn known_model_still_resolves_locally() {
        let p = pool(&["deepseek-v4-flash"], true, "https://openrouter.ai/api");
        assert_eq!(p.backends_for(Some("deepseek-v4-flash")).len(), 1);
    }

    // With NO catch-all configured the primary fallback must still apply,
    // otherwise a legacy single-backend deployment (only LUMID_LLM_BACKEND_URL
    // set, clients naming a model) would start failing.
    #[test]
    fn primary_fallback_survives_without_a_catchall() {
        let p = pool(&[], true, "");
        assert_eq!(
            p.backends_for(Some("anything-at-all")).len(),
            1,
            "without OpenRouter the primary must still absorb a named model"
        );
    }

    #[test]
    fn unnamed_request_uses_primary() {
        let p = pool(&["deepseek-v4-flash"], true, "https://openrouter.ai/api");
        assert_eq!(p.backends_for(None).len(), 1);
    }
}

#[cfg(test)]
mod queue_reason_tests {
    use super::*;

    // The real shape emitted by the GB10 backend (captured 2026-08-23).
    const DEFERRED_ONLY: &str = r#"
vllm:num_requests_running{engine="0",model_name="deepseek-v4-flash"} 3.0
vllm:num_requests_waiting{engine="0",model_name="deepseek-v4-flash"} 2.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="capacity"} 0.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="deferred"} 2.0
"#;

    const REAL_CONGESTION: &str = r#"
vllm:num_requests_running{engine="0",model_name="deepseek-v4-flash"} 16.0
vllm:num_requests_waiting{engine="0",model_name="deepseek-v4-flash"} 5.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="capacity"} 3.0
vllm:num_requests_waiting_by_reason{engine="0",model_name="deepseek-v4-flash",reason="deferred"} 2.0
"#;

    // Chunked prefill DEFERS long prefills by design. Those must not count as
    // congestion: at roof=2 the aggregate (2.0) would spill paying traffic to
    // the metered OpenRouter path while the engine is not full at all.
    #[test]
    fn deferred_requests_are_not_congestion() {
        assert_eq!(parse_num_requests_waiting(DEFERRED_ONLY), Some(0));
    }

    // Genuine capacity blocking is reported, and the deferred requests riding
    // alongside it are excluded rather than inflating the depth.
    #[test]
    fn capacity_blocked_requests_are_congestion() {
        assert_eq!(parse_num_requests_waiting(REAL_CONGESTION), Some(3));
    }

    // Older vLLM builds export no by_reason breakdown — fall back to the
    // aggregate so they behave exactly as before.
    #[test]
    fn falls_back_to_aggregate_without_by_reason() {
        let legacy = r#"
vllm:num_requests_running{engine="0"} 4.0
vllm:num_requests_waiting{engine="0"} 7.0
"#;
        assert_eq!(parse_num_requests_waiting(legacy), Some(7));
    }

    // Multi-engine backends sum.
    #[test]
    fn sums_across_engines() {
        let multi = r#"
vllm:num_requests_waiting_by_reason{engine="0",reason="capacity"} 2.0
vllm:num_requests_waiting_by_reason{engine="1",reason="capacity"} 3.0
vllm:num_requests_waiting_by_reason{engine="0",reason="deferred"} 9.0
"#;
        assert_eq!(parse_num_requests_waiting(multi), Some(5));
    }

    // No metrics at all -> unknown, which never gates (queue_depth stays -1).
    #[test]
    fn absent_metric_is_unknown() {
        assert_eq!(parse_num_requests_waiting("# nothing here\n"), None);
    }
}
