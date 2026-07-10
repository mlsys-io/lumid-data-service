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

// ──────────────────────────────────────────── BackendHandle

pub struct BackendHandle {
    pub url: String,
    inflight: AtomicI32,
    healthy: AtomicBool,
    failures: AtomicU32,
}

impl BackendHandle {
    fn new(url: String) -> Self {
        Self {
            url,
            inflight: AtomicI32::new(0),
            healthy: AtomicBool::new(true),
            failures: AtomicU32::new(0),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Relaxed)
    }

    pub fn inflight(&self) -> i32 {
        self.inflight.load(Relaxed)
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

        let mut intern = |url: &str| -> Arc<BackendHandle> {
            let key = url.trim_end_matches('/').to_string();
            seen.entry(key.clone())
                .or_insert_with(|| {
                    let h = Arc::new(BackendHandle::new(key));
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
            // Unhealthy backends sort far last; within healthy, prefer fewer inflight.
            let tier = if h.is_healthy() { 0i32 } else { 1_000_000 };
            tier + h.inflight()
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
