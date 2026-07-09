//! Tiered fixed-window rate limiter + per-key/IP concurrency limiter.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use dashmap::DashMap;
use http_body::Frame;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Parse "<n>/<unit>" → (limit, window_seconds). Defaults to 60/min on garbage.
fn parse_rate(spec: &str) -> (u32, u64) {
    let mut it = spec.split('/');
    let n: u32 = it.next().and_then(|s| s.trim().parse().ok()).unwrap_or(60);
    let unit = it.next().unwrap_or("minute").trim().trim_end_matches('s').to_lowercase();
    let secs = match unit.as_str() {
        "second" => 1,
        "minute" => 60,
        "hour" => 3600,
        "day" => 86400,
        _ => 60,
    };
    (n, secs)
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// ─── Fixed-window rate limiter ────────────────────────────────────────────────

pub struct RateLimiter {
    anon: (u32, u64),
    authed: (u32, u64),
    anon_spec: String,
    authed_spec: String,
    // key → (window_id, count)
    buckets: DashMap<String, (u64, u32)>,
}

pub struct Decision {
    pub allowed: bool,
    pub retry_after_s: u64,
    pub limit_spec: String,
    pub limit: u64,
    pub remaining: u64,
    pub reset_s: u64,
}

impl RateLimiter {
    pub fn new(anon_spec: &str, authed_spec: &str) -> Self {
        RateLimiter {
            anon: parse_rate(anon_spec),
            authed: parse_rate(authed_spec),
            anon_spec: anon_spec.to_string(),
            authed_spec: authed_spec.to_string(),
            buckets: DashMap::new(),
        }
    }

    /// `key` is "id:<sub>" / "presented:<ip>" / "ip:<ip>". Authed tier applies
    /// to id:/presented:, anon tier otherwise.
    pub fn check(&self, key: &str) -> Decision {
        let authed = key.starts_with("id:") || key.starts_with("presented:");
        let (limit, window) = if authed { self.authed } else { self.anon };
        let spec = if authed { &self.authed_spec } else { &self.anon_spec };
        let now = now_secs();
        let window_id = now / window;
        let mut entry = self.buckets.entry(key.to_string()).or_insert((window_id, 0));
        if entry.0 != window_id {
            *entry = (window_id, 0);
        }
        entry.1 += 1;
        let count = entry.1;
        let allowed = count <= limit;
        let reset_s = (window_id + 1) * window - now;
        let retry_after_s = if allowed { 0 } else { reset_s };
        Decision {
            allowed,
            retry_after_s,
            limit_spec: spec.clone(),
            limit: limit as u64,
            remaining: (limit as u64).saturating_sub(count as u64),
            reset_s,
        }
    }
}

// ─── Per-key / per-IP concurrency limiter ─────────────────────────────────────

/// Holds a reference to the in-flight counter; decrements when dropped.
/// Stored inside `CountedBody` so the slot is released only when the response
/// body is fully consumed (correct for SSE streams and regular JSON alike).
pub struct ConcurrencyGuard(Arc<AtomicU32>);

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

pub struct ConcurrencyLimiter {
    per_key: u32,
    per_ip: u32,
    counters: DashMap<String, Arc<AtomicU32>>,
}

impl ConcurrencyLimiter {
    pub fn new(per_key: u32, per_ip: u32) -> Self {
        ConcurrencyLimiter { per_key, per_ip, counters: DashMap::new() }
    }

    /// Try to acquire a concurrency slot for `bucket` (a key or IP string).
    /// Returns `None` when the limit is already reached.
    fn try_acquire(&self, bucket: &str, limit: u32) -> Option<ConcurrencyGuard> {
        let counter = self.counters
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone();
        let prev = counter.fetch_add(1, Ordering::AcqRel);
        if prev >= limit {
            counter.fetch_sub(1, Ordering::Release);
            None
        } else {
            Some(ConcurrencyGuard(counter))
        }
    }

    /// Acquire slots for both the per-key and per-IP buckets atomically.
    /// Returns `(key_guard, ip_guard)` or `None` if either limit is exceeded.
    pub fn acquire(&self, key: &str, ip: &str) -> Option<(ConcurrencyGuard, ConcurrencyGuard)> {
        let kg = self.try_acquire(key, self.per_key)?;
        match self.try_acquire(ip, self.per_ip) {
            Some(ig) => Some((kg, ig)),
            None => None, // kg drops here, releasing the key slot
        }
    }
}

// ─── Response body wrapper ────────────────────────────────────────────────────

/// Wraps an axum response body and holds the concurrency guards until the body
/// is fully consumed or dropped (client disconnects). This is what keeps the
/// in-flight slot accurate for SSE streams.
pub struct CountedBody {
    inner: Body,
    // Held until body drops; silence the dead-code lint explicitly.
    #[allow(dead_code)]
    guards: (ConcurrencyGuard, ConcurrencyGuard),
}

impl CountedBody {
    pub fn wrap(resp: Response, guards: (ConcurrencyGuard, ConcurrencyGuard)) -> Response {
        let (parts, body) = resp.into_parts();
        Response::from_parts(parts, Body::new(CountedBody { inner: body, guards }))
    }
}

impl http_body::Body for CountedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
