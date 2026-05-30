//! Tiered fixed-window rate limiter — port of the slowapi setup in
//! `api/auth.py`. Authed callers (`id:`/`presented:` keys) get the authed
//! tier; raw-IP callers get the stricter anon tier.

use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

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
        let retry_after_s = if allowed { 0 } else { (window_id + 1) * window - now };
        Decision {
            allowed,
            retry_after_s,
            limit_spec: spec.clone(),
        }
    }
}
