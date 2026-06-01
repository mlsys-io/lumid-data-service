//! Synthetic tick publisher — port of `api/realtime/synthetic.py`.
//!
//! Publishes a fake tick per second for a few test symbols so the hub + WS +
//! SSE pipeline can be validated before real upstreams exist. Enabled by
//! `LUMID_RT_SYNTHETIC=1`; off in any real deployment. Uses a tiny xorshift
//! PRNG seeded from the wall clock (no `rand` dependency for a dev-only tool).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::AsyncCommands;
use serde_json::json;

use super::hub::now_iso;

const SYMBOLS: [&str; 3] = ["TEST", "DEMO", "FAKE"];

struct XorShift(u64);
impl XorShift {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// Uniform f64 in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Uniform f64 in [lo, hi).
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + self.unit() * (hi - lo)
    }
}

pub fn run(mut redis: redis::aio::MultiplexedConnection) {
    tokio::spawn(async move {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15)
            | 1;
        let mut rng = XorShift(seed);
        let mut base: Vec<f64> = (0..SYMBOLS.len()).map(|i| 100.0 + i as f64 * 7.0).collect();
        tracing::info!("synthetic realtime publisher running (symbols={SYMBOLS:?})");
        loop {
            let now = now_iso();
            for (i, sym) in SYMBOLS.iter().enumerate() {
                base[i] += rng.range(-0.5, 0.5);
                let price = (base[i] * 100.0).round() / 100.0;
                let tick = json!({
                    "symbol": sym, "ts": now, "price": price,
                    "bid": ((price - 0.01) * 100.0).round() / 100.0,
                    "ask": ((price + 0.01) * 100.0).round() / 100.0,
                    "volume": (rng.range(100.0, 5000.0)) as i64,
                    "change_pct": (rng.range(-0.01, 0.01) * 10000.0).round() / 10000.0,
                    "source": "synthetic", "latency_ms": 0,
                });
                let _: Result<(), _> = redis.publish(format!("tick:{sym}"), tick.to_string()).await;
                if rng.unit() < 0.05 {
                    let news = json!({
                        "symbol": sym, "headline": format!("Synthetic news event for {sym}"),
                        "source": "synthetic", "category": "test", "ts": now, "lag_ms": 0,
                    });
                    let _: Result<(), _> =
                        redis.publish(format!("news:{sym}"), news.to_string()).await;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}
