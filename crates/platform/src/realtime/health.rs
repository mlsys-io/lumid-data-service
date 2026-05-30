//! Realtime upstream health registry — a small shared convention so the
//! generic status board can surface domain-specific feed health without
//! knowing what the feeds are.
//!
//! Each upstream worker reports its connection state into the Redis hash
//! `health:realtime` (field = worker name → JSON `{state, detail, ts}`). The
//! `/status` handler reads the whole hash and renders a "Realtime" section.
//! Best-effort: a Redis error just means no health is shown.

use std::collections::HashMap;

use redis::AsyncCommands;

const HEALTH_KEY: &str = "health:realtime";

/// One reported upstream's health.
pub struct Health {
    pub name: String,
    pub kind: String,   // "connection" (WS link) | "feed" (data flow)
    pub state: String,  // "up" | "down" | "degraded"
    pub detail: String,
    pub ts: String,
}

async fn report_kind(
    mut conn: redis::aio::MultiplexedConnection,
    name: &str,
    kind: &str,
    state: &str,
    detail: &str,
) {
    let v = serde_json::json!({
        "kind": kind,
        "state": state,
        "detail": detail,
        "ts": crate::realtime::hub::now_iso(),
    })
    .to_string();
    let _: Result<(), redis::RedisError> = conn.hset(HEALTH_KEY, name, v).await;
}

/// Report a WS upstream's raw link state (connect-success / login fail / drop).
/// Shows under "Feed connections" on the status board.
pub async fn report(conn: redis::aio::MultiplexedConnection, name: &str, state: &str, detail: &str) {
    report_kind(conn, name, "connection", state, detail).await
}

/// Report a data feed's liveness — call on subscribe AND on each flush so the
/// timestamp tracks data freshness. Shows under "Realtime feeds" (measured by
/// freshness age) on the status board.
pub async fn report_feed(conn: redis::aio::MultiplexedConnection, name: &str, state: &str, detail: &str) {
    report_kind(conn, name, "feed", state, detail).await
}

/// Read all reported upstream-health rows, sorted by name. Empty on any error.
pub async fn read_all(conn: &mut redis::aio::MultiplexedConnection) -> Vec<Health> {
    let map: HashMap<String, String> = conn.hgetall(HEALTH_KEY).await.unwrap_or_default();
    let mut out: Vec<Health> = map
        .into_iter()
        .map(|(name, raw)| {
            let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
            let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
            let kind = { let k = s("kind"); if k.is_empty() { "connection".into() } else { k } };
            Health { name, kind, state: { let st = s("state"); if st.is_empty() { "?".into() } else { st } }, detail: s("detail"), ts: s("ts") }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}
