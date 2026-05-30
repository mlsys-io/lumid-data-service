//! Per-process fan-out hub for realtime streams — port of
//! `api/realtime/hub.py`.
//!
//! Owns the active connection set, a per-symbol subscriber index, the tier
//! label per symbol, demand listeners (notified on 0->1 / 1->0 subscriber
//! transitions so upstream workers can claim/release a symbol), and a single
//! background listener task.
//!
//! Redis model: the Python hub dynamically SUBSCRIBEs `tick:<sym>` etc. per
//! demanded symbol. redis-rs makes dynamic (un)subscription on a live consumer
//! awkward, so this port instead `PSUBSCRIBE`s `tick:* news:* kol:*` once and
//! fans out by the local `subs_by_symbol` index. In a single-process,
//! demand-gated deployment (upstreams only publish symbols the hub asked for)
//! the message volume is identical — only symbols with subscribers ever flow.
//!
//! Slow-client safeguard: each connection has a bounded queue (default 100);
//! on overflow the OLDEST frame is dropped and the client is notified at most
//! once per 30 s.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify};

const DROP_WARN_INTERVAL: Duration = Duration::from_secs(30);
const LAG_BUF_CAP: usize = 1024;
const LAST_TICK_TTL_S: i64 = 3600;

pub fn now_iso() -> String {
    Utc::now()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Short, unique-enough connection id (timestamp nanos + atomic counter).
pub fn gen_conn_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:08x}{:04x}", t & 0xffff_ffff, n & 0xffff)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConnKind {
    Ws,
    Sse,
}

/// One client connection (WS or SSE). Frames are pushed by the hub listener
/// (and heartbeat task) into a bounded deque; the transport's sender task
/// drains it, woken by `notify`.
pub struct Connection {
    pub id: String,
    pub sub: String,
    pub kind: ConnKind,
    queue: Mutex<VecDeque<Value>>,
    pub notify: Notify,
    max_queue: usize,
    pub dropped: AtomicU64,
    pub lifetime_symbols: AtomicU64,
    last_drop_warn: Mutex<Option<Instant>>,
}

impl Connection {
    pub fn new(id: String, sub: String, kind: ConnKind, max_queue: usize) -> Arc<Self> {
        Arc::new(Self {
            id,
            sub,
            kind,
            queue: Mutex::new(VecDeque::with_capacity(max_queue.min(256))),
            notify: Notify::new(),
            max_queue,
            dropped: AtomicU64::new(0),
            lifetime_symbols: AtomicU64::new(0),
            last_drop_warn: Mutex::new(None),
        })
    }

    /// Enqueue a frame; on overflow drop the OLDEST and notify once per 30 s.
    pub async fn push(&self, frame: Value) {
        let mut q = self.queue.lock().await;
        if q.len() >= self.max_queue {
            q.pop_front();
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            q.push_back(frame);
            // Slow-client warning, rate-limited to once per interval.
            let mut warn = self.last_drop_warn.lock().await;
            let now = Instant::now();
            let due = warn.map(|t| now.duration_since(t) > DROP_WARN_INTERVAL).unwrap_or(true);
            if due {
                *warn = Some(now);
                if q.len() >= self.max_queue {
                    q.pop_front();
                }
                q.push_back(json!({
                    "type": "error", "code": "slow_client_dropped",
                    "message": format!("dropped {dropped} frames so far"),
                }));
            }
        } else {
            q.push_back(frame);
        }
        drop(q);
        self.notify.notify_one();
    }

    /// Drain all currently-queued frames (used by the transport sender after a
    /// `notify` wake-up).
    pub async fn drain(&self) -> Vec<Value> {
        let mut q = self.queue.lock().await;
        q.drain(..).collect()
    }
}

/// `cb(symbol, active)` — invoked when a symbol goes 0->1 (active=true) or
/// 1->0 (active=false) subscribers.
pub type DemandListener =
    Arc<dyn Fn(String, bool) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Default)]
struct HubState {
    connections: HashMap<String, Arc<Connection>>,
    subs_by_symbol: HashMap<String, HashMap<String, Arc<Connection>>>,
    connections_by_sub: HashMap<String, Arc<Connection>>,
    tier_by_symbol: HashMap<String, String>,
}

pub struct Hub {
    redis: Mutex<redis::aio::MultiplexedConnection>,
    state: Mutex<HubState>,
    demand_listeners: Mutex<Vec<DemandListener>>,
    lag_buf: Mutex<HashMap<String, VecDeque<i64>>>,
    stream_counts: Mutex<HashMap<String, u64>>,
}

impl Hub {
    pub fn new(redis: redis::aio::MultiplexedConnection) -> Arc<Self> {
        Arc::new(Self {
            redis: Mutex::new(redis),
            state: Mutex::new(HubState::default()),
            demand_listeners: Mutex::new(Vec::new()),
            lag_buf: Mutex::new(HashMap::new()),
            stream_counts: Mutex::new(HashMap::new()),
        })
    }

    /// Spawn the background Redis pub/sub listener. `client` opens a dedicated
    /// pub/sub connection (separate from the multiplexed command connection).
    pub fn start_listener(self: &Arc<Self>, client: redis::Client) {
        let hub = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = hub.clone().run_listener(&client).await {
                    tracing::warn!("hub listener error: {e}; reconnecting in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
        tracing::info!("realtime hub listener started");
    }

    async fn run_listener(self: Arc<Self>, client: &redis::Client) -> redis::RedisResult<()> {
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.psubscribe("tick:*").await?;
        pubsub.psubscribe("news:*").await?;
        pubsub.psubscribe("kol:*").await?;
        pubsub.subscribe("findata:rt:control").await?;
        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let channel = msg.get_channel_name().to_string();
            let Some((kind, key)) = channel.split_once(':') else {
                continue;
            };
            if kind != "tick" && kind != "news" && kind != "kol" {
                continue;
            }
            let raw: String = match msg.get_payload() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let payload: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            self.on_payload(kind, key, payload).await;
        }
        Ok(())
    }

    async fn on_payload(&self, kind: &str, key: &str, payload: Value) {
        // Tier C: stash the latest tick so new subscribers get an immediate
        // first frame on subscribe.
        if kind == "tick" && payload.is_object() {
            let mut r = self.redis.lock().await;
            let cache_key = format!("last:tick:{key}");
            let _: Result<(), _> = r.hset(&cache_key, "payload", payload.to_string()).await;
            let _: Result<(), _> = r.expire(&cache_key, LAST_TICK_TTL_S).await;
        }
        // Per-source lag + frame counters.
        if let Some(obj) = payload.as_object() {
            if let Some(src) = obj.get("source").and_then(|v| v.as_str()) {
                let bucket = format!("{kind}:{src}");
                if let Some(lag) = obj
                    .get("latency_ms")
                    .or_else(|| obj.get("lag_ms"))
                    .and_then(|v| v.as_i64())
                {
                    if lag >= 0 {
                        let mut lb = self.lag_buf.lock().await;
                        let buf = lb.entry(bucket.clone()).or_insert_with(VecDeque::new);
                        if buf.len() >= LAG_BUF_CAP {
                            buf.pop_front();
                        }
                        buf.push_back(lag);
                    }
                }
                *self.stream_counts.lock().await.entry(bucket).or_insert(0) += 1;
            }
        }
        let frame = json!({"type": kind, "data": payload});
        let recipients: Vec<Arc<Connection>> = {
            let st = self.state.lock().await;
            st.subs_by_symbol
                .get(key)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default()
        };
        for conn in recipients {
            conn.push(frame.clone()).await;
        }
    }

    // ----- connection registration -----

    /// Register a connection. With `enforce_concurrent`, an existing connection
    /// from the same `sub` is evicted (notified + returned so the caller can
    /// close its transport).
    pub async fn register(
        &self,
        conn: Arc<Connection>,
        enforce_concurrent: bool,
    ) -> Option<Arc<Connection>> {
        let prev = {
            let mut st = self.state.lock().await;
            st.connections.insert(conn.id.clone(), conn.clone());
            st.connections_by_sub.insert(conn.sub.clone(), conn.clone())
        };
        let evicted = match prev {
            Some(p) if enforce_concurrent && p.id != conn.id => {
                p.push(json!({
                    "type": "error", "code": "concurrent_limit",
                    "message": "another stream from this identity took over",
                }))
                .await;
                Some(p)
            }
            _ => None,
        };
        tracing::info!(
            "hub: connection registered sub={} id={} evicted={:?}",
            conn.sub,
            conn.id,
            evicted.as_ref().map(|e| &e.id)
        );
        evicted
    }

    pub async fn unregister(&self, conn: &Arc<Connection>) {
        let mut ended: Vec<String> = Vec::new();
        {
            let mut st = self.state.lock().await;
            st.connections.remove(&conn.id);
            // Drop from every symbol it was subscribed to.
            let syms: Vec<String> = st
                .subs_by_symbol
                .iter()
                .filter(|(_, m)| m.contains_key(&conn.id))
                .map(|(s, _)| s.clone())
                .collect();
            for sym in syms {
                if let Some(m) = st.subs_by_symbol.get_mut(&sym) {
                    m.remove(&conn.id);
                    if m.is_empty() {
                        st.subs_by_symbol.remove(&sym);
                        ended.push(sym);
                    }
                }
            }
            if st.connections_by_sub.get(&conn.sub).map(|c| c.id == conn.id).unwrap_or(false) {
                st.connections_by_sub.remove(&conn.sub);
            }
        }
        for sym in ended {
            self.fire_demand(sym, false);
        }
        tracing::info!(
            "hub: connection unregistered id={} dropped={}",
            conn.id,
            conn.dropped.load(Ordering::Relaxed)
        );
    }

    // ----- demand listeners + tier -----

    pub async fn register_demand_listener(&self, cb: DemandListener) {
        self.demand_listeners.lock().await.push(cb);
    }

    pub async fn set_tier(&self, symbol: &str, tier: &str) {
        let (changed, recipients) = {
            let mut st = self.state.lock().await;
            let prev = st.tier_by_symbol.insert(symbol.to_string(), tier.to_string());
            let changed = prev.as_deref().map(|p| p != tier).unwrap_or(false);
            let rec: Vec<Arc<Connection>> = if changed {
                st.subs_by_symbol
                    .get(symbol)
                    .map(|m| m.values().cloned().collect())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            (changed, rec)
        };
        if changed {
            for c in recipients {
                c.push(json!({"type": "tier_change", "symbol": symbol, "tier": tier}))
                    .await;
            }
        }
    }

    pub async fn get_tier(&self, symbol: &str) -> String {
        self.state
            .lock()
            .await
            .tier_by_symbol
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| "B".to_string())
    }

    fn fire_demand(&self, symbol: String, active: bool) {
        // Snapshot listeners and run them in registration order in a detached
        // task so a slow upstream doesn't block the hub.
        let listeners = self.demand_listeners.try_lock().map(|g| g.clone());
        if let Ok(listeners) = listeners {
            if listeners.is_empty() {
                return;
            }
            tokio::spawn(async move {
                for cb in listeners {
                    cb(symbol.clone(), active).await;
                }
            });
        }
    }

    // ----- subscribe / unsubscribe -----

    /// Add `symbols` to the connection. Returns {symbol: tier_label}.
    pub async fn subscribe(
        &self,
        conn: &Arc<Connection>,
        symbols: &[String],
    ) -> HashMap<String, String> {
        let mut out = HashMap::new();
        let mut new_demand: Vec<String> = Vec::new();
        {
            let mut st = self.state.lock().await;
            for sym in symbols {
                let entry = st.subs_by_symbol.entry(sym.clone()).or_default();
                let first = entry.is_empty();
                if !entry.contains_key(&conn.id) {
                    entry.insert(conn.id.clone(), conn.clone());
                    conn.lifetime_symbols.fetch_add(1, Ordering::Relaxed);
                }
                if first {
                    new_demand.push(sym.clone());
                }
                let tier = st.tier_by_symbol.get(sym).cloned().unwrap_or_else(|| "B".to_string());
                out.insert(sym.clone(), tier);
            }
        }
        // Tier-C replay outside the lock.
        for sym in symbols {
            self.replay_last(conn, sym).await;
        }
        for sym in new_demand {
            self.fire_demand(sym, true);
        }
        out
    }

    pub async fn unsubscribe(&self, conn: &Arc<Connection>, symbols: &[String]) -> Vec<String> {
        let mut removed = Vec::new();
        let mut ended = Vec::new();
        {
            let mut st = self.state.lock().await;
            for sym in symbols {
                if let Some(m) = st.subs_by_symbol.get_mut(sym) {
                    if m.remove(&conn.id).is_some() {
                        removed.push(sym.clone());
                    }
                    if m.is_empty() {
                        st.subs_by_symbol.remove(sym);
                        ended.push(sym.clone());
                    }
                }
            }
        }
        for sym in ended {
            self.fire_demand(sym, false);
        }
        removed
    }

    async fn replay_last(&self, conn: &Arc<Connection>, symbol: &str) {
        let raw: Option<String> = {
            let mut r = self.redis.lock().await;
            r.hget(format!("last:tick:{symbol}"), "payload").await.ok()
        };
        let Some(raw) = raw else { return };
        let Ok(mut payload) = serde_json::from_str::<Value>(&raw) else {
            return;
        };
        if let Some(obj) = payload.as_object_mut() {
            let src = obj.get("source").and_then(|v| v.as_str()).unwrap_or("");
            let tagged = if src.is_empty() {
                "cache".to_string()
            } else {
                format!("{src}:cache")
            };
            obj.insert("source".into(), Value::String(tagged));
        }
        conn.push(json!({"type": "tick", "data": payload})).await;
    }

    // ----- stats (for /freshness) -----

    pub async fn realtime_stats(&self) -> Value {
        let lag = self.lag_buf.lock().await;
        let counts = self.stream_counts.lock().await;
        let mut by_source = serde_json::Map::new();
        for (key, buf) in lag.iter() {
            if buf.is_empty() {
                continue;
            }
            let mut sorted: Vec<i64> = buf.iter().copied().collect();
            sorted.sort_unstable();
            let n = sorted.len();
            let pct = |p: f64| -> i64 {
                let idx = (((p * (n - 1) as f64).round()) as usize).min(n - 1);
                sorted[idx]
            };
            by_source.insert(
                key.clone(),
                json!({
                    "samples": n,
                    "p50_ms": pct(0.50),
                    "p95_ms": pct(0.95),
                    "p99_ms": pct(0.99),
                    "max_ms": sorted[n - 1],
                    "frames_seen": counts.get(key).copied().unwrap_or(0),
                }),
            );
        }
        let st = self.state.lock().await;
        json!({
            "connections": st.connections.len(),
            "symbols_active": st.subs_by_symbol.len(),
            "by_source": Value::Object(by_source),
        })
    }
}
