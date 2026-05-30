//! FMP WebSocket upstream (Tier A — crypto + forex) — port of
//! `api/realtime/upstream/fmp_ws.py`.
//!
//! FMP's stock WS endpoint has a server-side SSL chain issue; the crypto and
//! forex subdomains work, so this module runs **two** parallel WS connections:
//!
//!   - `wss://crypto.financialmodelingprep.com`  -> ticker like `btcusd`
//!   - `wss://forex.financialmodelingprep.com`   -> ticker like `eurusd`
//!
//! Both use the same login flow: connect, send
//! `{"event":"login","data":{"apiKey":"<KEY>"}}`, then `subscribe` /
//! `unsubscribe`. Incoming Q frames carry bid/ask quotes.
//!
//! Symbol routing (decided per demand event) — see [`classify`].
//!
//! Demand-driven: the hub demand listener is cheap — it routes (symbol, active)
//! over an unbounded mpsc to the owning feed's worker task, which holds the
//! socket + subscription set and applies subscribe/unsubscribe deltas + the
//! reconnect/backoff loop.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::TimeZone;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;

use crate::realtime::hub::Hub;

const CRYPTO_URL: &str = "wss://crypto.financialmodelingprep.com";
const FOREX_URL: &str = "wss://forex.financialmodelingprep.com";
const TIER_CRYPTO: &str = "A:fmp_crypto";
const TIER_FOREX: &str = "A:fmp_forex";

const BACKOFF_INITIAL: f64 = 1.0;
const BACKOFF_MAX: f64 = 60.0;

const FOREX_CURRENCIES: &[&str] = &[
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNH", "CNY", "HKD", "NZD", "SEK", "NOK",
    "MXN", "ZAR", "SGD", "INR", "KRW", "TRY", "BRL", "RUB", "TWD", "THB", "DKK", "PLN", "HUF",
    "CZK",
];
const CRYPTO_SUFFIXES: &[&str] = &["USD", "USDT", "USDC", "BTC", "ETH"];

/// Return `Some("crypto")`, `Some("forex")`, or `None` (not FMP-claimable).
///
/// Accepts the canonical (UPPER) symbol used by the hub. Forex pairs win over
/// crypto when the symbol is two valid currency codes (e.g. EURUSD -> forex,
/// BTCUSD -> crypto since BTC is not a currency).
fn classify(symbol: &str) -> Option<&'static str> {
    if symbol.is_empty() {
        return None;
    }
    let s = symbol.to_uppercase();
    if s.contains(':') {
        return None; // exchange-prefixed (e.g. BINANCE:BTCUSDT) -> Finnhub
    }
    let all_alpha = s.chars().all(|c| c.is_ascii_alphabetic());
    if all_alpha && s.len() == 6 {
        let (base, quote) = s.split_at(3);
        if FOREX_CURRENCIES.contains(&base) && FOREX_CURRENCIES.contains(&quote) {
            return Some("forex");
        }
    }
    if all_alpha && (6..=8).contains(&s.len()) && CRYPTO_SUFFIXES.iter().any(|suf| s.ends_with(suf))
    {
        return Some("crypto");
    }
    None
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn epoch_ms_to_iso(t_ms: i64) -> String {
    let secs = t_ms.div_euclid(1000);
    let millis = t_ms.rem_euclid(1000) as u32;
    match chrono::Utc.timestamp_opt(secs, millis * 1_000_000) {
        chrono::LocalResult::Single(dt) => {
            dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        }
        _ => crate::realtime::hub::now_iso(),
    }
}

/// One demand event routed to a feed worker.
struct Demand {
    symbol: String,
    active: bool,
}

/// Spawn a single FMP WS feed (crypto or forex). Returns the sender the demand
/// listener uses to push (symbol, active) deltas to the worker.
fn spawn_feed(
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    name: &'static str,
    url: &'static str,
    tier: &'static str,
    token: String,
    slot_cap: usize,
) -> UnboundedSender<Demand> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Demand>();
    let slot_cap = slot_cap.max(1);
    tokio::spawn(async move {
        // The worker owns the desired-subscription set; the WS connection is
        // (re)built on each reconnect and re-subscribes the desired set.
        let mut desired: HashSet<String> = HashSet::new();
        let mut backoff = BACKOFF_INITIAL;

        loop {
            tracing::info!("FMP {name}: connecting...");
            match connect_and_run(
                &hub,
                redis.clone(),
                name,
                url,
                tier,
                &token,
                slot_cap,
                &mut desired,
                &mut rx,
                &mut backoff,
            )
            .await
            {
                Ok(()) => {
                    // Clean stop (channel closed) — nothing more to do.
                    return;
                }
                Err(e) => {
                    tracing::warn!("FMP {name} WS dropped: {e}");
                }
            }
            // Reconnect with jittered exponential backoff.
            let jitter = backoff * 0.5 * rand_unit();
            let nap = backoff + jitter;
            tracing::info!("FMP {name} reconnect in {nap:.1}s");
            sleep(Duration::from_secs_f64(nap)).await;
            backoff = (backoff * 2.0).min(BACKOFF_MAX);
        }
    });
    tx
}

/// Cheap, dependency-free unit-interval pseudo-random for backoff jitter.
fn rand_unit() -> f64 {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (n % 1000) as f64 / 1000.0
}

/// Single connection lifecycle: connect, login, re-subscribe `desired`, then
/// pump frames + drain demand deltas until the socket drops or the demand
/// channel closes. Returns `Ok(())` only when the demand channel is closed
/// (clean shutdown); a dropped socket returns `Err` so the caller reconnects.
#[allow(clippy::too_many_arguments)]
async fn connect_and_run(
    hub: &Arc<Hub>,
    mut redis: redis::aio::MultiplexedConnection,
    name: &'static str,
    url: &'static str,
    tier: &'static str,
    token: &str,
    slot_cap: usize,
    desired: &mut HashSet<String>,
    rx: &mut mpsc::UnboundedReceiver<Demand>,
    backoff: &mut f64,
) -> anyhow::Result<()> {
    let (ws_stream, _) = timeout(Duration::from_secs(10), tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout"))??;
    let (mut writer, mut reader) = ws_stream.split();

    // Login.
    let login = json!({"event": "login", "data": {"apiKey": token}});
    writer.send(Message::Text(login.to_string())).await?;

    // Wait for the login ack before any subscribes.
    let ack = timeout(Duration::from_secs(10), reader.next()).await;
    match ack {
        Ok(Some(Ok(Message::Text(raw)))) => {
            let msg: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
            let ok = msg.get("event").and_then(|v| v.as_str()) == Some("login")
                && msg.get("status").and_then(|v| v.as_i64()) == Some(200);
            if ok {
                tracing::info!("FMP {name} authenticated");
                // Reset backoff after a healthy authenticated connection so a
                // later transient drop reconnects fast (mirrors Python).
                *backoff = BACKOFF_INITIAL;
            } else {
                tracing::error!("FMP {name} login failed: {msg}");
                sleep(Duration::from_secs(5)).await;
                return Err(anyhow::anyhow!("login rejected"));
            }
        }
        Ok(Some(Ok(_))) => {
            tracing::warn!("FMP {name} login no-ack: unexpected frame");
            sleep(Duration::from_secs(5)).await;
            return Err(anyhow::anyhow!("login no-ack"));
        }
        Ok(Some(Err(e))) => {
            return Err(anyhow::anyhow!("login recv error: {e}"));
        }
        Ok(None) | Err(_) => {
            tracing::warn!("FMP {name} login no-ack (timeout/closed)");
            sleep(Duration::from_secs(5)).await;
            return Err(anyhow::anyhow!("login no-ack"));
        }
    }

    // Re-subscribe everything we believe we want (capped at slot_cap), and set
    // the tier for each claimed symbol.
    let mut subscribed: HashSet<String> = HashSet::new();
    let to_sub: Vec<String> = desired.iter().take(slot_cap).cloned().collect();
    for sym in to_sub {
        let wire = sym.to_lowercase();
        writer
            .send(Message::Text(
                json!({"event": "subscribe", "data": {"ticker": wire}}).to_string(),
            ))
            .await?;
        subscribed.insert(sym.clone());
        hub.set_tier(&sym, tier).await;
        tracing::info!(
            "FMP {name}: subscribed {sym} (slots={}/{slot_cap})",
            subscribed.len()
        );
    }

    loop {
        tokio::select! {
            // Demand deltas — apply against the live socket.
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    // Channel closed -> clean shutdown.
                    return Ok(());
                };
                if cmd.active {
                    if subscribed.contains(&cmd.symbol) {
                        desired.insert(cmd.symbol.clone());
                        continue;
                    }
                    if subscribed.len() >= slot_cap {
                        // Over-cap demand is NOT recorded (matches Python): do
                        // not let `desired` grow past slot_cap, else the
                        // reconnect re-subscribe picks an arbitrary subset.
                        tracing::debug!(
                            "FMP {name}: slot full ({}/{slot_cap}), skipping {}",
                            subscribed.len(), cmd.symbol
                        );
                        continue;
                    }
                    let wire = cmd.symbol.to_lowercase();
                    if let Err(e) = writer.send(Message::Text(
                        json!({"event": "subscribe", "data": {"ticker": wire}}).to_string(),
                    )).await {
                        tracing::warn!("FMP {name} subscribe send failed for {}: {e}", cmd.symbol);
                        return Err(anyhow::anyhow!("subscribe send failed"));
                    }
                    // Record in `desired` only once actually subscribed, so the
                    // set mirrors the live subscription (always <= slot_cap).
                    desired.insert(cmd.symbol.clone());
                    subscribed.insert(cmd.symbol.clone());
                    hub.set_tier(&cmd.symbol, tier).await;
                    tracing::info!(
                        "FMP {name}: subscribed {} (slots={}/{slot_cap})",
                        cmd.symbol, subscribed.len()
                    );
                } else {
                    desired.remove(&cmd.symbol);
                    if !subscribed.contains(&cmd.symbol) {
                        continue;
                    }
                    let wire = cmd.symbol.to_lowercase();
                    let _ = writer.send(Message::Text(
                        json!({"event": "unsubscribe", "data": {"ticker": wire}}).to_string(),
                    )).await;
                    subscribed.remove(&cmd.symbol);
                    // Relinquish the tier label if we still own it — clear the
                    // key (no spurious tier_change frame), mirroring Python's
                    // tier_by_symbol.pop().
                    hub.clear_tier_if(&cmd.symbol, tier).await;
                }
            }
            // Inbound frames.
            frame = reader.next() => {
                match frame {
                    Some(Ok(Message::Text(raw))) => {
                        handle_frame(&mut redis, name, &raw).await;
                    }
                    Some(Ok(Message::Binary(b))) => {
                        if let Ok(raw) = String::from_utf8(b) {
                            handle_frame(&mut redis, name, &raw).await;
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = writer.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Err(anyhow::anyhow!("socket closed"));
                    }
                    Some(Ok(_)) => { /* Pong / frame -> ignore */ }
                    Some(Err(e)) => {
                        return Err(anyhow::anyhow!("recv error: {e}"));
                    }
                }
            }
        }
    }
}

/// Parse one inbound FMP frame and, if it's a data quote, publish a `tick:<SYM>`.
async fn handle_frame(redis: &mut redis::aio::MultiplexedConnection, name: &str, raw: &str) {
    let msg: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return,
    };
    // Login / subscribe acks: {"event":"...", "status":..., "message":"..."}
    if msg.get("event").is_some() {
        if let Some(status) = msg.get("status").and_then(|v| v.as_i64()) {
            if status >= 400 {
                tracing::warn!("FMP {name} ack error: {msg}");
            }
        }
        return;
    }
    // Data frame: {"s":"btcusd","t":..., "type":"Q"|"T", "bp":...,"ap":...}
    let sym_wire = match msg.get("s").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return,
    };
    let t_ms = match msg.get("t").and_then(|v| v.as_i64()) {
        Some(t) => t,
        None => return,
    };
    let sym = sym_wire.to_uppercase();

    let num = |k: &str| msg.get(k).and_then(|v| v.as_f64());
    let bp = num("bp");
    let ap = num("ap");
    let lp = num("lp");
    let ls = num("ls");

    // Mid price preferred; fall through to last or bid. (Python: lp -> ap -> bp.)
    let price = match lp.or(ap).or(bp) {
        Some(p) => p,
        None => return,
    };

    let latency = (now_ms() - t_ms).max(0);
    let tick = json!({
        "symbol": sym,
        "ts": epoch_ms_to_iso(t_ms),
        "price": price,
        "bid": bp,
        "ask": ap,
        "volume": ls,
        "source": format!("tier_a:{name}"),
        "latency_ms": latency,
    });
    let payload = tick.to_string();
    let res: Result<(), _> = redis.publish(format!("tick:{sym}"), payload).await;
    if let Err(e) = res {
        tracing::warn!("FMP {name} redis publish failed: {e}");
    }
}

/// Entry point wired into `realtime::start`. Spawns the crypto + forex feed
/// workers and registers the demand listener that routes symbols to them.
///
/// MUST register its demand listener BEFORE finnhub so it claims crypto/forex
/// first (the orchestrator calls this `start` before finnhub's).
pub async fn start(
    hub: std::sync::Arc<crate::realtime::hub::Hub>,
    redis: redis::aio::MultiplexedConnection,
    settings: std::sync::Arc<crate::config::Settings>,
) -> anyhow::Result<()> {
    let token = settings.fmp_key.clone();
    if token.is_empty() {
        tracing::warn!("FINDATA_FMP_KEY empty; FMP WS upstream disabled");
        return Ok(());
    }
    let slot_cap = settings.rt_tier_a_fmp_cap;

    let crypto_tx = spawn_feed(
        hub.clone(),
        redis.clone(),
        "crypto",
        CRYPTO_URL,
        TIER_CRYPTO,
        token.clone(),
        slot_cap,
    );
    let forex_tx = spawn_feed(
        hub.clone(),
        redis.clone(),
        "forex",
        FOREX_URL,
        TIER_FOREX,
        token,
        slot_cap,
    );

    // Demand listener: FMP gets first refusal on crypto/forex-shaped symbols.
    // The closure is cheap — it only routes the delta over the right mpsc.
    let listener: crate::realtime::hub::DemandListener = Arc::new(move |symbol: String, active: bool| {
        let crypto_tx = crypto_tx.clone();
        let forex_tx = forex_tx.clone();
        Box::pin(async move {
            match classify(&symbol) {
                Some("crypto") => {
                    let _ = crypto_tx.send(Demand { symbol, active });
                }
                Some("forex") => {
                    let _ = forex_tx.send(Demand { symbol, active });
                }
                _ => {}
            }
        })
    });
    hub.register_demand_listener(listener).await;

    tracing::info!("FMP WS upstream started (crypto + forex)");
    Ok(())
}
