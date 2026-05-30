//! Finnhub equity WebSocket upstream (Tier A primary) — port of
//! `api/realtime/upstream/finnhub_ws.py`.
//!
//! Single long-lived connection to `wss://ws.finnhub.io?token=<KEY>`.
//! Subscribes/unsubscribes equity symbols based on hub demand events, with a
//! slot cap (`rt_tier_a_finnhub_cap`, default 60 on the Finnhub free tier).
//! Publishes normalized ticks to Redis `tick:<symbol>` and labels the
//! symbol's tier as `A:finnhub` on the hub.
//!
//! ALSO shadow-subscribes crypto/forex pairs on the SAME connection (Finnhub
//! allows only ONE WS per token). Those are published only when the FMP
//! primary feed hasn't published the symbol within `CF_FRESH_WINDOW_S` (8 s)
//! — a hot standby with no duplicate stream in normal operation. Shadow subs
//! never claim the hub tier and don't count against the equity slot cap.
//!
//! Trade frame from Finnhub:
//!   {"type":"trade","data":[{"s":"AAPL","p":233.14,"t":1689178293742,
//!                            "v":100,"c":["12","37"]}, ...]}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_tungstenite::tungstenite::Message;

use findata::config::Settings;
use findata::realtime::hub::Hub;

const TIER_LABEL: &str = "A:finnhub";
const WS_URL_TPL: &str = "wss://ws.finnhub.io?token=";

const BACKOFF_INITIAL_S: f64 = 1.0;
const BACKOFF_MAX_S: f64 = 60.0;

/// Crypto/forex standby freshness window: stay silent while a fresh
/// non-finnhub tier_a (FMP) tick is present for the symbol.
const CF_FRESH_WINDOW_S: f64 = 8.0;

/// Staleness watchdog: Finnhub's equity WS occasionally goes "subscribed but
/// not delivering" — the socket stays alive but no trade frames arrive. We
/// detect a trade-frame gap during market hours and first re-send subscribes,
/// then force a reconnect.
const WATCHDOG_INTERVAL_S: u64 = 30;
const STALE_RESUB_S: f64 = 120.0; // no trades this long (mkt hours) -> re-send subscribes
const STALE_RECONNECT_S: f64 = 300.0; // still nothing -> force-close to reconnect

const FOREX_CURRENCIES: &[&str] = &[
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNH", "CNY", "HKD", "NZD", "SEK", "NOK",
    "MXN", "ZAR", "SGD", "INR", "KRW", "TRY", "BRL", "RUB", "TWD", "THB", "DKK", "PLN", "HUF",
    "CZK",
];
const CRYPTO_SUFFIXES: &[&str] = &["USD", "USDT", "USDC", "BTC", "ETH"];

/// Classify a canonical (UPPER) hub symbol as "crypto", "forex", or None
/// (not FMP-claimable). Mirrors `fmp_ws.classify`. Forex pairs win over
/// crypto when the symbol is two valid currency codes (EURUSD -> forex,
/// BTCUSD -> crypto since BTC is not a currency).
fn classify(symbol: &str) -> Option<&'static str> {
    if symbol.is_empty() {
        return None;
    }
    let s = symbol.to_uppercase();
    if s.contains(':') {
        return None; // exchange-prefixed (e.g. BINANCE:BTCUSDT) -> Finnhub
    }
    let is_alpha = |t: &str| t.chars().all(|c| c.is_ascii_uppercase());
    if s.len() == 6 && is_alpha(&s) {
        let (base, quote) = s.split_at(3);
        if FOREX_CURRENCIES.contains(&base) && FOREX_CURRENCIES.contains(&quote) {
            return Some("forex");
        }
    }
    if (6..=8).contains(&s.len())
        && is_alpha(&s)
        && CRYPTO_SUFFIXES.iter().any(|suf| s.ends_with(suf))
    {
        return Some("crypto");
    }
    None
}

/// Canonical crypto/forex hub symbol -> Finnhub WS symbol, else None.
/// BTCUSD->BINANCE:BTCUSDT, ETHUSD->BINANCE:ETHUSDT, EURUSD->OANDA:EUR_USD.
fn to_finnhub_cf(sym: &str) -> Option<String> {
    let kind = classify(sym)?;
    let s = sym.to_uppercase();
    if kind == "forex" && s.len() == 6 {
        return Some(format!("OANDA:{}_{}", &s[..3], &s[3..]));
    }
    if kind == "crypto" {
        for suf in ["USDT", "USDC", "USD"] {
            if s.ends_with(suf) {
                let base = &s[..s.len() - suf.len()];
                let binance_quote = match suf {
                    "USDC" => "USDC",
                    _ => "USDT", // USD and USDT both -> USDT on Binance
                };
                return Some(format!("BINANCE:{}{}", base, binance_quote));
            }
        }
    }
    None
}

fn epoch_ms_to_iso(t_ms: i64) -> String {
    let secs = t_ms.div_euclid(1000);
    let nanos = (t_ms.rem_euclid(1000) * 1_000_000) as u32;
    match Utc.timestamp_opt(secs, nanos).single() {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => findata::realtime::hub::now_iso(),
    }
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Rough US-equity regular+early hours gate in UTC: Mon-Fri, ~13:00-20:30 UTC
/// (09:00-16:30 ET). Intentionally loose — a false positive just triggers a
/// harmless re-subscribe; ignores holidays.
fn is_market_hours() -> bool {
    use chrono::{Datelike, Timelike, Weekday};
    let now = Utc::now();
    if matches!(now.weekday(), Weekday::Sat | Weekday::Sun) {
        return false;
    }
    let mins = now.hour() * 60 + now.minute();
    (13 * 60..=20 * 60 + 30).contains(&mins)
}

/// Demand event delivered from the hub listener closure to the worker task.
struct DemandMsg {
    symbol: String,
    active: bool,
}

pub async fn start(
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    settings: Arc<Settings>,
) -> Result<()> {
    let token = settings.finnhub_key.clone();
    if token.is_empty() {
        tracing::warn!("FINDATA_FINNHUB_KEY empty; Finnhub WS upstream disabled");
        return Ok(());
    }
    let slot_cap = settings.rt_tier_a_finnhub_cap.max(1);

    let (tx, rx) = mpsc::unbounded_channel::<DemandMsg>();

    // Cheap demand listener: just forward (symbol, active) to the worker.
    let listener: findata::realtime::hub::DemandListener = Arc::new(move |symbol: String, active: bool| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(DemandMsg { symbol, active });
        })
    });
    hub.register_demand_listener(listener).await;

    tokio::spawn(async move {
        let mut worker = Worker::new(hub, redis, token, slot_cap);
        worker.run(rx).await;
    });

    tracing::info!("Finnhub WS upstream started (slots={slot_cap})");
    Ok(())
}

struct Worker {
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    token: String,
    slot_cap: usize,
    /// Equity symbols we believe we hold on the wire.
    subscribed: HashSet<String>,
    /// Crypto/forex standby: finnhub_sym -> canonical hub sym.
    shadow: HashMap<String, String>,
    /// finnhub_syms currently subscribed on the wire (standby).
    shadow_subscribed: HashSet<String>,
    /// Last EQUITY trade time (watchdog liveness only — crypto/forex 24/7
    /// frames would mask an equity drift during market hours).
    last_trade: Instant,
}

impl Worker {
    fn new(
        hub: Arc<Hub>,
        redis: redis::aio::MultiplexedConnection,
        token: String,
        slot_cap: usize,
    ) -> Self {
        Self {
            hub,
            redis,
            token,
            slot_cap,
            subscribed: HashSet::new(),
            shadow: HashMap::new(),
            shadow_subscribed: HashSet::new(),
            last_trade: Instant::now(),
        }
    }

    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<DemandMsg>) {
        let mut backoff = BACKOFF_INITIAL_S;
        loop {
            let url = format!("{WS_URL_TPL}{}", self.token);
            tracing::info!("Finnhub WS connecting...");
            match self.run_connection(&url, &mut rx).await {
                Ok(()) => {} // clean stop (rx closed) — should not normally happen
                Err(e) => tracing::warn!("Finnhub WS dropped: {e}"),
            }
            if rx.is_closed() {
                break;
            }
            let jitter = backoff * 0.5 * rand_unit();
            let sleep = backoff + jitter;
            tracing::info!("Finnhub WS reconnect in {sleep:.1}s");
            tokio::time::sleep(Duration::from_secs_f64(sleep)).await;
            backoff = (backoff * 2.0).min(BACKOFF_MAX_S);
        }
    }

    /// One connection lifetime: connect, re-subscribe everything, then pump
    /// messages + demand events + watchdog until the socket drops.
    async fn run_connection(
        &mut self,
        url: &str,
        rx: &mut mpsc::UnboundedReceiver<DemandMsg>,
    ) -> Result<()> {
        let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
        let (mut write, mut read) = ws.split();

        // On (re)connect, re-subscribe everything we believed we had.
        let to_resub: Vec<String> = self.subscribed.drain().collect();
        for sym in to_resub {
            if self.subscribed.len() >= self.slot_cap {
                break;
            }
            // Skip symbols claimed by another Tier A upstream (FMP crypto/forex).
            let tier = self.hub.get_tier(&sym).await;
            if tier.starts_with("A:") && !tier.starts_with("A:finnhub") {
                continue;
            }
            send_sub(&mut write, "subscribe", &sym).await?;
            self.subscribed.insert(sym.clone());
            self.hub.set_tier(&sym, TIER_LABEL).await;
        }
        // Re-subscribe crypto/forex shadows on this connection.
        self.shadow_subscribed.clear();
        let shadows: Vec<String> = self.shadow.keys().cloned().collect();
        for fh in shadows {
            send_sub(&mut write, "subscribe", &fh).await?;
            self.shadow_subscribed.insert(fh);
        }

        self.last_trade = Instant::now();
        let mut watchdog = interval(Duration::from_secs(WATCHDOG_INTERVAL_S));
        watchdog.tick().await; // consume the immediate first tick
        let mut resubbed_at: Option<Instant> = None;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(txt))) => {
                            if self.handle(&txt, &mut write).await? {
                                // returned true means socket should close (none currently)
                            }
                        }
                        Some(Ok(Message::Binary(b))) => {
                            if let Ok(txt) = String::from_utf8(b) {
                                self.handle(&txt, &mut write).await?;
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            write.send(Message::Pong(p)).await?;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            return Ok(());
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(e.into()),
                    }
                }
                demand = rx.recv() => {
                    match demand {
                        Some(d) => self.on_demand(d, &mut write).await?,
                        None => return Ok(()), // channel closed -> stop
                    }
                }
                _ = watchdog.tick() => {
                    // Only meaningful while holding equity subs in mkt hours.
                    if self.subscribed.is_empty() || !is_market_hours() {
                        continue;
                    }
                    let gap = self.last_trade.elapsed().as_secs_f64();
                    if gap > STALE_RECONNECT_S {
                        tracing::warn!(
                            "Finnhub WS watchdog: {gap:.0}s without trades during \
                             market hours — forcing reconnect");
                        // Drop the connection; run() will reconnect. Held subs
                        // stay in self.subscribed and get re-subscribed.
                        return Ok(());
                    } else if gap > STALE_RESUB_S
                        && resubbed_at.map(|t| t.elapsed().as_secs_f64() > STALE_RESUB_S).unwrap_or(true)
                    {
                        tracing::warn!(
                            "Finnhub WS watchdog: {gap:.0}s without trades — \
                             re-sending {} subscribes", self.subscribed.len());
                        let held: Vec<String> = self.subscribed.iter().cloned().collect();
                        for sym in held {
                            // Re-apply the cross-tier guard: don't re-subscribe
                            // a symbol another Tier-A upstream now owns.
                            let tier = self.hub.get_tier(&sym).await;
                            if tier.starts_with("A:") && !tier.starts_with("A:finnhub") {
                                continue;
                            }
                            send_sub(&mut write, "subscribe", &sym).await?;
                        }
                        resubbed_at = Some(Instant::now());
                    }
                }
            }
        }
    }

    /// Apply a demand event. Returns when subscribe/unsubscribe deltas are
    /// flushed to the socket.
    async fn on_demand<S>(&mut self, d: DemandMsg, write: &mut S) -> Result<()>
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let symbol = d.symbol;
        // Crypto/forex: shadow-subscribe on this same connection as a standby.
        if let Some(fh) = to_finnhub_cf(&symbol) {
            if d.active {
                let canon = symbol.to_uppercase();
                self.shadow.insert(fh.clone(), canon);
                if !self.shadow_subscribed.contains(&fh) {
                    send_sub(write, "subscribe", &fh).await?;
                    self.shadow_subscribed.insert(fh.clone());
                    tracing::info!(
                        "Finnhub: shadow-subscribed {fh} (standby for {})",
                        self.shadow.get(&fh).map(String::as_str).unwrap_or(""));
                }
            }
            return Ok(());
        }

        if d.active {
            if self.subscribed.contains(&symbol) {
                return Ok(());
            }
            if self.subscribed.len() >= self.slot_cap {
                tracing::debug!(
                    "Finnhub: slot full ({}/{}); skipping {symbol}",
                    self.subscribed.len(), self.slot_cap);
                return Ok(());
            }
            // Skip symbols already claimed by another Tier A upstream.
            let tier = self.hub.get_tier(&symbol).await;
            if tier.starts_with("A:") && !tier.starts_with("A:finnhub") {
                return Ok(());
            }
            send_sub(write, "subscribe", &symbol).await?;
            self.subscribed.insert(symbol.clone());
            self.hub.set_tier(&symbol, TIER_LABEL).await;
            tracing::info!(
                "Finnhub: subscribed {symbol} (slots={}/{})",
                self.subscribed.len(), self.slot_cap);
        } else {
            if !self.subscribed.contains(&symbol) {
                return Ok(());
            }
            send_sub(write, "unsubscribe", &symbol).await?;
            self.subscribed.remove(&symbol);
            // Relinquish the tier label if we still own it (no tier_change
            // frame), mirroring Python's tier_by_symbol.pop().
            self.hub.clear_tier_if(&symbol, TIER_LABEL).await;
        }
        Ok(())
    }

    /// Handle one inbound text frame. Returns Ok(false) normally.
    async fn handle<S>(&mut self, raw: &str, write: &mut S) -> Result<bool>
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let msg: Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(_) => return Ok(false),
        };
        match msg.get("type").and_then(|v| v.as_str()) {
            Some("trade") => {
                let mut saw_equity = false;
                if let Some(arr) = msg.get("data").and_then(|v| v.as_array()) {
                    for trade in arr {
                        let s = trade.get("s").and_then(|v| v.as_str()).unwrap_or("");
                        if self.shadow.contains_key(s) {
                            self.emit_cf_tick(trade).await;
                        } else {
                            saw_equity = true;
                            self.emit_tick(trade).await;
                        }
                    }
                }
                if saw_equity {
                    self.last_trade = Instant::now();
                }
            }
            Some("ping") => {
                // Finnhub sometimes sends app-level pings; respond on app level.
                let _ = write.send(Message::Text(json!({"type": "pong"}).to_string())).await;
            }
            Some("error") => {
                tracing::warn!("Finnhub WS error: {msg}");
            }
            _ => {} // subscribe acks etc. ignored
        }
        Ok(false)
    }

    async fn emit_tick(&mut self, trade: &Value) {
        let sym = match trade.get("s").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };
        let price = match trade.get("p").and_then(|v| v.as_f64()) {
            Some(p) => p,
            None => return,
        };
        let ts_ms = match trade.get("t").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => return,
        };
        let volume = trade.get("v").and_then(|v| v.as_f64());
        let latency = (now_ms() - ts_ms).max(0);
        let tick = json!({
            "symbol": sym,
            "ts": epoch_ms_to_iso(ts_ms),
            "price": price,
            "volume": volume,
            "source": "tier_a:us",
            "latency_ms": latency,
        });
        let _: Result<(), _> = self
            .redis
            .publish(format!("tick:{sym}"), tick.to_string())
            .await;
    }

    /// Publish a crypto/forex standby tick — ONLY if the FMP primary feed
    /// hasn't published this symbol within CF_FRESH_WINDOW_S (no dup stream).
    async fn emit_cf_tick(&mut self, trade: &Value) {
        let fh = trade.get("s").and_then(|v| v.as_str()).unwrap_or("");
        let sym = match self.shadow.get(fh) {
            Some(s) => s.clone(),
            None => return,
        };
        let price = match trade.get("p").and_then(|v| v.as_f64()) {
            Some(p) => p,
            None => return,
        };
        let ts_ms = match trade.get("t").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => return,
        };
        // Standby gate: stay silent while a fresh non-finnhub tier_a (FMP)
        // tick is present for this symbol.
        let cached: Option<String> = self
            .redis
            .hget(format!("last:tick:{sym}"), "payload")
            .await
            .ok();
        if let Some(cached) = cached {
            if let Ok(last) = serde_json::from_str::<Value>(&cached) {
                let src = last.get("source").and_then(|v| v.as_str()).unwrap_or("");
                if src.starts_with("tier_a:") && !src.contains("finnhub") {
                    if let Some(ts) = last.get("ts").and_then(|v| v.as_str()) {
                        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(ts) {
                            let age = (Utc::now() - parsed.with_timezone(&Utc))
                                .num_milliseconds() as f64
                                / 1000.0;
                            if (0.0..=CF_FRESH_WINDOW_S).contains(&age) {
                                return;
                            }
                        }
                    }
                }
            }
        }
        let kind = classify(&sym);
        let volume = trade.get("v").and_then(|v| v.as_f64());
        let latency = (now_ms() - ts_ms).max(0);
        let source = match kind {
            Some(k) => format!("tier_a:finnhub_{k}"),
            None => "tier_a:finnhub_cf".to_string(),
        };
        let tick = json!({
            "symbol": sym,
            "ts": epoch_ms_to_iso(ts_ms),
            "price": price,
            "volume": volume,
            "source": source,
            "latency_ms": latency,
        });
        let _: Result<(), _> = self
            .redis
            .publish(format!("tick:{sym}"), tick.to_string())
            .await;
    }
}

async fn send_sub<S>(write: &mut S, event: &str, symbol: &str) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let frame = json!({"type": event, "symbol": symbol}).to_string();
    write.send(Message::Text(frame)).await?;
    Ok(())
}

/// Cheap uniform [0,1) without pulling in the `rand` crate.
fn rand_unit() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (n % 1_000_000) as f64 / 1_000_000.0
}
