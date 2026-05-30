//! Tier-B REST polling fallback — port of `api/realtime/upstream/polling.py`.
//!
//! For symbols that no Tier-A WS upstream claimed (both Finnhub and FMP WS slot
//! caps exhausted, or that neither feed carries), poll the appropriate REST
//! quote endpoint every `rt_tier_b_poll_sec` seconds (min 2) and publish ticks
//! to `tick:<sym>`.
//!
//! Coarser than Tier A but bounded API load, and quota-safe because each symbol
//! is on its own clock.
//!
//! Provider routing (same provider that owns the asset class on the WS side, to
//! keep the data shape consistent):
//!   - crypto / forex shorthand (`BTCUSD`, `EURUSD`) -> FMP `/stable/quote`
//!   - everything else (stocks, exchange-prefixed `BINANCE:BTCUSDT`) -> Finnhub `/quote`
//!
//! Demand-driven: a per-symbol poll task is spawned on the 0->1 transition and
//! cancelled on 1->0. We only claim a symbol if no Tier-A upstream already owns
//! it, and a running loop cedes if a Tier-A upstream claims the symbol later.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::config::Settings;
use crate::realtime::hub::{now_iso, DemandListener, Hub};

const TIER_LABEL: &str = "B";
const FINNHUB_QUOTE: &str = "https://finnhub.io/api/v1/quote";
const FMP_QUOTE: &str = "https://financialmodelingprep.com/stable/quote";

/// ISO-4217 currency codes we recognise for forex-pair classification.
/// Mirrors `_FOREX_CURRENCIES` in `fmp_ws.py`.
const FOREX_CURRENCIES: [&str; 27] = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNH", "CNY", "HKD", "NZD", "SEK", "NOK",
    "MXN", "ZAR", "SGD", "INR", "KRW", "TRY", "BRL", "RUB", "TWD", "THB", "DKK", "PLN", "HUF",
    "CZK",
];
const CRYPTO_SUFFIXES: [&str; 5] = ["USD", "USDT", "USDC", "BTC", "ETH"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Crypto,
    Forex,
    Other,
}

/// Port of `fmp_ws.classify`: returns Crypto / Forex (FMP-claimable) or Other.
/// Forex wins over crypto when the symbol is two valid currency codes (e.g.
/// `EURUSD` -> forex, `BTCUSD` -> crypto since BTC is not a currency).
fn classify(symbol: &str) -> Kind {
    if symbol.is_empty() {
        return Kind::Other;
    }
    let s = symbol.to_ascii_uppercase();
    if s.contains(':') {
        // exchange-prefixed (e.g. BINANCE:BTCUSDT) -> Finnhub
        return Kind::Other;
    }
    let is_alpha = |t: &str| t.chars().all(|c| c.is_ascii_uppercase());
    // Exactly 6 alpha chars that split into two known currencies -> forex.
    if s.len() == 6 && is_alpha(&s) {
        let (base, quote) = s.split_at(3);
        if FOREX_CURRENCIES.contains(&base) && FOREX_CURRENCIES.contains(&quote) {
            return Kind::Forex;
        }
    }
    // 6-8 alpha chars ending in a crypto quote suffix -> crypto.
    if (6..=8).contains(&s.len()) && is_alpha(&s) && CRYPTO_SUFFIXES.iter().any(|suf| s.ends_with(suf)) {
        return Kind::Crypto;
    }
    Kind::Other
}

/// Demand event delivered from the hub listener to the controller task.
struct Demand {
    symbol: String,
    active: bool,
}

/// Parse a JSON number that may arrive as a number or a numeric string.
fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// A truthy (non-null, non-zero) f64, mirroring Python's `if row.get("o")`.
fn truthy_f64(v: Option<&Value>) -> Option<f64> {
    as_f64(v).filter(|x| *x != 0.0)
}

pub async fn start(
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    settings: Arc<Settings>,
) -> anyhow::Result<()> {
    let finnhub_key = settings.finnhub_key.clone();
    let fmp_key = settings.fmp_key.clone();
    if finnhub_key.is_empty() && fmp_key.is_empty() {
        tracing::warn!("polling upstream not started: no API keys configured");
        return Ok(());
    }
    let poll_sec = settings.rt_tier_b_poll_sec.max(2);

    let (tx, rx) = mpsc::unbounded_channel::<Demand>();

    // Cheap demand listener: just forward (symbol, active) to the controller.
    let listener: DemandListener = Arc::new(move |symbol: String, active: bool| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(Demand { symbol, active });
        })
    });
    hub.register_demand_listener(listener).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    tokio::spawn(controller(hub, redis, client, finnhub_key, fmp_key, poll_sec, rx));

    tracing::info!("Tier B polling upstream started (poll={poll_sec}s)");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn controller(
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    client: reqwest::Client,
    finnhub_key: String,
    fmp_key: String,
    poll_sec: u64,
    mut rx: mpsc::UnboundedReceiver<Demand>,
) {
    // symbol -> poll-loop join handle. Owned solely by this task.
    let tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>> = Arc::new(Mutex::new(HashMap::new()));

    while let Some(d) = rx.recv().await {
        if d.active {
            // Only take if not already claimed by a Tier-A upstream.
            let tier = hub.get_tier(&d.symbol).await;
            if tier.starts_with("A:") {
                continue;
            }
            {
                let mut t = tasks.lock().await;
                if t.contains_key(&d.symbol) {
                    continue;
                }
                let handle = tokio::spawn(poll_loop(
                    d.symbol.clone(),
                    hub.clone(),
                    redis.clone(),
                    client.clone(),
                    finnhub_key.clone(),
                    fmp_key.clone(),
                    poll_sec,
                ));
                t.insert(d.symbol.clone(), handle);
            }
            hub.set_tier(&d.symbol, TIER_LABEL).await;
            tracing::info!("Tier B: started polling for {}", d.symbol);
        } else {
            let handle = tasks.lock().await.remove(&d.symbol);
            if let Some(h) = handle {
                h.abort();
                tracing::info!("Tier B: stopped polling for {}", d.symbol);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn poll_loop(
    symbol: String,
    hub: Arc<Hub>,
    mut redis: redis::aio::MultiplexedConnection,
    client: reqwest::Client,
    finnhub_key: String,
    fmp_key: String,
    poll_sec: u64,
) {
    let interval = std::time::Duration::from_secs(poll_sec);
    loop {
        // If a Tier-A upstream picked this up while we were polling, cede.
        let tier = hub.get_tier(&symbol).await;
        if tier.starts_with("A:") {
            tracing::info!("Tier B: yielding {symbol} to {tier}");
            return;
        }
        if let Some(tick) = fetch_once(&symbol, &client, &finnhub_key, &fmp_key).await {
            let payload = tick.to_string();
            let res: Result<(), _> = redis.publish(format!("tick:{symbol}"), payload).await;
            if let Err(e) = res {
                tracing::warn!("polling redis publish failed for {symbol}: {e}");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn fetch_once(
    symbol: &str,
    client: &reqwest::Client,
    finnhub_key: &str,
    fmp_key: &str,
) -> Option<Value> {
    let kind = classify(symbol);
    let t0 = Instant::now();

    if matches!(kind, Kind::Crypto | Kind::Forex) && !fmp_key.is_empty() {
        // FMP REST quote.
        let resp = client
            .get(FMP_QUOTE)
            .query(&[("symbol", symbol.to_ascii_uppercase().as_str()), ("apikey", fmp_key)])
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let data: Value = resp.json().await.ok()?;
        // Response is a list; take the first object (or the object itself).
        let row = match &data {
            Value::Array(arr) => arr.first()?,
            Value::Object(_) => &data,
            _ => return None,
        };
        let obj = row.as_object()?;
        let price = as_f64(obj.get("price")).or_else(|| as_f64(obj.get("close")))?;
        let latency_ms = t0.elapsed().as_millis() as i64;
        return Some(json!({
            "symbol": symbol,
            "ts": now_iso(),
            "price": price,
            "bid": as_f64(obj.get("bid")),
            "ask": as_f64(obj.get("ask")),
            "volume": as_f64(obj.get("volume")),
            "change_pct": as_f64(obj.get("changesPercentage")).map(|p| p / 100.0),
            "source": "tier_b:1",
            "latency_ms": latency_ms,
        }));
    }

    if !finnhub_key.is_empty() {
        let resp = client
            .get(FINNHUB_QUOTE)
            .query(&[("symbol", symbol), ("token", finnhub_key)])
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let data: Value = resp.json().await.ok()?;
        let obj = data.as_object()?;
        // `c` = current price; skip if missing or zero.
        let price = as_f64(obj.get("c")).filter(|p| *p != 0.0)?;
        let latency_ms = t0.elapsed().as_millis() as i64;
        return Some(json!({
            "symbol": symbol,
            "ts": now_iso(),
            "price": price,
            "open": truthy_f64(obj.get("o")),
            "high": truthy_f64(obj.get("h")),
            "low": truthy_f64(obj.get("l")),
            "close": truthy_f64(obj.get("pc")),
            "change_pct": as_f64(obj.get("dp")).map(|p| p / 100.0),
            "source": "tier_b:2",
            "latency_ms": latency_ms,
        }));
    }

    None
}
