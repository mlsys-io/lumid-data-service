//! News upstream — periodic Finnhub company-news poll, dedup, fan-out.
//!
//! Port of `api/realtime/upstream/news.py`.
//!
//! For each actively-subscribed symbol the hub knows about, this worker polls
//! Finnhub's REST `company-news` every `rt_news_poll_sec` (default 60s) for the
//! last 24h, dedups against a per-symbol seen-id sliding window in Redis, and
//! publishes new items to channel `news:<symbol>`. The hub's pub/sub listener
//! forwards `news:<symbol>` frames to every subscribed connection, so this
//! module has zero contract with WS / SSE — it just feeds the bus.
//!
//! Demand-driven: a cheap demand-listener closure forwards `(symbol, active)`
//! transitions over an mpsc channel to a controller task that owns the
//! per-symbol polling tasks (spawn on 0->1, abort on 1->0). News polling makes
//! sense only for symbols Finnhub carries company-news for, so exchange-prefixed
//! (`:`) and FX/crypto pair-shaped symbols are skipped.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use futures_util::future::FutureExt;
use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::Settings;
use crate::realtime::hub::{now_iso, Hub};

const FINNHUB_COMPANY_NEWS: &str = "https://finnhub.io/api/v1/company-news";
/// Remember article ids for 6 hours per symbol.
const SEEN_TTL_SEC: i64 = 6 * 3600;
/// Lookback window for each poll.
const LOOKBACK_HOURS: i64 = 24;
/// Keep at most the newest N ids in each seen-set.
const SEEN_KEEP: isize = 1000;

pub async fn start(
    hub: Arc<Hub>,
    redis: redis::aio::MultiplexedConnection,
    settings: Arc<Settings>,
) -> anyhow::Result<()> {
    let finnhub_key = settings.finnhub_key.clone();
    if finnhub_key.is_empty() {
        tracing::warn!("FINDATA_FINNHUB_KEY empty; news upstream disabled");
        return Ok(());
    }
    let poll_sec = settings.rt_news_poll_sec.max(10);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()?;

    let (tx, rx) = mpsc::unbounded_channel::<(String, bool)>();

    // Controller task: owns per-symbol polling tasks; applies demand deltas.
    tokio::spawn(controller(rx, redis, client, finnhub_key, poll_sec));

    // Cheap demand listener: just forward the transition to the controller.
    let listener: crate::realtime::hub::DemandListener = Arc::new(move |sym: String, active: bool| {
        let tx = tx.clone();
        async move {
            let _ = tx.send((sym, active));
        }
        .boxed()
    });
    hub.register_demand_listener(listener).await;

    tracing::info!(
        "news upstream started (poll={}s, lookback={}h)",
        poll_sec,
        LOOKBACK_HOURS
    );
    Ok(())
}

/// Long-running controller: spawns one polling task per demanded symbol, aborts
/// it when demand drops to zero.
async fn controller(
    mut rx: mpsc::UnboundedReceiver<(String, bool)>,
    redis: redis::aio::MultiplexedConnection,
    client: reqwest::Client,
    finnhub_key: String,
    poll_sec: u64,
) {
    let mut tasks: HashMap<String, JoinHandle<()>> = HashMap::new();
    while let Some((symbol, active)) = rx.recv().await {
        if active {
            if tasks.contains_key(&symbol) {
                continue;
            }
            // News polling only makes sense for symbols Finnhub recognizes.
            // Skip exchange-prefixed crypto (`:`) and FMP-side FX/crypto
            // pair-shaped symbols Finnhub doesn't carry company-news for.
            if symbol.contains(':') || is_pair_shaped(&symbol) {
                continue;
            }
            let handle = tokio::spawn(poll_loop(
                symbol.clone(),
                redis.clone(),
                client.clone(),
                finnhub_key.clone(),
                poll_sec,
            ));
            tasks.insert(symbol.clone(), handle);
            tracing::info!("news: started polling for {symbol}");
        } else if let Some(h) = tasks.remove(&symbol) {
            h.abort();
            tracing::info!("news: stopped polling for {symbol}");
        }
    }
}

fn is_pair_shaped(symbol: &str) -> bool {
    let s = symbol.to_uppercase();
    if matches!(s.len(), 6 | 7 | 8) && s.chars().all(|c| c.is_ascii_alphabetic()) {
        return s.ends_with("USD")
            || s.ends_with("USDT")
            || s.ends_with("USDC")
            || s.ends_with("BTC")
            || s.ends_with("ETH");
    }
    false
}

async fn poll_loop(
    symbol: String,
    mut redis: redis::aio::MultiplexedConnection,
    client: reqwest::Client,
    finnhub_key: String,
    poll_sec: u64,
) {
    let interval = std::time::Duration::from_secs(poll_sec);
    loop {
        fetch_once(&symbol, &mut redis, &client, &finnhub_key).await;
        tokio::time::sleep(interval).await;
    }
}

async fn fetch_once(
    symbol: &str,
    redis: &mut redis::aio::MultiplexedConnection,
    client: &reqwest::Client,
    finnhub_key: &str,
) {
    let to_d = Utc::now().date_naive();
    let days = ((LOOKBACK_HOURS + 23) / 24).max(1);
    let from_d = to_d - chrono::Duration::days(days);

    let resp = match client
        .get(FINNHUB_COMPANY_NEWS)
        .query(&[
            ("symbol", symbol),
            ("from", &from_d.to_string()),
            ("to", &to_d.to_string()),
            ("token", finnhub_key),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("news fetch error for {symbol}: {e}");
            return;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!("news fetch {} for {symbol}", resp.status().as_u16());
        return;
    }
    let items: Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return,
    };
    let Some(items) = items.as_array() else {
        return;
    };

    let seen_key = format!("news:seen:{symbol}");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // Drop ids older than SEEN_TTL_SEC from the sorted set.
    let _: Result<i64, _> = redis
        .zrembyscore(&seen_key, 0i64, now - SEEN_TTL_SEC)
        .await;

    let mut new_count = 0usize;
    for it in items {
        let Some(obj) = it.as_object() else {
            continue;
        };
        let article_id = obj
            .get("id")
            .and_then(value_to_id)
            .or_else(|| obj.get("url").and_then(|v| v.as_str().map(str::to_string)))
            .unwrap_or_default();
        if article_id.is_empty() {
            continue;
        }
        // NX add: only insert if absent; CH makes the reply count newly-added
        // members. 1 => new, 0 => already seen.
        let added: i64 = redis::cmd("ZADD")
            .arg(&seen_key)
            .arg("NX")
            .arg("CH")
            .arg(now)
            .arg(&article_id)
            .query_async(&mut *redis)
            .await
            .unwrap_or(0);
        if added == 0 {
            continue; // already seen
        }
        new_count += 1;

        let ts_unix = obj.get("datetime").and_then(|v| v.as_i64()).unwrap_or(0);
        let ts_iso = if ts_unix > 0 {
            Utc.timestamp_opt(ts_unix, 0)
                .single()
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                .unwrap_or_else(now_iso)
        } else {
            now_iso()
        };
        let lag_ms: Value = if ts_unix > 0 {
            json!(((now - ts_unix) * 1000).max(0))
        } else {
            Value::Null
        };

        let payload = json!({
            "symbol": symbol,
            "headline": obj.get("headline").and_then(|v| v.as_str()).unwrap_or(""),
            "source": obj.get("source").and_then(|v| v.as_str()).unwrap_or("wire"),
            "category": obj.get("category").cloned().unwrap_or(Value::Null),
            "url": obj.get("url").cloned().unwrap_or(Value::Null),
            "ts": ts_iso,
            "lag_ms": lag_ms,
        });
        let _: Result<(), _> = redis
            .publish(format!("news:{symbol}"), payload.to_string())
            .await;
    }

    // Cap the seen-set so it doesn't grow unbounded. No EXPIRE — an empty zset
    // must survive memory pressure or duplicates resurface (Redis
    // maxmemory-policy=volatile-lru).
    let _: Result<i64, _> = redis
        .zremrangebyrank(&seen_key, 0, -(SEEN_KEEP + 1))
        .await;

    if new_count > 0 {
        tracing::info!("news[{symbol}]: {new_count} new items published");
    }
}

/// Coerce a Finnhub article `id` (number or string) into a stable string key.
fn value_to_id(v: &Value) -> Option<String> {
    match v {
        // Match Python's `str(id or url or "")`: a falsy 0 id falls through to
        // the url, so treat numeric zero as absent.
        Value::Number(n) if n.as_i64() != Some(0) && n.as_f64() != Some(0.0) => {
            Some(n.to_string())
        }
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}
