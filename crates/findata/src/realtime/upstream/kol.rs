//! KOL (Twitter cashtag) upstream — port of `api/realtime/upstream/kol.py`.
//!
//! Per actively-subscribed symbol, this worker polls twitterapi.io's
//! `advanced_search` for tweets mentioning the symbol's cashtag (`$SYMBOL`)
//! since the last poll, and fans new tweets out on the Redis channel
//! `kol:<symbol>`. The hub's listener routes that channel to subscribed
//! WS/SSE connections.
//!
//! Demand-driven: a hub demand listener spawns a per-symbol poll task on the
//! 0->1 subscriber transition and aborts it on 1->0. Cost-aware: tweets are
//! charged per-1k by twitterapi.io, so we poll on a slow cadence (default
//! 300 s), bound each query with `since_time`, cap retained dedup ids per
//! symbol, and skip raw crypto/forex-shaped tickers Twitter barely covers.
//!
//! Curation: tweets are only published if their author handle is on the active
//! `news.kol_roster` allowlist. The roster snapshot is refreshed in-memory
//! every `ROSTER_REFRESH_SEC` so admins can edit the table without a restart.
//!
//! Env (via `settings`): `twitterapi_key`, `rt_kol_poll_sec`,
//! `rt_kol_max_per_poll`. If the key is empty this worker is a no-op.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use redis::AsyncCommands;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::realtime::hub::Hub;

const ADVANCED_SEARCH: &str = "https://api.twitterapi.io/twitter/tweet/advanced_search";
const SEEN_TTL_SEC: i64 = 24 * 3600;
const ROSTER_REFRESH_SEC: u64 = 300;
const TWEET_CACHE_CAP: isize = 200;
const RECALL_TTL_SEC: i64 = 7 * 86400;

/// Shared per-symbol last-seen unix timestamp so the next query can use
/// `since_time` and avoid re-fetching the same tweets.
type LastSeen = Arc<Mutex<HashMap<String, i64>>>;

struct KolState {
    http: reqwest::Client,
    redis: redis::aio::MultiplexedConnection,
    hub: Arc<Hub>,
    pool: Pool,
    api_key: String,
    poll_sec: u64,
    max_per_poll: usize,
    /// lowercased roster handles + last refresh instant (unix secs).
    roster: Mutex<(HashSet<String>, u64)>,
    last_seen: LastSeen,
    /// Active per-symbol poll tasks, keyed by symbol.
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Twitter's `createdAt` is RFC-2822-ish, e.g. `Tue Dec 10 07:00:30 +0000 2024`.
/// Reformat to `Tue, 10 Dec 2024 07:00:30 +0000` for chrono's rfc2822 parser.
fn parse_created_at(value: &str) -> Option<DateTime<Utc>> {
    if value.is_empty() {
        return None;
    }
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    let (dow, mon, day, t, tz, yr) = (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
    let reformatted = format!("{dow}, {day} {mon} {yr} {t} {tz}");
    DateTime::parse_from_rfc2822(&reformatted)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn iso_millis(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Extract uppercased cashtags (`$AAPL`) from text, sorted + deduped.
fn extract_cashtags(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut set: HashSet<String> = HashSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_alphabetic() {
                j += 1;
            }
            let len = j - start;
            // `\b` after 1..=8 alpha chars: the char at j must not be
            // alphanumeric/underscore (word boundary).
            if (1..=8).contains(&len) {
                let boundary = j >= bytes.len()
                    || !(bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_');
                if boundary {
                    set.insert(text[start..j].to_uppercase());
                }
            }
            i = j.max(i + 1);
        } else {
            i += 1;
        }
    }
    let mut out: Vec<String> = set.into_iter().collect();
    out.sort();
    out
}

/// Mirror of the Python `_twitter_eligible`: skip exchange-prefixed symbols and
/// raw crypto/forex-shaped tickers, where cashtag usage is sparse.
fn twitter_eligible(symbol: &str) -> bool {
    if symbol.contains(':') {
        return false;
    }
    let s = symbol.to_uppercase();
    let len = s.chars().count();
    if !(1..=8).contains(&len) || !s.chars().all(|c| c.is_ascii_alphabetic()) {
        return false;
    }
    if (6..=8).contains(&len)
        && (s.ends_with("USD")
            || s.ends_with("USDT")
            || s.ends_with("USDC")
            || s.ends_with("BTC")
            || s.ends_with("ETH"))
    {
        return false;
    }
    true
}

impl KolState {
    /// Refresh the roster snapshot from `news.kol_roster` if stale (or forced).
    async fn refresh_roster(&self, force: bool) {
        let now = now_unix() as u64;
        {
            let g = self.roster.lock().await;
            if !force && now.saturating_sub(g.1) < ROSTER_REFRESH_SEC {
                return;
            }
        }
        let handles: Option<HashSet<String>> = async {
            let client = self.pool.get().await.ok()?;
            let rows = client
                .query("SELECT lower(handle) FROM news.kol_roster WHERE active", &[])
                .await
                .ok()?;
            Some(rows.iter().filter_map(|r| r.try_get::<_, String>(0).ok()).collect())
        }
        .await;
        match handles {
            Some(set) => {
                let mut g = self.roster.lock().await;
                g.0 = set;
                g.1 = now;
            }
            None => tracing::warn!("KOL roster refresh failed"),
        }
    }

    async fn fetch_once(&self, symbol: &str) {
        // Refresh roster if stale (cheap; one query, in-process cache).
        self.refresh_roster(false).await;

        let now = now_unix();
        let since = {
            let ls = self.last_seen.lock().await;
            ls.get(symbol).copied().unwrap_or(now - self.poll_sec as i64)
        };
        let query = format!("${symbol} since_time:{since}");

        let resp = match self
            .http
            .get(ADVANCED_SEARCH)
            .header("X-API-Key", &self.api_key)
            .query(&[("query", query.as_str()), ("queryType", "Latest")])
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("KOL fetch error for {symbol}: {e}");
                return;
            }
        };
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let snippet: String = body.chars().take(160).collect();
            tracing::warn!("KOL fetch {status} for {symbol}: {snippet}");
            return;
        }
        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!("KOL non-JSON for {symbol}");
                return;
            }
        };
        let tweets = match body.get("tweets").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return,
        };

        let mut redis = self.redis.clone();
        let seen_key = format!("kol:seen:{symbol}");
        let _: Result<(), _> = redis
            .zrembyscore(&seen_key, 0, now - SEEN_TTL_SEC)
            .await;

        // Roster snapshot for the curation gate.
        let roster: HashSet<String> = self.roster.lock().await.0.clone();

        let mut new_count = 0usize;
        let mut max_seen_ts = since;

        for t in tweets.iter().take(self.max_per_poll) {
            if !t.is_object() {
                continue;
            }
            let tid = match t.get("id") {
                Some(Value::String(s)) if !s.is_empty() => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                _ => continue,
            };

            // Dedup: ZADD NX returns the number of newly-added members; 0 means
            // we've already seen this id this window.
            let added: i64 = redis::cmd("ZADD")
                .arg(&seen_key)
                .arg("NX")
                .arg(now)
                .arg(&tid)
                .query_async(&mut redis)
                .await
                .unwrap_or(0);
            if added == 0 {
                continue;
            }

            let author = t.get("author").cloned().unwrap_or(Value::Null);
            let username = author
                .get("userName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim_start_matches('@')
                .to_lowercase();
            // Curation gate: drop tweets from authors not on the roster.
            if !roster.is_empty() && !roster.contains(&username) {
                continue;
            }
            new_count += 1;

            let text = t.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let created = parse_created_at(
                t.get("createdAt").and_then(|v| v.as_str()).unwrap_or(""),
            );
            let created_iso = match &created {
                Some(dt) => {
                    max_seen_ts = max_seen_ts.max(dt.timestamp());
                    iso_millis(dt)
                }
                None => crate::realtime::hub::now_iso(),
            };
            let lag_ms = created
                .map(|dt| (Utc::now() - dt).num_milliseconds().max(0))
                .unwrap_or(0);
            let matched = extract_cashtags(&text);

            let user_name = author.get("userName").cloned().unwrap_or(Value::Null);
            let payload = json!({
                "handle": user_name,
                "display_name": author.get("name").cloned().unwrap_or(Value::Null),
                "text": text,
                "created_at": created_iso.clone(),
                "matched_symbols": matched,
                "likes": author_or_tweet(t, "likeCount"),
                "retweets": author_or_tweet(t, "retweetCount"),
                "replies": author_or_tweet(t, "replyCount"),
                "quotes": author_or_tweet(t, "quoteCount"),
                "views": author_or_tweet(t, "viewCount"),
                "url": author_or_tweet(t, "url"),
                "id": tid,
                "lang": author_or_tweet(t, "lang"),
                // Spec-shaped extras for the realtime stream:
                "author": user_name,
                "author_followers": author.get("followers").cloned().unwrap_or(Value::Null),
                "is_blue_verified": author.get("isBlueVerified").cloned().unwrap_or(Value::Null),
                "ts": created_iso,
                "source": "polling",
                "lag_ms": lag_ms,
            });
            let blob = payload.to_string();
            let _: Result<(), _> = redis.publish(format!("kol:{symbol}"), &blob).await;

            // Recall cache (Redis LISTs, capped + TTL'd) for the REST
            // `/kols/.../tweets` endpoints. Recall-only — Twitter terms
            // prohibit durable persistence; evicted on cap / TTL.
            let hkey = format!("kol_tweets:by_handle:{username}");
            let skey = format!("kol_tweets:by_symbol:{symbol}");
            let mut pipe = redis::pipe();
            pipe.lpush(&hkey, &blob).ignore();
            pipe.ltrim(&hkey, 0, TWEET_CACHE_CAP - 1).ignore();
            pipe.lpush(&skey, &blob).ignore();
            pipe.ltrim(&skey, 0, TWEET_CACHE_CAP - 1).ignore();
            pipe.expire(&hkey, RECALL_TTL_SEC).ignore();
            pipe.expire(&skey, RECALL_TTL_SEC).ignore();
            let _: Result<(), _> = pipe.query_async(&mut redis).await;
        }

        {
            let mut ls = self.last_seen.lock().await;
            let prev = ls.get(symbol).copied().unwrap_or(0);
            if max_seen_ts > prev {
                ls.insert(symbol.to_string(), max_seen_ts);
            }
        }
        // Bound the dedup ZSET to its newest 1000 entries.
        let _: Result<(), _> = redis.zremrangebyrank(&seen_key, 0, -1001).await;
        if new_count > 0 {
            tracing::info!("kol[{symbol}]: {new_count} new tweets published");
        }
    }

    async fn poll_loop(self: Arc<Self>, symbol: String) {
        let interval = std::time::Duration::from_secs(self.poll_sec.max(30));
        loop {
            self.fetch_once(&symbol).await;
            tokio::time::sleep(interval).await;
        }
    }
}

/// Read a field from the tweet object, returning JSON null when absent.
fn author_or_tweet(t: &Value, key: &str) -> Value {
    t.get(key).cloned().unwrap_or(Value::Null)
}

pub async fn start(
    hub: std::sync::Arc<crate::realtime::hub::Hub>,
    redis: redis::aio::MultiplexedConnection,
    settings: std::sync::Arc<crate::config::Settings>,
    pool: Pool,
) -> anyhow::Result<()> {
    let api_key = settings.twitterapi_key.clone();
    if api_key.is_empty() {
        tracing::warn!("FINDATA_TWITTERAPI_KEY empty; KOL upstream disabled");
        return Ok(());
    }

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let state = Arc::new(KolState {
        http,
        redis,
        hub: hub.clone(),
        pool,
        api_key,
        poll_sec: settings.rt_kol_poll_sec.max(30),
        max_per_poll: settings.rt_kol_max_per_poll.clamp(1, 100),
        roster: Mutex::new((HashSet::new(), 0)),
        last_seen: Arc::new(Mutex::new(HashMap::new())),
        tasks: Mutex::new(HashMap::new()),
    });

    // Prime the roster before accepting demand.
    state.refresh_roster(true).await;
    {
        let g = state.roster.lock().await;
        tracing::info!(
            "KOL upstream started (poll={}s, max_per_poll={}, roster={})",
            state.poll_sec,
            state.max_per_poll,
            g.0.len()
        );
    }

    // Demand listener: spawn/abort per-symbol poll tasks on 0<->1 transitions.
    let st = state.clone();
    let listener: crate::realtime::hub::DemandListener = Arc::new(move |symbol: String, active: bool| {
        let st = st.clone();
        Box::pin(async move {
            if active {
                if !twitter_eligible(&symbol) {
                    return;
                }
                let mut tasks = st.tasks.lock().await;
                if tasks.contains_key(&symbol) {
                    return;
                }
                // KOL never claims a hub tier slot — it's an additive overlay
                // alongside whatever tick tier serves the symbol (matches the
                // Python, which never touches hub tiers here).
                let sym2 = symbol.clone();
                let handle = tokio::spawn(st.clone().poll_loop(symbol.clone()));
                tasks.insert(sym2, handle);
                tracing::info!("KOL: started polling for {symbol}");
            } else {
                let mut tasks = st.tasks.lock().await;
                if let Some(h) = tasks.remove(&symbol) {
                    h.abort();
                    tracing::info!("KOL: stopped polling for {symbol}");
                }
            }
        })
    });
    hub.register_demand_listener(listener).await;

    Ok(())
}
