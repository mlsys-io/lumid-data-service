//! KOL roster + Redis tweet recall + durable archive.
//! Ports of api/queries/kols.py and api/queries/kol_tweets.py.

use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use redis::AsyncCommands;
use serde_json::{Map, Value};
use tokio_postgres::types::ToSql;

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;
use crate::state::AppState;

type P = Vec<Box<dyn ToSql + Sync + Send>>;
fn refs(p: &P) -> Vec<&(dyn ToSql + Sync)> {
    p.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

// ----- roster -----
pub async fn list_active(pool: &Pool, include_inactive: bool) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT handle, display_name, twitter_id, follower_tier, notes, active, added_at, \
                    added_by, updated_at FROM news.kol_roster WHERE ($1::boolean OR active) \
              ORDER BY follower_tier NULLS LAST, handle",
            &[&include_inactive],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

// ----- Redis-backed live recall -----
async fn lrange_json(st: &AppState, key: &str, limit: i64) -> Vec<Value> {
    let Some(conn) = st.redis.clone() else { return vec![] };
    let mut conn = conn;
    let stop = (limit - 1).max(0) as isize;
    let raw: Vec<String> = conn.lrange(key, 0isize, stop).await.unwrap_or_default();
    raw.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
}

pub async fn tweets_by_handle(st: &AppState, handle: &str, limit: i64) -> Vec<Value> {
    lrange_json(st, &format!("kol_tweets:by_handle:{}", handle.to_lowercase()), limit).await
}

pub async fn tweets_by_symbol(st: &AppState, symbol: &str, limit: i64) -> Vec<Value> {
    lrange_json(st, &format!("kol_tweets:by_symbol:{}", symbol.to_uppercase()), limit).await
}

pub async fn tweets_recent(st: &AppState, handles: &[String], limit: i64) -> Vec<Value> {
    if handles.is_empty() {
        return vec![];
    }
    let mut bucket: Vec<Value> = Vec::new();
    for h in handles {
        bucket.extend(tweets_by_handle(st, h, limit).await);
    }
    bucket.sort_by(|a, b| {
        let ka = a.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let kb = b.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        kb.cmp(ka) // reverse (newest first)
    });
    bucket.truncate(limit.max(0) as usize);
    bucket
}

// ----- durable archive (news.kol_tweets) -----
fn default_since(since: Option<DateTime<Utc>>) -> DateTime<Utc> {
    since.unwrap_or_else(|| Utc::now() - Duration::days(30))
}

pub async fn history_by_handle(
    pool: &Pool,
    handle: &str,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    cashtag: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: P = vec![
        Box::new(handle.trim_start_matches('@').to_lowercase()),
        Box::new(default_since(since)),
    ];
    let mut where_ = "kol_username = $1 AND created_at >= $2".to_string();
    if let Some(u) = until {
        params.push(Box::new(u));
        where_.push_str(&format!(" AND created_at <= ${}", params.len()));
    }
    if let Some(c) = cashtag {
        params.push(Box::new(vec![c.to_uppercase().trim_start_matches('$').to_string()]));
        where_.push_str(&format!(" AND cashtags @> ${}::text[]", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 500)));
    let lim = params.len();
    let sql = format!(
        "SELECT tweet_id, created_at, kol_username, author_username, author_name, author_followers, \
                author_verified, tweet_type, lang, text, url, cashtags, hashtags, mentioned_users, \
                media_urls, retweet_count, reply_count, like_count, quote_count, bookmark_count, view_count \
           FROM news.kol_tweets WHERE {where_} ORDER BY created_at DESC LIMIT ${lim}"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn history_by_symbol(
    pool: &Pool,
    symbol: &str,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    handle: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: P = vec![
        Box::new(vec![symbol.to_uppercase().trim_start_matches('$').to_string()]),
        Box::new(default_since(since)),
    ];
    let mut where_ = "cashtags @> $1::text[] AND created_at >= $2".to_string();
    if let Some(u) = until {
        params.push(Box::new(u));
        where_.push_str(&format!(" AND created_at <= ${}", params.len()));
    }
    if let Some(h) = handle {
        params.push(Box::new(h.trim_start_matches('@').to_lowercase()));
        where_.push_str(&format!(" AND kol_username = ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 500)));
    let lim = params.len();
    let sql = format!(
        "SELECT tweet_id, created_at, kol_username, author_username, author_name, author_followers, \
                author_verified, tweet_type, lang, text, url, cashtags, hashtags, media_urls, \
                retweet_count, like_count, quote_count, view_count \
           FROM news.kol_tweets WHERE {where_} ORDER BY created_at DESC LIMIT ${lim}"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn search(
    pool: &Pool,
    q: &str,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: P = vec![Box::new(q.to_string()), Box::new(default_since(since))];
    let mut where_ = "to_tsvector('english', coalesce(text,'')) @@ websearch_to_tsquery('english', $1) \
                      AND created_at >= $2"
        .to_string();
    if let Some(u) = until {
        params.push(Box::new(u));
        where_.push_str(&format!(" AND created_at <= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let sql = format!(
        "SELECT tweet_id, created_at, kol_username, author_username, author_name, author_verified, \
                tweet_type, lang, text, url, cashtags, hashtags, media_urls, retweet_count, \
                like_count, view_count FROM news.kol_tweets WHERE {where_} \
          ORDER BY created_at DESC LIMIT ${lim}"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn archive_stats(pool: &Pool) -> ApiResult<Map<String, Value>> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "SELECT (SELECT count(*) FROM news.kol_tweets) AS total_rows, \
                    (SELECT count(*) FROM (SELECT DISTINCT kol_username FROM news.kol_tweets) k) AS distinct_kols, \
                    (SELECT created_at FROM news.kol_tweets ORDER BY created_at ASC LIMIT 1) AS earliest, \
                    (SELECT created_at FROM news.kol_tweets ORDER BY created_at DESC LIMIT 1) AS latest",
            &[],
        )
        .await?;
    let mut out = row_to_object_pub(&row);
    out.insert("last_ingest".to_string(), Value::Null);
    Ok(out)
}

fn row_to_object_pub(row: &tokio_postgres::Row) -> Map<String, Value> {
    crate::db::rows::row_to_object(row)
}

/// Add `media_proxy_urls` derived from `media_urls` (matches _attach_proxy_urls).
pub fn attach_proxy_urls(mut rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    for r in rows.iter_mut() {
        if let Some(Value::Array(urls)) = r.get("media_urls") {
            if !urls.is_empty() {
                let proxied: Vec<Value> = urls
                    .iter()
                    .filter_map(|u| u.as_str())
                    .map(|u| Value::String(format!("/kols/media/by-url?u={}", pct_encode(u))))
                    .collect();
                r.insert("media_proxy_urls".to_string(), Value::Array(proxied));
            }
        }
    }
    rows
}

// Response-model field sets (Python pydantic filters extras + pads missing
// with defaults). We replicate that projection for byte-parity.
const KOLTWEET_FIELDS: [&str; 8] =
    ["handle", "display_name", "text", "created_at", "matched_symbols", "likes", "retweets", "url"];
const ARCHIVE_FIELDS: [&str; 22] = [
    "tweet_id", "created_at", "kol_username", "author_username", "author_name",
    "author_followers", "author_verified", "tweet_type", "lang", "text", "url",
    "cashtags", "hashtags", "mentioned_users", "media_urls", "media_proxy_urls",
    "retweet_count", "reply_count", "like_count", "quote_count", "bookmark_count", "view_count",
];

/// Project a row to `fields` in order: take existing value or a default
/// (empty array for names in `empty_list`, else null). Drops extra keys.
fn project(mut obj: Map<String, Value>, fields: &[&str], empty_list: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    for f in fields {
        let v = obj.remove(*f).unwrap_or_else(|| {
            if empty_list.contains(f) { Value::Array(vec![]) } else { Value::Null }
        });
        out.insert((*f).to_string(), v);
    }
    out
}

/// Re-canonicalize a datetime string the way pydantic's `datetime` field does:
/// parse then re-serialize with `Z` + (no-fraction | 6-digit micros). Non-parseable
/// values pass through unchanged.
fn canon_dt(v: Value) -> Value {
    if let Value::String(s) = &v {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            let dt = dt.with_timezone(&Utc);
            let fmt = if dt.timestamp_subsec_nanos() == 0 {
                chrono::SecondsFormat::Secs
            } else {
                chrono::SecondsFormat::Micros
            };
            return Value::String(dt.to_rfc3339_opts(fmt, true));
        }
    }
    v
}

/// Project live-recall tweets to the KOLTweet response shape.
pub fn project_recall(tweets: Vec<Value>) -> Vec<Value> {
    tweets
        .into_iter()
        .map(|t| {
            let mut obj = t.as_object().cloned().unwrap_or_default();
            if let Some(c) = obj.remove("created_at") {
                obj.insert("created_at".to_string(), canon_dt(c));
            }
            // project reads fields in KOLTWEET_FIELDS order, so output order is stable.
            Value::Object(project(obj, &KOLTWEET_FIELDS, &["matched_symbols"]))
        })
        .collect()
}

/// Project archive rows to the KOLTweetArchiveRow shape (pad missing → null).
pub fn project_archive(rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    rows.into_iter().map(|r| project(r, &ARCHIVE_FIELDS, &[])).collect()
}

/// Percent-encode like Python urllib.parse.quote(u, safe="") — keep only
/// unreserved chars A-Za-z0-9 _ . - ~.
fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}
