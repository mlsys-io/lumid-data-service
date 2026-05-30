//! News queries — port of `api/queries/news.py` (per-symbol feed).

use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

const FOR_SYMBOL_SQL: &str = r#"
    WITH base AS (
      SELECT published_at, publisher, headline, summary, url, category
        FROM news.articles
       WHERE symbol = $1 AND published_at >= $2
      UNION ALL
      SELECT published_at, publisher, headline, summary, url, category
        FROM news.articles_by_symbol
       WHERE symbol = $1 AND published_at >= $2
    ),
    per_url AS (
      SELECT DISTINCT ON (url)
             published_at, publisher, headline, summary, url, category
        FROM base
       ORDER BY url, published_at DESC
    )
    SELECT published_at, publisher, headline, summary, url, category
      FROM per_url
     ORDER BY published_at DESC
     LIMIT $3
"#;

pub async fn for_symbol(
    pool: &Pool,
    symbol: &str,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    // Default 30-day window (matches the hypertable chunk interval for pruning).
    let since = since.unwrap_or_else(|| Utc::now() - Duration::days(30));
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(FOR_SYMBOL_SQL, &[&symbol.to_uppercase(), &since, &limit])
        .await?;
    Ok(rows_to_objects(&rows))
}

// ----- global feeds: latest / search / stats -----
fn category_alias(c: &str) -> String {
    match c.to_lowercase().as_str() {
        "general" => "general_market",
        "stock" => "stock_market",
        "press" | "press_release" => "press_release",
        "crypto" => "crypto",
        "forex" => "forex",
        "company" => "company",
        other => return other.to_string(),
    }
    .to_string()
}

pub async fn latest(
    pool: &Pool,
    category: Option<&str>,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let since = since.unwrap_or_else(|| Utc::now() - Duration::days(30));
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![Box::new(since)];
    let mut where_ = "published_at >= $1".to_string();
    if let Some(c) = category {
        params.push(Box::new(category_alias(c)));
        where_.push_str(&format!(" AND category = ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let sql = format!(
        "SELECT published_at, publisher, headline, summary, url, category, symbol \
           FROM news.articles WHERE {where_} ORDER BY published_at DESC LIMIT ${lim}"
    );
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn search(
    pool: &Pool,
    q: &str,
    category: Option<&str>,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let since = since.unwrap_or_else(|| Utc::now() - Duration::days(30));
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(q.to_string()), Box::new(since)];
    let mut where_ =
        "search_tsv @@ websearch_to_tsquery('english', $1) AND published_at >= $2".to_string();
    if let Some(c) = category {
        params.push(Box::new(category_alias(c)));
        where_.push_str(&format!(" AND category = ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let sql = format!(
        "SELECT published_at, publisher, headline, summary, url, category, symbol \
           FROM news.articles WHERE {where_} ORDER BY published_at DESC LIMIT ${lim}"
    );
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn stats(pool: &Pool) -> ApiResult<serde_json::Value> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT category, \
                    count(*) FILTER (WHERE published_at > now() - interval '7 days')  AS rows_last_7d, \
                    count(*) FILTER (WHERE published_at > now() - interval '30 days') AS rows_last_30d, \
                    max(published_at) AS latest_in_60d \
               FROM news.articles WHERE published_at > now() - interval '60 days' \
              GROUP BY category ORDER BY rows_last_30d DESC NULLS LAST",
            &[],
        )
        .await?;
    Ok(serde_json::json!({"categories": rows_to_objects(&rows)}))
}

// ----- news_meta: social + symbol sentiment -----
pub async fn social_sentiment(
    pool: &Pool,
    symbol: &str,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(symbol.to_uppercase())];
    let mut where_ = vec!["symbol = $1".to_string()];
    if let Some(ts) = since {
        params.push(Box::new(ts));
        where_.push(format!("ts >= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 1000)));
    let lim = params.len();
    let sql = format!(
        "SELECT ts, mention::float8 AS mention, positive_score::float8 AS positive_score, \
                negative_score::float8 AS negative_score, positive_mention::float8 AS positive_mention, \
                negative_mention::float8 AS negative_mention FROM news.social_sentiment \
          WHERE {} ORDER BY ts DESC LIMIT ${lim}",
        where_.join(" AND ")
    );
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn symbol_sentiment(
    pool: &Pool,
    symbol: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT period_end_date, buzz::float8 AS buzz, weekly_avg::float8 AS weekly_avg, \
                    articles_last_week, sentiment_score::float8 AS sentiment_score, \
                    bearish_pct::float8 AS bearish_pct, bullish_pct::float8 AS bullish_pct \
               FROM news.symbol_sentiment WHERE symbol = $1 ORDER BY period_end_date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
