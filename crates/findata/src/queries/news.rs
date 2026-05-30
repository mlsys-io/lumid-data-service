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
