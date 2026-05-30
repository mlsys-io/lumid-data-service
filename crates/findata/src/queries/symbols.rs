//! Symbol queries — port of `api/queries/symbols.py`.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

// Verbatim from queries/symbols.py:search — unified searchable catalog with a
// LATERAL join to the latest profile row for sector/industry.
const SEARCH_SQL: &str = r#"
    WITH hits AS (
      SELECT symbol, name, asset_class
        FROM reference._searchable_symbols
       WHERE symbol ILIKE $1 OR name ILIKE $2
       ORDER BY (symbol = upper($3)) DESC,
                (symbol ILIKE $4) DESC,
                length(symbol),
                symbol
       LIMIT $5
    )
    SELECT h.symbol, h.name,
           p.sector,
           COALESCE(p.industry, h.asset_class) AS industry
      FROM hits h
      LEFT JOIN LATERAL (
        SELECT sector, industry FROM reference.profile
         WHERE symbol = h.symbol AND source='fmp'
         ORDER BY ingest_ts DESC LIMIT 1
      ) p ON true
     ORDER BY (h.symbol = upper($3)) DESC,
              (h.symbol ILIKE $4) DESC,
              length(h.symbol),
              h.symbol
"#;

pub async fn search(pool: &Pool, q: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 100);
    let prefix = format!("{q}%");
    let sub = format!("%{q}%");
    let client = pool.get().await?;
    let rows = client
        .query(SEARCH_SQL, &[&prefix, &sub, &q, &prefix, &limit])
        .await?;
    Ok(rows_to_objects(&rows))
}
