//! Symbol queries — port of `api/queries/symbols.py`.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use findata::db::rows::{row_to_object, rows_to_objects};
use findata::error::ApiResult;

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

const GET_EQUITY_SQL: &str = r#"
    SELECT a.symbol, a.name,
           COALESCE(a.exchange, p.raw_exchange) AS exchange,
           a.country, a.ipo_date,
           p.sector, p.industry,
           p.market_cap::float8 AS market_cap,
           COALESCE(a.is_etf, false)  AS is_etf,
           COALESCE(a.is_fund, false) AS is_fund,
           'equity' AS asset_class
      FROM reference.active_symbols a
      LEFT JOIN LATERAL (
        SELECT sector, industry, market_cap, raw->>'exchange' AS raw_exchange
          FROM reference.profile
         WHERE symbol = a.symbol AND source='fmp'
         ORDER BY ingest_ts DESC LIMIT 1
      ) p ON true
     WHERE a.symbol = $1
"#;

const GET_ETF_SQL: &str = r#"
    SELECT e.symbol, e.name,
           NULL::text AS exchange,
           e.domicile AS country,
           e.inception_date::date AS ipo_date,
           NULL::text AS sector,
           e.asset_class AS industry,
           e.assets_under_management::float8 AS market_cap,
           true AS is_etf, false AS is_fund,
           'etf' AS asset_class
      FROM reference.etf_info e
     WHERE e.symbol = $1
"#;

const GET_OTHER_SQL: &str = r#"
    SELECT s.symbol, s.name,
           NULL::text AS exchange, NULL::text AS country, NULL::date AS ipo_date,
           NULL::text AS sector,
           s.asset_class AS industry,
           NULL::float8 AS market_cap,
           s.is_etf, s.is_fund, s.asset_class
      FROM reference._searchable_symbols s
     WHERE s.symbol = $1
"#;

fn asset_class_exchange(ac: &str) -> Option<&'static str> {
    match ac {
        "cryptocurrency" => Some("CRYPTO"),
        "forex" => Some("FX"),
        "index" => Some("INDEX"),
        "commodity" => Some("COMMODITY"),
        _ => None,
    }
}

pub async fn get_one(pool: &Pool, symbol: &str) -> ApiResult<Option<Map<String, Value>>> {
    let sym = symbol.to_uppercase();
    let client = pool.get().await?;
    // 1) equities + tagged ETFs/funds
    if let Some(row) = client.query_opt(GET_EQUITY_SQL, &[&sym]).await? {
        let mut obj = row_to_object(&row);
        obj.remove("asset_class"); // internal-only; not in the Symbol response shape
        return Ok(Some(obj));
    }
    // 2) ETF universe
    if let Some(row) = client.query_opt(GET_ETF_SQL, &[&sym]).await? {
        let mut obj = row_to_object(&row);
        obj.remove("asset_class");
        return Ok(Some(obj));
    }
    // 3) crypto/forex/index/commodity — synthesize exchange from asset_class
    if let Some(row) = client.query_opt(GET_OTHER_SQL, &[&sym]).await? {
        let mut obj = row_to_object(&row);
        let ac = obj.get("asset_class").and_then(|v| v.as_str()).unwrap_or("").to_string();
        obj.insert(
            "exchange".to_string(),
            asset_class_exchange(&ac).map(|s| Value::String(s.to_string())).unwrap_or(Value::Null),
        );
        obj.remove("asset_class");
        return Ok(Some(obj));
    }
    Ok(None)
}

pub async fn universe(pool: &Pool) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query("SELECT symbol FROM reference._stock_universe ORDER BY symbol", &[])
        .await?;
    Ok(rows_to_objects(&rows))
}
