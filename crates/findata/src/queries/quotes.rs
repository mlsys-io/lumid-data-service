//! Quote snapshot (Redis last-tick), day-range/SMA stats, and metric snapshot.
//! Ports of api/queries/{quotes_snapshot,quote_stats,metrics_snapshot}.py.

use deadpool_postgres::Pool;
use redis::AsyncCommands;
use serde_json::{json, Map, Value};

use crate::db::rows::row_to_object;
use crate::error::ApiResult;
use crate::state::AppState;

/// Per-symbol last-tick from `last:tick:<sym>` hashes. Missing/undecodable →
/// `{symbol, ts:null, price:null, source:...}`.
pub async fn snapshot(st: &AppState, symbols: &[String]) -> Vec<Value> {
    let mut out = Vec::with_capacity(symbols.len());
    let Some(conn) = st.redis.clone() else {
        return symbols
            .iter()
            .map(|s| json!({"symbol": s, "ts": null, "price": null, "source": "no_cache"}))
            .collect();
    };
    let mut conn = conn;
    for sym in symbols {
        let raw: Option<String> = conn.hget(format!("last:tick:{sym}"), "payload").await.ok().flatten();
        match raw {
            None => out.push(json!({"symbol": sym, "ts": null, "price": null, "source": "no_cache"})),
            Some(s) => match serde_json::from_str::<Value>(&s) {
                Ok(v) => out.push(v),
                Err(_) => out.push(json!({"symbol": sym, "ts": null, "price": null, "source": "decode_error"})),
            },
        }
    }
    out
}

const STOCK_STATS_SQL: &str = r#"
    WITH ranked AS (
      SELECT date, close, high, low, ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
        FROM market.ohlc_daily_adjusted
       WHERE symbol = $1 AND adjustment = 'dividend'
       ORDER BY date DESC LIMIT 200
    )
    SELECT
      (CASE WHEN count(*) FILTER (WHERE rn <= 50)  >= 50  THEN AVG(close) FILTER (WHERE rn <= 50)  END)::float8 AS sma_50,
      (CASE WHEN count(*) FILTER (WHERE rn <= 200) >= 200 THEN AVG(close) FILTER (WHERE rn <= 200) END)::float8 AS sma_200,
      max(high) FILTER (WHERE rn = 1)::float8 AS day_high,
      min(low)  FILTER (WHERE rn = 1)::float8 AS day_low,
      max(date) AS as_of, count(*)::int AS sample_size
      FROM ranked
"#;

const NONSTOCK_STATS_SQL: &str = r#"
    WITH daily AS (
      SELECT (date_trunc('day', ts))::date AS d, (array_agg(close ORDER BY ts DESC))[1] AS close,
             max(high) AS high, min(low) AS low
        FROM market.ohlc_1min WHERE symbol = $1 AND ts >= (now() - interval '210 days') GROUP BY 1
    ), ranked AS (
      SELECT d, close, high, low, ROW_NUMBER() OVER (ORDER BY d DESC) AS rn FROM daily ORDER BY d DESC LIMIT 200
    )
    SELECT
      (CASE WHEN count(*) FILTER (WHERE rn <= 50)  >= 50  THEN AVG(close) FILTER (WHERE rn <= 50)  END)::float8 AS sma_50,
      (CASE WHEN count(*) FILTER (WHERE rn <= 200) >= 200 THEN AVG(close) FILTER (WHERE rn <= 200) END)::float8 AS sma_200,
      max(high) FILTER (WHERE rn = 1)::float8 AS day_high,
      min(low)  FILTER (WHERE rn = 1)::float8 AS day_low,
      max(d) AS as_of, count(*)::int AS sample_size
      FROM ranked
"#;

pub async fn stats(pool: &Pool, symbol: &str) -> ApiResult<Value> {
    let sym = symbol.to_uppercase();
    let client = pool.get().await?;
    let mut row = client.query_opt(STOCK_STATS_SQL, &[&sym]).await?;
    let stock_empty = row
        .as_ref()
        .map(|r| r.get::<_, i32>("sample_size") == 0)
        .unwrap_or(true);
    if stock_empty {
        row = client.query_opt(NONSTOCK_STATS_SQL, &[&sym]).await?;
    }
    let d = row.map(|r| row_to_object(&r)).unwrap_or_default();
    Ok(json!({
        "symbol": sym,
        "as_of": d.get("as_of").cloned().unwrap_or(Value::Null),
        "day_high": d.get("day_high").cloned().unwrap_or(Value::Null),
        "day_low": d.get("day_low").cloned().unwrap_or(Value::Null),
        "sma_50": d.get("sma_50").cloned().unwrap_or(Value::Null),
        "sma_200": d.get("sma_200").cloned().unwrap_or(Value::Null),
        "sample_size": d.get("sample_size").cloned().unwrap_or(json!(0)),
    }))
}

/// Wide metric snapshot: metric_name → value (float if parsable, else string).
pub async fn metrics_snapshot(pool: &Pool, symbol: &str) -> ApiResult<Map<String, Value>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT metric_name, metric_value FROM fundamentals.finnhub_metric \
              WHERE symbol = $1 ORDER BY metric_name",
            &[&symbol.to_uppercase()],
        )
        .await?;
    let mut out = Map::new();
    for r in &rows {
        let name: String = r.get("metric_name");
        let raw: Option<String> = r.get("metric_value");
        let v = match raw {
            None => Value::Null,
            Some(s) => match s.parse::<f64>() {
                Ok(f) => serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::String(s)),
                Err(_) => Value::String(s),
            },
        };
        out.insert(name, v);
    }
    Ok(out)
}
