//! Technical indicators — port of routes/technical.py inline SQL.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub const INDICATORS: [&str; 9] =
    ["sma", "ema", "dema", "tema", "wma", "rsi", "adx", "williams", "standarddeviation"];

pub async fn indicators(
    pool: &Pool,
    symbol: &str,
    indicator: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    qb.cmp("ts", ">=", start);
    qb.cmp("ts", "<=", end);
    if let Some(i) = indicator {
        qb.eq("indicator", i.to_string());
    }
    let lim = qb.push(limit.clamp(1, 5000));
    let sql = format!(
        "SELECT ts, indicator, period_length, timeframe, value, open, high, low, close, volume \
           FROM market.technical_indicators WHERE {} ORDER BY ts DESC, indicator LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn latest(pool: &Pool, symbol: &str) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT DISTINCT ON (indicator) ts, indicator, period_length, timeframe, value, \
                    open, high, low, close, volume FROM market.technical_indicators \
              WHERE symbol = $1 ORDER BY indicator, ts DESC",
            &[&symbol.to_uppercase()],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
