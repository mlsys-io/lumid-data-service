//! OHLC queries — port of `api/queries/ohlc.py`.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use findata::db::rows::rows_to_objects;
use findata::error::{ApiError, ApiResult};

pub const ALLOWED_INTERVALS: [&str; 7] =
    ["1min", "5min", "15min", "30min", "1hour", "4hour", "1d"];

fn intraday_table(interval: &str) -> Option<&'static str> {
    match interval {
        "1min" => Some("market.ohlc_1min"),
        "5min" => Some("market.ohlc_5min"),
        "15min" => Some("market.ohlc_15min"),
        "30min" => Some("market.ohlc_30min"),
        "1hour" => Some("market.ohlc_1hour"),
        "4hour" => Some("market.ohlc_4hour"),
        _ => None,
    }
}

fn bars_per_day(interval: &str) -> i64 {
    match interval {
        "1min" => 390,
        "5min" => 78,
        "15min" => 26,
        "30min" => 13,
        "1hour" => 7,
        "4hour" => 2,
        _ => 390,
    }
}

fn estimate_bars(start: DateTime<Utc>, end: DateTime<Utc>, interval: &str) -> i64 {
    let span_days = ((end - start).num_days() + 1).max(1);
    if interval == "1d" {
        span_days
    } else {
        span_days * bars_per_day(interval)
    }
}

const SQL_STOCK_DAILY: &str = r#"
    SELECT date::timestamptz AS ts, open, high, low, close, volume
      FROM market.ohlc_daily_adjusted
     WHERE symbol = $1 AND adjustment = 'dividend'
       AND date >= $2::date AND date <= $3::date
     ORDER BY date
"#;

const SQL_ROLLUP_DAILY: &str = r#"
    SELECT (date_trunc('day', ts))::date::timestamptz AS ts,
           (array_agg(open  ORDER BY ts))[1]      AS open,
           max(high)                              AS high,
           min(low)                               AS low,
           (array_agg(close ORDER BY ts DESC))[1] AS close,
           sum(volume)::float8                    AS volume
      FROM market.ohlc_1min
     WHERE symbol = $1
       AND ts >= $2::date AND ts < ($3::date + interval '1 day')
     GROUP BY 1
     ORDER BY 1
"#;

pub async fn query(
    pool: &Pool,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    interval: &str,
    row_cap: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    if !ALLOWED_INTERVALS.contains(&interval) {
        return Err(ApiError::BadRequest(format!(
            "interval must be one of {ALLOWED_INTERVALS:?} (got {interval:?})"
        )));
    }
    let est = estimate_bars(start, end, interval);
    if est > row_cap {
        return Err(ApiError::BadRequest(format!(
            "would return ~{est} rows; cap is {row_cap}. Narrow the range or use a coarser interval."
        )));
    }
    let sym = symbol.to_uppercase();
    let client = pool.get().await?;

    if interval == "1d" {
        let start_d = start.date_naive();
        let end_d = end.date_naive();
        let rows = client.query(SQL_STOCK_DAILY, &[&sym, &start_d, &end_d]).await?;
        if !rows.is_empty() {
            return Ok(rows_to_objects(&rows));
        }
        let rows = client.query(SQL_ROLLUP_DAILY, &[&sym, &start_d, &end_d]).await?;
        return Ok(rows_to_objects(&rows));
    }

    let table = intraday_table(interval).expect("validated above");
    let sql = format!(
        "SELECT ts, open, high, low, close, volume FROM {table} \
         WHERE symbol = $1 AND ts >= $2 AND ts <= $3 ORDER BY ts"
    );
    let rows = client.query(&sql, &[&sym, &start, &end]).await?;
    Ok(rows_to_objects(&rows))
}
