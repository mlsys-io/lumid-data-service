//! Analysis domain — ratios, key metrics, growth. Ports of
//! api/queries/{ratios,key_metrics,financial_growth}.py.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::{ApiError, ApiResult};

const META_KEYS: [&str; 7] = [
    "symbol", "date", "period", "calendarYear", "reportedCurrency", "fiscalYear", "cik",
];

fn period_filter(period: &str) -> ApiResult<&'static str> {
    match period {
        "quarter" => Ok(" AND period_type IN ('Q1','Q2','Q3','Q4')"),
        "fy" => Ok(" AND period_type = 'FY'"),
        "all" => Ok(""),
        _ => Err(ApiError::BadRequest("period must be quarter, fy, or all".into())),
    }
}

/// Coerce a leaf to float-or-null, matching the Python response models
/// (`dict[str, Optional[float]]`) for ratios/growth — numbers and numeric
/// strings both become JSON numbers; nulls stay null; anything else passes.
fn coerce_float(v: Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Number(n) => n
            .as_f64()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Value::String(s) => match s.parse::<f64>() {
            Ok(f) => serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::String(s)),
            Err(_) => Value::String(s),
        },
        other => other,
    }
}

/// Move the `raw` jsonb into `out_key`, dropping the upstream meta keys and
/// coercing each remaining value to float (per the Python response model).
fn pivot_raw(mut obj: Map<String, Value>, out_key: &str) -> Map<String, Value> {
    let raw = obj.remove("raw");
    let filtered = match raw {
        Some(Value::Object(m)) => Value::Object(
            m.into_iter()
                .filter(|(k, _)| !META_KEYS.contains(&k.as_str()))
                .map(|(k, v)| (k, coerce_float(v)))
                .collect(),
        ),
        _ => Value::Object(Map::new()),
    };
    obj.insert(out_key.to_string(), filtered);
    obj
}

pub async fn ratios(
    pool: &Pool,
    symbol: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pf = period_filter(period)?;
    let limit = limit.clamp(1, 200);
    let sql = format!(
        "SELECT period_end_date, period_type, raw FROM fundamentals.ratios \
         WHERE symbol = $1 AND source = 'fmp'{pf} ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows).into_iter().map(|o| pivot_raw(o, "ratios")).collect())
}

pub async fn key_metrics(
    pool: &Pool,
    symbol: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pf = period_filter(period)?;
    let limit = limit.clamp(1, 200);
    let sql = format!(
        "SELECT period_end_date, period_type, pe::float8, pb::float8, ps::float8, \
                ev_ebitda::float8, ev_revenue::float8, debt_to_equity::float8, \
                current_ratio::float8, quick_ratio::float8, roe::float8, roa::float8, \
                fcf_yield::float8 \
           FROM fundamentals.key_metrics \
          WHERE symbol = $1 AND source = 'fmp'{pf} ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows))
}

/// Shared growth-table reader; pivots `raw` into a `growth` map.
pub async fn growth(
    pool: &Pool,
    table: &str,
    symbol: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pf = period_filter(period)?;
    let limit = limit.clamp(1, 200);
    // `table` comes only from a fixed internal set (see handlers) — never user input.
    let sql = format!(
        "SELECT period_end_date, period_type, raw FROM {table} \
         WHERE symbol = $1 AND source = 'fmp'{pf} ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows).into_iter().map(|o| pivot_raw(o, "growth")).collect())
}
