//! Corporate actions — port of api/queries/corp_actions.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn dividends(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(d) = since {
        qb.cmp("date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT date, record_date, payment_date, declaration_date, amount::float8 AS amount, \
                adj_amount::float8 AS adj_amount, yield_pct::float8 AS yield_pct, frequency \
           FROM market.dividends WHERE {} ORDER BY date DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn splits(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT date, numerator::float8 AS numerator, denominator::float8 AS denominator, \
                    ratio::float8 AS ratio FROM market.splits WHERE symbol = $1 \
              ORDER BY date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn market_cap_history(
    pool: &Pool,
    symbol: &str,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(d) = start {
        qb.cmp("date", ">=", d);
    }
    if let Some(d) = end {
        qb.cmp("date", "<=", d);
    }
    let lim = qb.push(limit.clamp(1, 5000));
    let sql = format!(
        "SELECT date, market_cap::float8 AS market_cap FROM market.market_cap \
          WHERE {} ORDER BY date DESC LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    let mut out = rows_to_objects(&rows);
    // caller-friendly ascending order (matches the Python post-sort)
    out.sort_by(|a, b| {
        a.get("date").and_then(|v| v.as_str()).cmp(&b.get("date").and_then(|v| v.as_str()))
    });
    Ok(out)
}
