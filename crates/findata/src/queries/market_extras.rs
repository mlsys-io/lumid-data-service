//! Market extras — port of routes/market_extras.py inline SQL.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn market_movers(pool: &Pool, kind: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 100);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT snapshot_ts, symbol, name, price, change, changes_percentage \
               FROM market.market_movers WHERE kind = $1 \
              ORDER BY snapshot_ts DESC, changes_percentage DESC NULLS LAST LIMIT $2",
            &[&kind, &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn dividends_calendar(
    pool: &Pool,
    from: NaiveDate,
    to: NaiveDate,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 2000);
    let client = pool.get().await?;
    let rows = client
        .query(
            r#"SELECT symbol, date, record_date, payment_date, declaration_date,
                      adj_dividend, dividend, "yield", frequency
                 FROM events.dividends_calendar WHERE date >= $1 AND date <= $2
                ORDER BY date, symbol LIMIT $3"#,
            &[&from, &to, &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn splits_calendar(
    pool: &Pool,
    from: NaiveDate,
    to: NaiveDate,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 2000);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT symbol, date, numerator, denominator FROM events.splits_calendar \
              WHERE date >= $1 AND date <= $2 ORDER BY date, symbol LIMIT $3",
            &[&from, &to, &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

/// One of the four sector/industry snapshot tables. `table`/`label`/`metric`
/// come from a fixed internal set (never user input).
async fn snapshot(
    pool: &Pool,
    table: &str,
    label: &str,
    metric: &str,
    order: &str,
    exchange: Option<&str>,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut sql = format!(
        "SELECT snapshot_date, {label}, exchange, {metric} FROM market.{table} \
          WHERE snapshot_date = (SELECT max(snapshot_date) FROM market.{table})"
    );
    let client = pool.get().await?;
    let rows = if let Some(ex) = exchange {
        sql.push_str(&format!(" AND exchange = $1 ORDER BY {order}"));
        client.query(&sql, &[&ex.to_string()]).await?
    } else {
        sql.push_str(&format!(" ORDER BY {order}"));
        client.query(&sql, &[]).await?
    };
    Ok(rows_to_objects(&rows))
}

pub async fn sectors_pe(pool: &Pool, exchange: Option<&str>) -> ApiResult<Vec<Map<String, Value>>> {
    snapshot(pool, "sector_pe_snapshot", "sector", "pe", "sector", exchange).await
}
pub async fn sectors_perf(pool: &Pool, exchange: Option<&str>) -> ApiResult<Vec<Map<String, Value>>> {
    snapshot(pool, "sector_performance_snapshot", "sector", "average_change", "average_change DESC NULLS LAST", exchange).await
}
pub async fn industries_pe(pool: &Pool, exchange: Option<&str>) -> ApiResult<Vec<Map<String, Value>>> {
    snapshot(pool, "industry_pe_snapshot", "industry", "pe", "industry", exchange).await
}
pub async fn industries_perf(pool: &Pool, exchange: Option<&str>) -> ApiResult<Vec<Map<String, Value>>> {
    snapshot(pool, "industry_performance_snapshot", "industry", "average_change", "average_change DESC NULLS LAST", exchange).await
}

pub async fn exec_comp(pool: &Pool, industry: &str) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT industry_title, year, average_compensation \
               FROM reference.executive_compensation_benchmark \
              WHERE upper(industry_title) = upper($1) OR upper(industry_title) LIKE upper($1)||'%' \
              ORDER BY year DESC",
            &[&industry],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn universe_active(pool: &Pool, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 70000);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT DISTINCT ON (symbol) symbol, name, snapshot_date \
               FROM reference.actively_trading_list ORDER BY symbol, snapshot_date DESC LIMIT $1",
            &[&limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn index_constituents(
    pool: &Pool,
    index_symbol: &str,
    as_of: NaiveDate,
) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT index_symbol, constituent_symbol, name, sector, sub_sector, cik, \
                    added_date, removed_date FROM reference.index_constituents \
              WHERE upper(index_symbol) = upper($1) \
                AND (added_date IS NULL OR added_date <= $2) \
                AND (removed_date IS NULL OR removed_date > $2) \
              ORDER BY constituent_symbol",
            &[&index_symbol, &as_of],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
