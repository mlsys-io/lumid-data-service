//! Earnings calendar — port of api/queries/earnings.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};
use tokio_postgres::types::ToSql;

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn calendar(
    pool: &Pool,
    symbol: Option<&str>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut where_: Vec<String> = Vec::new();
    if let Some(s) = symbol {
        params.push(Box::new(s.to_uppercase()));
        where_.push(format!("symbol = ${}", params.len()));
    }
    if let Some(d) = start {
        params.push(Box::new(d));
        where_.push(format!("report_date >= ${}", params.len()));
    }
    if let Some(d) = end {
        params.push(Box::new(d));
        where_.push(format!("report_date <= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 500)));
    let limit_idx = params.len();
    let where_clause = if where_.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_.join(" AND "))
    };
    let sql = format!(
        "SELECT symbol, report_date, fiscal_date, time_of_day, eps_estimated, eps_actual, \
                revenue_estimated, revenue_actual \
           FROM events.earnings_calendar {where_clause} \
          ORDER BY report_date DESC, symbol LIMIT ${limit_idx}"
    );
    let refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs).await?;
    Ok(rows_to_objects(&rows))
}
