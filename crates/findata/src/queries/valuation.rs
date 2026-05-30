//! Valuation — DCF, enterprise values, financial scores, owner earnings.
//! Port of api/queries/valuation.py.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::{ApiError, ApiResult};

fn period_filter(period: &str) -> ApiResult<&'static str> {
    match period {
        "quarter" => Ok(" AND period_type IN ('Q1','Q2','Q3','Q4')"),
        "fy" => Ok(" AND period_type = 'FY'"),
        "all" => Ok(""),
        _ => Err(ApiError::BadRequest("period must be quarter, fy, or all".into())),
    }
}

pub async fn dcf(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT as_of, variant, dcf_value::float8 AS dcf_value, \
                    stock_price::float8 AS stock_price FROM fundamentals.dcf \
              WHERE symbol = $1 ORDER BY as_of DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn enterprise_values(
    pool: &Pool,
    symbol: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pf = period_filter(period)?;
    let limit = limit.clamp(1, 200);
    let sql = format!(
        "SELECT period_end_date, period_type, enterprise_value::float8 AS enterprise_value, \
                market_cap::float8 AS market_cap, total_debt::float8 AS total_debt, \
                cash_and_short_term::float8 AS cash_and_short_term \
           FROM fundamentals.enterprise_values \
          WHERE symbol = $1 AND source = 'fmp'{pf} ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn financial_scores(
    pool: &Pool,
    symbol: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT as_of, currency, altman_z::float8 AS altman_z, piotroski::float8 AS piotroski, \
                    working_capital::float8 AS working_capital, total_assets::float8 AS total_assets, \
                    retained_earnings::float8 AS retained_earnings, ebit::float8 AS ebit, \
                    market_cap::float8 AS market_cap, total_liabilities::float8 AS total_liabilities, \
                    revenue::float8 AS revenue FROM fundamentals.financial_scores \
              WHERE symbol = $1 ORDER BY as_of DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn owner_earnings(
    pool: &Pool,
    symbol: &str,
    period: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let pf = period_filter(period)?;
    let limit = limit.clamp(1, 200);
    let sql = format!(
        "SELECT period_end_date, period_type, owner_earnings::float8 AS owner_earnings \
           FROM fundamentals.owner_earnings WHERE symbol = $1{pf} \
          ORDER BY period_end_date DESC LIMIT $2"
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &[&symbol.to_uppercase(), &limit]).await?;
    Ok(rows_to_objects(&rows))
}
