//! Earnings history + quality — port of api/queries/earnings_history.py.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn history(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT fiscal_date, report_date, time_of_day, actual_eps::float8 AS actual_eps, \
                    estimated_eps::float8 AS estimated_eps, surprise::float8 AS surprise, \
                    surprise_pct::float8 AS surprise_pct, actual_revenue::float8 AS actual_revenue, \
                    estimated_revenue::float8 AS estimated_revenue FROM fundamentals.earnings \
              WHERE symbol = $1 ORDER BY fiscal_date DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn quality(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT period_end_date, period_type, letter_score, score::float8 AS score, \
                    growth::float8 AS growth, leverage::float8 AS leverage, \
                    profitability::float8 AS profitability, cash_generation::float8 AS cash_generation \
               FROM fundamentals.earnings_quality_score WHERE symbol = $1 \
              ORDER BY period_end_date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
