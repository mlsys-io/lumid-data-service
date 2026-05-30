//! XBRL as-reported filings — port of routes/xbrl.py inline SQL.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use findata::db::rows::{row_to_object, rows_to_objects};
use findata::error::ApiResult;

pub async fn index(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 500);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT access_number, cik, year, quarter, form, start_date, end_date, \
                    filed_date, accepted_date FROM raw.finnhub_financials_reported \
              WHERE symbol = $1 ORDER BY filed_date DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn filing(
    pool: &Pool,
    symbol: &str,
    accession: &str,
) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT access_number, cik, year, quarter, form, start_date, end_date, \
                    filed_date, accepted_date, payload FROM raw.finnhub_financials_reported \
              WHERE symbol = $1 AND access_number = $2",
            &[&symbol.to_uppercase(), &accession],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}
