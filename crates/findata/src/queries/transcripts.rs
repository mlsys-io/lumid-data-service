//! Earnings-call transcripts — port of api/queries/transcripts.py.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::{row_to_object, rows_to_objects};
use crate::error::ApiResult;

pub async fn list_for_symbol(
    pool: &Pool,
    symbol: &str,
    year: Option<i32>,
    quarter: Option<i32>,
    limit: i64,
    include_body: bool,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(y) = year {
        qb.eq("fiscal_year", y);
    }
    if let Some(q) = quarter {
        qb.eq("quarter", q);
    }
    let lim = qb.push(limit.clamp(1, 50));
    let body_col = if include_body {
        "transcript"
    } else {
        "left(coalesce(transcript,''), 500) AS transcript_excerpt"
    };
    let sql = format!(
        "SELECT symbol, fiscal_year, quarter, call_date, {body_col} \
           FROM raw.fmp_earning_call_transcripts WHERE {} \
          ORDER BY fiscal_year DESC, quarter DESC LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn one_full(
    pool: &Pool,
    symbol: &str,
    year: i32,
    quarter: i32,
) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT symbol, fiscal_year, quarter, call_date, transcript \
               FROM raw.fmp_earning_call_transcripts \
              WHERE symbol = $1 AND fiscal_year = $2 AND quarter = $3 LIMIT 1",
            &[&symbol.to_uppercase(), &year, &quarter],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}
