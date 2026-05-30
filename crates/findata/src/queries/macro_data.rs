//! Macro — treasury rates, economic indicators/calendar, COT.
//! Port of api/queries/macro_extra.py. (Module named `macro_data` — `macro`
//! is a reserved Rust keyword.)

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn treasury_rates(
    pool: &Pool,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(d) = start {
        qb.cmp("date", ">=", d);
    }
    if let Some(d) = end {
        qb.cmp("date", "<=", d);
    }
    let lim = qb.push(limit.clamp(1, 5000));
    let sql = format!(
        "SELECT date, m1::float8 AS m1, m2::float8 AS m2, m3::float8 AS m3, m6::float8 AS m6, \
                y1::float8 AS y1, y2::float8 AS y2, y3::float8 AS y3, y5::float8 AS y5, \
                y7::float8 AS y7, y10::float8 AS y10, y20::float8 AS y20, y30::float8 AS y30 \
           FROM macro.treasury_rates {} ORDER BY date DESC LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    let mut out = rows_to_objects(&rows);
    out.sort_by(|a, b| a.get("date").and_then(|v| v.as_str()).cmp(&b.get("date").and_then(|v| v.as_str())));
    Ok(out)
}

pub async fn economic_indicators(
    pool: &Pool,
    indicator: Option<&str>,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(i) = indicator {
        qb.eq("indicator", i.to_string());
    }
    if let Some(d) = since {
        qb.cmp("date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 5000));
    let sql = format!(
        "SELECT indicator, date, value::float8 AS value FROM macro.economic_indicators {} \
          ORDER BY indicator, date DESC LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn economic_calendar(
    pool: &Pool,
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
    country: Option<&str>,
    impact: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(d) = since {
        qb.cmp("date", ">=", d);
    }
    if let Some(d) = until {
        qb.cmp("date", "<=", d);
    }
    if let Some(c) = country {
        qb.eq("country", c.to_string());
    }
    if let Some(i) = impact {
        qb.eq("impact", i.to_string());
    }
    let lim = qb.push(limit.clamp(1, 5000));
    let sql = format!(
        "SELECT date, event, country, actual, estimate, previous, impact FROM macro.economic_calendar {} \
          ORDER BY date DESC LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn cot(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 100);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT report_date, name, sector, open_interest_all::float8 AS open_interest, \
                    noncomm_positions_long_all::float8 AS noncomm_long, \
                    noncomm_positions_short_all::float8 AS noncomm_short, \
                    comm_positions_long_all::float8 AS comm_long, \
                    comm_positions_short_all::float8 AS comm_short, \
                    change_in_open_interest_all::float8 AS change_oi \
               FROM macro.commitment_of_traders WHERE symbol = $1 ORDER BY report_date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
