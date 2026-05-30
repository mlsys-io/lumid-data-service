//! Events extras — IPOs, M&A, FDA calendar. Port of api/queries/events_extra.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn ipos(
    pool: &Pool,
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(d) = since {
        qb.cmp("ipo_date", ">=", d);
    }
    if let Some(d) = until {
        qb.cmp("ipo_date", "<=", d);
    }
    let lim = qb.push(limit.clamp(1, 1000));
    // `price` is text (dirty FMP strings) — clean entities/whitespace then cast or null.
    let sql = format!(
        r#"SELECT symbol, ipo_date, exchange, name, number_of_shares::float8 AS number_of_shares,
               CASE WHEN btrim(regexp_replace(price, '&[^;]+;|\s', '', 'g')) ~ '^-?[0-9.]+$'
                    THEN btrim(regexp_replace(price, '&[^;]+;|\s', '', 'g'))::float8
                    ELSE NULL END AS price,
               total_shares_value::float8 AS total_shares_value, status
          FROM events.ipos {} ORDER BY ipo_date DESC NULLS LAST LIMIT ${lim}"#,
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn mergers_acquisitions(
    pool: &Pool,
    since: Option<NaiveDate>,
    accepting_symbol: Option<&str>,
    target_symbol: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(d) = since {
        qb.cmp("announced_date", ">=", d);
    }
    if let Some(s) = accepting_symbol {
        qb.eq("accepting_symbol", s.to_uppercase());
    }
    if let Some(s) = target_symbol {
        qb.eq("target_symbol", s.to_uppercase());
    }
    let lim = qb.push(limit.clamp(1, 1000));
    let sql = format!(
        "SELECT announced_date, accepted_date, accepting_symbol, accepting_name, accepting_cik, \
                target_symbol, target_name, target_cik, expected_close_date, \
                deal_value::float8 AS deal_value, deal_currency, status, link \
           FROM events.mergers_acquisitions {} ORDER BY announced_date DESC NULLS LAST LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn fda_calendar(
    pool: &Pool,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    if let Some(d) = since {
        qb.cmp("from_date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 1000));
    let sql = format!(
        "SELECT from_date, to_date, event, url FROM events.fda_calendar {} \
          ORDER BY from_date DESC NULLS LAST LIMIT ${lim}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}
