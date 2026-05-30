//! Regulatory + ESG + filings — ports of api/queries/{filings,esg,regulatory_extra}.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

pub async fn filings(
    pool: &Pool,
    symbol: &str,
    form: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(f) = form {
        qb.cmp("upper(form)", "=", f.to_uppercase());
    }
    let lim = qb.push(limit.clamp(1, 200));
    let sql = format!(
        "SELECT accession_no, form, filed_date, accepted_date, report_url, filing_url \
           FROM regulatory.sec_filings WHERE {} ORDER BY filed_date DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

// ----- ESG -----
pub async fn esg_disclosures(
    pool: &Pool,
    symbol: &str,
    year: Option<i32>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(y) = year {
        qb.eq("year", y);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT year, category, metric, value::float8 AS value, unit FROM regulatory.esg_disclosures \
          WHERE {} ORDER BY year DESC NULLS LAST, category, metric LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn esg_ratings(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT year, environmental::float8 AS environmental, social::float8 AS social, \
                    governance::float8 AS governance, total::float8 AS total, risk_rating, industry_rank \
               FROM regulatory.esg_ratings WHERE symbol = $1 ORDER BY year DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn esg_historical(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT year, environmental::float8 AS environmental, social::float8 AS social, \
                    governance::float8 AS governance, total::float8 AS total \
               FROM regulatory.esg_historical WHERE symbol = $1 ORDER BY year DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

// ----- regulatory_extra -----
pub async fn lobbying(
    pool: &Pool,
    symbol: &str,
    year: Option<i32>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(y) = year {
        qb.eq("year", y);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT filing_date, senate_id, registrant, client, issue, year, amount::float8 AS amount \
           FROM regulatory.lobbying WHERE {} ORDER BY filing_date DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn usa_spending(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(d) = since {
        qb.cmp("action_date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT action_date, recipient, awarding_agency, amount::float8 AS amount, naics_code, description \
           FROM regulatory.usa_spending WHERE {} ORDER BY action_date DESC LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn uspto_patents(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(d) = since {
        qb.cmp("filing_date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT filing_date, granted_date, patent_id, title FROM regulatory.uspto_patents \
          WHERE {} ORDER BY filing_date DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn visa_applications(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(d) = since {
        qb.cmp("received_date", ">=", d);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT received_date, case_number, case_status, visa_class, employer_name, job_title, \
                wage_rate::float8 AS wage_rate FROM regulatory.visa_applications \
          WHERE {} ORDER BY received_date DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}
