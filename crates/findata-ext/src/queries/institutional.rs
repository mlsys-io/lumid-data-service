//! Institutional 13-F analytics — port of routes/institutional.py inline SQL.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use findata::db::qb::Qb;
use findata::db::rows::rows_to_objects;
use findata::error::ApiResult;

pub async fn holder_analytics(
    pool: &Pool,
    symbol: &str,
    year: Option<i32>,
    quarter: Option<i32>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("symbol", symbol.to_uppercase());
    if let Some(y) = year {
        qb.cmp("extract(year from period_end_date)::int", "=", y);
    }
    if let Some(q) = quarter {
        qb.cmp("extract(quarter from period_end_date)::int", "=", q);
    }
    let lim = qb.push(limit.clamp(1, 1000));
    let sql = format!(
        "SELECT period_end_date, cik, investor_name, industry_title, weight, last_weight, \
                change_in_weight, change_in_weight_percentage, market_value, last_market_value, \
                change_in_market_value, shares_number, last_shares_number, change_in_shares_number, \
                ownership, change_in_ownership, holding_period, first_added, is_new, is_sold_out, \
                performance, performance_percentage, avg_price_paid, quarter_end_price \
           FROM ownership.institutional_holder_analytics WHERE {} \
          ORDER BY period_end_date DESC, market_value DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn holder_performance(pool: &Pool, cik: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 500);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT date, investor_name, portfolio_size, securities_added, securities_removed, \
                    market_value, previous_market_value, change_in_market_value, change_in_market_value_percentage, \
                    average_holding_period, average_holding_period_top10, average_holding_period_top20, \
                    turnover, performance, performance_percentage, performance1year, performance_percentage1year, \
                    performance3year, performance_percentage3year, performance5year, performance_percentage5year, \
                    performance_relative_to_sp500_percentage FROM ownership.holder_performance_summary \
              WHERE cik = $1 ORDER BY date DESC LIMIT $2",
            &[&cik, &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn holder_industries(
    pool: &Pool,
    cik: &str,
    year: Option<i32>,
    quarter: Option<i32>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.eq("cik", cik.to_string());
    if let Some(y) = year {
        qb.cmp("extract(year from date)::int", "=", y);
    }
    if let Some(q) = quarter {
        qb.cmp("extract(quarter from date)::int", "=", q);
    }
    let lim = qb.push(limit.clamp(1, 500));
    let sql = format!(
        "SELECT date, investor_name, industry_title, weight, last_weight, change_in_weight, \
                change_in_weight_percentage, performance, performance_percentage, last_performance, \
                change_in_performance, number_of_companies, last_number_of_companies \
           FROM ownership.holder_industry_breakdown WHERE {} \
          ORDER BY date DESC, weight DESC NULLS LAST LIMIT ${lim}",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn holder_dates(pool: &Pool, cik: &str, limit: i64) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 500);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT date, filing_date, form_type, quarter, year FROM ownership.institutional_dates \
              WHERE cik = $1 ORDER BY date DESC LIMIT $2",
            &[&cik, &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn industries_summary(
    pool: &Pool,
    year: Option<i32>,
    quarter: Option<i32>,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    qb.where_.push("TRUE".to_string());
    if let Some(y) = year {
        qb.cmp("extract(year from date)::int", "=", y);
    }
    if let Some(q) = quarter {
        qb.cmp("extract(quarter from date)::int", "=", q);
    }
    let sql = format!(
        "SELECT industry_title, date, industry_value FROM ownership.institutional_industry_summary \
          WHERE {} ORDER BY date DESC, industry_value DESC NULLS LAST LIMIT 1000",
        qb.and_join()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}
