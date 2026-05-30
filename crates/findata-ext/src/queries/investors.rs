//! Investors domain — holders, insider, funds. Ports of
//! api/queries/{holders,insider,funds}.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};
use tokio_postgres::types::ToSql;

use findata::db::rows::rows_to_objects;
use findata::error::ApiResult;

// `+ Send` so the params vec can be held across `.await` (Handler needs a Send future).
type Params = Vec<Box<dyn ToSql + Sync + Send>>;

fn refs(p: &Params) -> Vec<&(dyn ToSql + Sync)> {
    p.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

// ----- holders -----
pub async fn holders_top(
    pool: &Pool,
    symbol: &str,
    asof: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Value> {
    let sym = symbol.to_uppercase();
    let client = pool.get().await?;
    let asof = match asof {
        Some(d) => Some(d),
        None => client
            .query_opt(
                "SELECT max(report_period) AS rp FROM ownership.institutional_positions \
                 WHERE symbol = $1 AND source = 'finnhub'",
                &[&sym],
            )
            .await?
            .and_then(|r| r.get::<_, Option<NaiveDate>>("rp")),
    };
    let Some(asof) = asof else {
        return Ok(json!({"symbol": sym, "as_of": null, "count": 0, "holders": []}));
    };
    let limit = limit.clamp(1, 100);
    let rows = client
        .query(
            "SELECT institution_name, shares::float8 AS shares, \
                    market_value::float8 AS market_value \
               FROM ownership.institutional_positions \
              WHERE symbol = $1 AND source = 'finnhub' AND report_period = $2 \
              ORDER BY market_value DESC NULLS LAST LIMIT $3",
            &[&sym, &asof, &limit],
        )
        .await?;
    let holders = rows_to_objects(&rows);
    Ok(json!({
        "symbol": sym, "as_of": asof.to_string(), "count": holders.len(), "holders": holders
    }))
}

// ----- insider -----
pub async fn insider_transactions(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: Params = vec![Box::new(symbol.to_uppercase())];
    let mut where_ = vec!["symbol = $1".to_string()];
    if let Some(d) = since {
        params.push(Box::new(d));
        where_.push(format!("date >= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 500)));
    let limit_idx = params.len();
    let sql = format!(
        "SELECT date, insider_name, insider_title, transaction_type, \
                shares::float8 AS shares, price::float8 AS price, value::float8 AS value \
           FROM ownership.insider_transactions WHERE {} \
          ORDER BY date DESC NULLS LAST LIMIT ${limit_idx}",
        where_.join(" AND ")
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn insider_sentiment(
    pool: &Pool,
    symbol: &str,
    limit_months: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit_months.clamp(1, 120);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT year, month, change_shares::float8 AS change_shares, mspr::float8 AS mspr \
               FROM ownership.insider_sentiment WHERE symbol = $1 \
              ORDER BY year DESC, month DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn insider_statistics(
    pool: &Pool,
    symbol: &str,
    limit_quarters: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit_quarters.clamp(1, 60);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT period_end_date, year, quarter, buys, sells, \
                    buys_value::float8 AS buys_value, sells_value::float8 AS sells_value \
               FROM ownership.insider_trading_statistics WHERE symbol = $1 \
              ORDER BY period_end_date DESC LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

// ----- funds -----
pub async fn fund_ownership(
    pool: &Pool,
    symbol: &str,
    asof: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Value> {
    let sym = symbol.to_uppercase();
    let client = pool.get().await?;
    let asof = match asof {
        Some(d) => Some(d),
        None => client
            .query_opt(
                "SELECT max(as_of) AS d FROM ownership.fund_ownership WHERE symbol=$1",
                &[&sym],
            )
            .await?
            .and_then(|r| r.get::<_, Option<NaiveDate>>("d")),
    };
    let Some(asof) = asof else {
        return Ok(json!({"symbol": sym, "as_of": null, "count": 0, "funds": []}));
    };
    let limit = limit.clamp(1, 200);
    let rows = client
        .query(
            "SELECT fund_name, shares::float8 AS shares, market_value::float8 AS market_value, \
                    weight_pct::float8 AS weight_pct \
               FROM ownership.fund_ownership WHERE symbol = $1 AND as_of = $2 \
              ORDER BY market_value DESC NULLS LAST LIMIT $3",
            &[&sym, &asof, &limit],
        )
        .await?;
    let funds = rows_to_objects(&rows);
    Ok(json!({"symbol": sym, "as_of": asof.to_string(), "count": funds.len(), "funds": funds}))
}

// ----- acquisitions (SC 13D/G) -----
pub async fn acquisitions(
    pool: &Pool,
    symbol: &str,
    since: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut params: Params = vec![Box::new(symbol.to_uppercase())];
    let mut where_ = vec!["symbol = $1".to_string()];
    if let Some(d) = since {
        params.push(Box::new(d));
        where_.push(format!("date >= ${}", params.len()));
    }
    params.push(Box::new(limit.clamp(1, 200)));
    let lim = params.len();
    let sql = format!(
        "SELECT date, filer, filer_type, percent_of_class::float8 AS percent_of_class, \
                shares_owned::float8 AS shares_owned \
           FROM ownership.acquisition_of_beneficial_ownership WHERE {} \
          ORDER BY date DESC NULLS LAST LIMIT ${lim}",
        where_.join(" AND ")
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn funds_disclosure(pool: &Pool, symbol: &str, limit: i64) -> ApiResult<Value> {
    let sym = symbol.to_uppercase();
    let limit = limit.clamp(1, 200);
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT fund_name, as_of, shares::float8 AS shares, weight_pct::float8 AS weight_pct \
               FROM ownership.funds_disclosure_holders_latest WHERE symbol = $1 \
              ORDER BY weight_pct DESC NULLS LAST LIMIT $2",
            &[&sym, &limit],
        )
        .await?;
    let funds = rows_to_objects(&rows);
    Ok(json!({"symbol": sym, "count": funds.len(), "funds": funds}))
}
