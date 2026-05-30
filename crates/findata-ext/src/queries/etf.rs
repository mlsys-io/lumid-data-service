//! ETF cluster — port of api/queries/etf.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};

use findata::db::rows::{row_to_object, rows_to_objects};
use findata::error::ApiResult;

pub async fn info(pool: &Pool, symbol: &str) -> ApiResult<Option<Map<String, Value>>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT symbol, name, description, isin, asset_class, security_cusip, domicile, \
                    website, etf_company, expense_ratio::float8 AS expense_ratio, \
                    assets_under_management::float8 AS assets_under_management, \
                    avg_volume::float8 AS avg_volume, inception_date, nav::float8 AS nav, \
                    nav_currency, holdings_count, is_actively_trading, updated_at, sectors \
               FROM reference.etf_info WHERE symbol = $1 AND source = 'fmp' LIMIT 1",
            &[&symbol.to_uppercase()],
        )
        .await?;
    Ok(row.map(|r| row_to_object(&r)))
}

pub async fn holdings(
    pool: &Pool,
    etf_symbol: &str,
    asof: Option<NaiveDate>,
    limit: i64,
) -> ApiResult<Value> {
    let sym = etf_symbol.to_uppercase();
    let client = pool.get().await?;
    let asof = match asof {
        Some(d) => Some(d),
        None => client
            .query_opt("SELECT max(as_of) AS d FROM reference.etf_holdings WHERE etf_symbol=$1", &[&sym])
            .await?
            .and_then(|r| r.get::<_, Option<NaiveDate>>("d")),
    };
    let Some(asof) = asof else {
        return Ok(json!({"etf_symbol": sym, "as_of": null, "count": 0, "holdings": []}));
    };
    let limit = limit.clamp(1, 500);
    let rows = client
        .query(
            "SELECT asset_symbol, asset_name, isin, cusip, shares_number::float8 AS shares_number, \
                    weight_percentage::float8 AS weight_pct, market_value::float8 AS market_value \
               FROM reference.etf_holdings WHERE etf_symbol = $1 AND as_of = $2 \
              ORDER BY weight_percentage DESC NULLS LAST LIMIT $3",
            &[&sym, &asof, &limit],
        )
        .await?;
    let holdings = rows_to_objects(&rows);
    Ok(json!({"etf_symbol": sym, "as_of": asof.to_string(), "count": holdings.len(), "holdings": holdings}))
}

pub async fn sector_weightings(pool: &Pool, etf_symbol: &str) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            // ETFWeighting model carries both sector + country (one null per row).
            "SELECT sector, NULL::text AS country, weight_percentage::float8 AS weight_pct, as_of \
               FROM reference.etf_sector_weightings WHERE etf_symbol = $1 \
              ORDER BY weight_percentage DESC NULLS LAST",
            &[&etf_symbol.to_uppercase()],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn country_weightings(pool: &Pool, etf_symbol: &str) -> ApiResult<Vec<Map<String, Value>>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT NULL::text AS sector, country, weight_percentage::float8 AS weight_pct, as_of \
               FROM reference.etf_country_weightings WHERE etf_symbol = $1 \
              ORDER BY weight_percentage DESC NULLS LAST",
            &[&etf_symbol.to_uppercase()],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}

pub async fn symbol_etf_exposure(
    pool: &Pool,
    symbol: &str,
    limit: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let limit = limit.clamp(1, 500);
    let client = pool.get().await?;
    let rows = client
        .query(
            "WITH latest AS (\
               SELECT DISTINCT ON (etf_symbol) etf_symbol, as_of, weight_pct, shares, market_value \
                 FROM reference.etf_asset_exposure WHERE holding_symbol = $1 \
                ORDER BY etf_symbol, as_of DESC) \
             SELECT etf_symbol, as_of, weight_pct::float8 AS weight_pct, shares::float8 AS shares, \
                    market_value::float8 AS market_value FROM latest \
              ORDER BY weight_pct DESC NULLS LAST LIMIT $2",
            &[&symbol.to_uppercase(), &limit],
        )
        .await?;
    Ok(rows_to_objects(&rows))
}
