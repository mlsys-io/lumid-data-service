//! Server-side screener — port of api/queries/screener.py.

use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use crate::db::qb::Qb;
use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

#[derive(Clone, Default)]
pub struct Filters {
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub country: Option<String>,
    pub exchange: Option<String>,
    pub is_etf: Option<bool>,
    pub is_fund: Option<bool>,
    pub market_cap_min: Option<f64>,
    pub market_cap_max: Option<f64>,
    pub symbol_prefix: Option<String>,
}

// Push predicates in the same order as the Python builder.
fn build(qb: &mut Qb, f: &Filters) {
    if let Some(p) = &f.symbol_prefix {
        qb.cmp("a.symbol", "ILIKE", format!("{}%", p.to_uppercase()));
    }
    if let Some(c) = &f.country {
        qb.cmp("upper(a.country)", "=", c.to_uppercase());
    }
    if let Some(e) = &f.exchange {
        qb.cmp("upper(a.exchange)", "=", e.to_uppercase());
    }
    if let Some(v) = f.is_etf {
        qb.eq("a.is_etf", v);
    }
    if let Some(v) = f.is_fund {
        qb.eq("a.is_fund", v);
    }
    if let Some(s) = &f.sector {
        qb.eq("p.sector", s.clone());
    }
    if let Some(i) = &f.industry {
        qb.eq("p.industry", i.clone());
    }
    // Cast the numeric column to float8 so the bound f64 param type matches
    // (tokio-postgres won't serialize f64 into a `numeric` parameter slot).
    if let Some(v) = f.market_cap_min {
        qb.cmp("p.market_cap::float8", ">=", v);
    }
    if let Some(v) = f.market_cap_max {
        qb.cmp("p.market_cap::float8", "<=", v);
    }
}

const LATEST_PROFILE_CTE: &str = "WITH latest_profile AS (\
    SELECT DISTINCT ON (symbol) symbol, sector, industry, market_cap \
      FROM reference.profile WHERE source='fmp' ORDER BY symbol, ingest_ts DESC)";

pub async fn screen(
    pool: &Pool,
    f: &Filters,
    limit: i64,
    offset: i64,
) -> ApiResult<Vec<Map<String, Value>>> {
    let mut qb = Qb::new();
    build(&mut qb, f);
    let lim = qb.push(limit.clamp(1, 1000));
    let off = qb.push(offset.max(0));
    let sql = format!(
        "{LATEST_PROFILE_CTE} \
         SELECT a.symbol, a.name, a.exchange, a.country, a.is_etf, a.is_fund, \
                p.sector, p.industry, p.market_cap::float8 AS market_cap \
           FROM reference.active_symbols a \
           LEFT JOIN latest_profile p ON p.symbol = a.symbol {} \
          ORDER BY p.market_cap DESC NULLS LAST, a.symbol LIMIT ${lim} OFFSET ${off}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &qb.refs()).await?;
    Ok(rows_to_objects(&rows))
}

pub async fn count(pool: &Pool, f: &Filters) -> ApiResult<i64> {
    let mut qb = Qb::new();
    build(&mut qb, f);
    let sql = format!(
        "{LATEST_PROFILE_CTE} SELECT count(*) FROM reference.active_symbols a \
           LEFT JOIN latest_profile p ON p.symbol = a.symbol {}",
        qb.where_clause()
    );
    let client = pool.get().await?;
    let row = client.query_one(&sql, &qb.refs()).await?;
    Ok(row.get::<_, i64>(0))
}
