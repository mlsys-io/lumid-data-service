//! Government trades domain — congressional / executive-branch trades.
//! Port of api/queries/gov_trades.py.

use chrono::NaiveDate;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};
use tokio_postgres::types::ToSql;

use crate::db::rows::rows_to_objects;
use crate::error::ApiResult;

// `+ Send` so the params vec can be held across `.await` (Handler needs a Send future).
type Params = Vec<Box<dyn ToSql + Sync + Send>>;

fn refs(p: &Params) -> Vec<&(dyn ToSql + Sync)> {
    p.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

pub async fn for_symbol(
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
    let lim = params.len();
    let sql = format!(
        "SELECT date, chamber, member_name, party, state, \
                transaction_type, \
                amount_min::float8 AS amount_min, \
                amount_max::float8 AS amount_max \
           FROM ownership.gov_trades WHERE {} \
          ORDER BY date DESC NULLS LAST LIMIT ${lim}",
        where_.join(" AND ")
    );
    let client = pool.get().await?;
    let rows = client.query(&sql, &refs(&params)).await?;
    Ok(rows_to_objects(&rows))
}
