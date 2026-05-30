//! Earnings calendar handler.

use axum::extract::{Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::earnings as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct EarningsParams {
    pub symbol: Option<String>,
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 {
    50
}

pub async fn earnings_calendar(
    State(st): State<AppState>,
    Query(p): Query<EarningsParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    let rows = q::calendar(&st.pool, p.symbol.as_deref(), p.start, p.end, p.limit).await?;
    Ok(Json(strip_lineage_rows(rows)))
}
