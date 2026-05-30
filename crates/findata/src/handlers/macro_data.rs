//! Macro handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::macro_data as q;
use crate::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct TreasuryParams {
    #[serde(rename = "from")]
    pub start: Option<NaiveDate>,
    #[serde(rename = "to")]
    pub end: Option<NaiveDate>,
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 { 200 }
pub async fn treasury_rates(
    State(st): State<AppState>,
    Query(p): Query<TreasuryParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::treasury_rates(&st.pool, p.start, p.end, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct IndicatorParams {
    pub indicator: Option<String>,
    pub since: Option<NaiveDate>,
    #[serde(default = "d200")]
    pub limit: i64,
}
pub async fn economic_indicators(
    State(st): State<AppState>,
    Query(p): Query<IndicatorParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(
        q::economic_indicators(&st.pool, p.indicator.as_deref(), p.since, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct CalendarParams {
    #[serde(rename = "from")]
    pub since: Option<NaiveDate>,
    #[serde(rename = "to")]
    pub until: Option<NaiveDate>,
    pub country: Option<String>,
    pub impact: Option<String>,
    #[serde(default = "d200")]
    pub limit: i64,
}
pub async fn economic_calendar(
    State(st): State<AppState>,
    Query(p): Query<CalendarParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(
        q::economic_calendar(&st.pool, p.since, p.until, p.country.as_deref(), p.impact.as_deref(), p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct CotParams {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }
pub async fn cot(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<CotParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::cot(&st.pool, &symbol, p.limit).await?)))
}
