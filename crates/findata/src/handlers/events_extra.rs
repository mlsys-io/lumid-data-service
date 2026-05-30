//! Events-extra handlers — IPOs, M&A, FDA.
use axum::extract::{Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::events_extra as q;
use crate::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct IposParams {
    #[serde(rename = "from")]
    pub since: Option<NaiveDate>,
    #[serde(rename = "to")]
    pub until: Option<NaiveDate>,
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }
pub async fn ipos(State(st): State<AppState>, Query(p): Query<IposParams>) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::ipos(&st.pool, p.since, p.until, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct MnaParams {
    #[serde(rename = "from")]
    pub since: Option<NaiveDate>,
    pub accepting_symbol: Option<String>,
    pub target_symbol: Option<String>,
    #[serde(default = "d100")]
    pub limit: i64,
}
pub async fn mergers_acquisitions(
    State(st): State<AppState>,
    Query(p): Query<MnaParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(
        q::mergers_acquisitions(&st.pool, p.since, p.accepting_symbol.as_deref(), p.target_symbol.as_deref(), p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct FdaParams {
    #[serde(rename = "from")]
    pub since: Option<NaiveDate>,
    #[serde(default = "d100")]
    pub limit: i64,
}
pub async fn fda_calendar(State(st): State<AppState>, Query(p): Query<FdaParams>) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::fda_calendar(&st.pool, p.since, p.limit).await?)))
}
