//! Corp-actions handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::corp_actions as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct DivParams {
    pub since: Option<NaiveDate>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 { 50 }

pub async fn dividends(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<DivParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(q::dividends(&st.pool, &symbol, p.since, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct SplitParams {
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn splits(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SplitParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(q::splits(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct McapParams {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    #[serde(default = "d1000")]
    pub limit: i64,
}
fn d1000() -> i64 { 1000 }

pub async fn market_cap_history(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<McapParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::market_cap_history(&st.pool, &symbol, p.start, p.end, p.limit).await?,
    )))
}
