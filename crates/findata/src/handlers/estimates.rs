//! Estimates handlers — price-target, grades, recommendation, analyst-estimates.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::{strip_lineage, strip_lineage_rows};
use crate::error::{ApiError, ApiResult};
use crate::queries::estimates as q;
use crate::state::AppState;

pub async fn price_target(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = q::price_target(&st.pool, &symbol)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no price target for {symbol:?}")))?;
    Ok(Json(strip_lineage(row)))
}

#[derive(Deserialize)]
pub struct LimitParam {
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 {
    50
}

pub async fn grades(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<LimitParam>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(q::grades(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct RecParam {
    #[serde(default = "d12")]
    pub limit: i64,
}
fn d12() -> i64 {
    12
}

pub async fn recommendation(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<RecParam>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::recommendation(&st.pool, &symbol, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct AnalystParams {
    #[serde(default = "quarter")]
    pub period: String,
    pub since: Option<String>,
    #[serde(default = "d20")]
    pub limit_periods: i64,
}
fn quarter() -> String {
    "quarter".into()
}
fn d20() -> i64 {
    20
}

pub async fn analyst_estimates(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<AnalystParams>,
) -> ApiResult<Json<Vec<Value>>> {
    let rows =
        q::analyst_estimates(&st.pool, &symbol, &p.period, p.since.as_deref(), p.limit_periods)
            .await?;
    Ok(Json(rows))
}
