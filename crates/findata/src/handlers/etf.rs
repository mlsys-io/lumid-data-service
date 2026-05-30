//! ETF handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::{ApiError, ApiResult};
use crate::queries::etf as q;
use crate::state::AppState;

pub async fn info(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = q::info(&st.pool, &symbol)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no ETF info for {symbol:?}")))?;
    Ok(Json(row))
}

#[derive(Deserialize)]
pub struct HoldingsParams {
    pub asof: Option<NaiveDate>,
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }

pub async fn holdings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<HoldingsParams>,
) -> ApiResult<Json<Value>> {
    Ok(Json(q::holdings(&st.pool, &symbol, p.asof, p.limit).await?))
}

pub async fn sector_weightings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(q::sector_weightings(&st.pool, &symbol).await?))
}

pub async fn country_weightings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(q::country_weightings(&st.pool, &symbol).await?))
}

#[derive(Deserialize)]
pub struct ExposureParams {
    #[serde(default = "d100")]
    pub limit: i64,
}
pub async fn symbol_etf_exposure(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<ExposureParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(q::symbol_etf_exposure(&st.pool, &symbol, p.limit).await?))
}
