//! Investors handlers — holders, insider, funds.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::investors as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct AsofLimit {
    pub asof: Option<NaiveDate>,
    #[serde(default = "d_holders")]
    pub limit: i64,
}
fn d_holders() -> i64 {
    25
}

pub async fn holders_top(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<AsofLimit>,
) -> ApiResult<Json<Value>> {
    Ok(Json(q::holders_top(&st.pool, &symbol, p.asof, p.limit).await?))
}

#[derive(Deserialize)]
pub struct SinceLimit {
    pub since: Option<NaiveDate>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 {
    50
}

pub async fn insider_transactions(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SinceLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::insider_transactions(&st.pool, &symbol, p.since, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct MonthsParam {
    #[serde(default = "d24")]
    pub limit_months: i64,
}
fn d24() -> i64 {
    24
}

pub async fn insider_sentiment(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<MonthsParam>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::insider_sentiment(&st.pool, &symbol, p.limit_months).await?,
    )))
}

#[derive(Deserialize)]
pub struct QuartersParam {
    #[serde(default = "d16")]
    pub limit_quarters: i64,
}
fn d16() -> i64 {
    16
}

pub async fn insider_statistics(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<QuartersParam>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::insider_statistics(&st.pool, &symbol, p.limit_quarters).await?,
    )))
}

#[derive(Deserialize)]
pub struct FundAsofLimit {
    pub asof: Option<NaiveDate>,
    #[serde(default = "d50")]
    pub limit: i64,
}

pub async fn fund_ownership(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<FundAsofLimit>,
) -> ApiResult<Json<Value>> {
    Ok(Json(q::fund_ownership(&st.pool, &symbol, p.asof, p.limit).await?))
}

pub async fn funds_disclosure(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<FundAsofLimit>,
) -> ApiResult<Json<Value>> {
    Ok(Json(q::funds_disclosure(&st.pool, &symbol, p.limit).await?))
}

#[derive(Deserialize)]
pub struct AcqParams {
    pub since: Option<NaiveDate>,
    #[serde(default = "d_acq")]
    pub limit: i64,
}
fn d_acq() -> i64 {
    50
}
pub async fn acquisitions(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<AcqParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::acquisitions(&st.pool, &symbol, p.since, p.limit).await?,
    )))
}
