//! Institutional 13-F handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use findata::db::lineage::strip_lineage_rows;
use findata::error::ApiResult;
use crate::queries::institutional as q;
use findata::state::AppState;

#[derive(Deserialize)]
pub struct YQLimit {
    pub year: Option<i32>,
    pub quarter: Option<i32>,
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 { 200 }

pub async fn holder_analytics(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<YQLimit>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::holder_analytics(&st.pool, &symbol, p.year, p.quarter, p.limit).await?);
    Ok(Json(json!({"symbol": symbol.to_uppercase(), "count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct Lim100 {
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }

pub async fn holder_performance(
    State(st): State<AppState>,
    Path(cik): Path<String>,
    Query(p): Query<Lim100>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::holder_performance(&st.pool, &cik, p.limit).await?);
    Ok(Json(json!({"cik": cik, "count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct YQLimit50 {
    pub year: Option<i32>,
    pub quarter: Option<i32>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 { 50 }

pub async fn holder_industries(
    State(st): State<AppState>,
    Path(cik): Path<String>,
    Query(p): Query<YQLimit50>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::holder_industries(&st.pool, &cik, p.year, p.quarter, p.limit).await?);
    Ok(Json(json!({"cik": cik, "count": data.len(), "data": data})))
}

pub async fn holder_dates(
    State(st): State<AppState>,
    Path(cik): Path<String>,
    Query(p): Query<Lim100>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::holder_dates(&st.pool, &cik, p.limit).await?);
    Ok(Json(json!({"cik": cik, "count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct YQ {
    pub year: Option<i32>,
    pub quarter: Option<i32>,
}

pub async fn industries_summary(
    State(st): State<AppState>,
    Query(p): Query<YQ>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::industries_summary(&st.pool, p.year, p.quarter).await?);
    Ok(Json(json!({"count": data.len(), "data": data})))
}
