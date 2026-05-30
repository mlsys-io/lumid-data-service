//! Regulatory + ESG + filings handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::regulatory as q;
use crate::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct FilingsParams {
    pub form: Option<String>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 { 50 }
pub async fn filings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<FilingsParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::filings(&st.pool, &symbol, p.form.as_deref(), p.limit).await?)))
}

#[derive(Deserialize)]
pub struct YearLimit {
    pub year: Option<i32>,
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }
pub async fn esg_disclosures(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<YearLimit>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::esg_disclosures(&st.pool, &symbol, p.year, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct Lim20 {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }
pub async fn esg_ratings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim20>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::esg_ratings(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct Lim30 {
    #[serde(default = "d30")]
    pub limit: i64,
}
fn d30() -> i64 { 30 }
pub async fn esg_historical(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim30>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::esg_historical(&st.pool, &symbol, p.limit).await?)))
}

pub async fn lobbying(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<YearLimit>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::lobbying(&st.pool, &symbol, p.year, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct SinceLimit {
    pub since: Option<NaiveDate>,
    #[serde(default = "d100")]
    pub limit: i64,
}
pub async fn usa_spending(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SinceLimit>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::usa_spending(&st.pool, &symbol, p.since, p.limit).await?)))
}
pub async fn uspto_patents(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SinceLimit>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::uspto_patents(&st.pool, &symbol, p.since, p.limit).await?)))
}
pub async fn visa_applications(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SinceLimit>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::visa_applications(&st.pool, &symbol, p.since, p.limit).await?)))
}
