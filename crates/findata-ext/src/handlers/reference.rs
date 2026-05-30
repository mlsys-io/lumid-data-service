//! Reference depth + misc handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use findata::db::lineage::strip_lineage_rows;
use findata::error::ApiResult;
use crate::queries::reference as q;
use findata::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct CurrentOnly {
    #[serde(default)]
    pub current_only: bool,
}
pub async fn executives(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<CurrentOnly>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::executives(&st.pool, &symbol, p.current_only).await?)))
}

#[derive(Deserialize)]
pub struct Lim50 {
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 { 50 }
pub async fn compensation(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim50>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::compensation(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct Lim25 {
    #[serde(default = "d25")]
    pub limit: i64,
}
fn d25() -> i64 { 25 }
pub async fn peers(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim25>,
) -> ApiResult<Json<Vec<Value>>> {
    Ok(Json(q::peers(&st.pool, &symbol, p.limit).await?))
}

#[derive(Deserialize)]
pub struct SupplyParams {
    pub kind: Option<String>,
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn supply_chain(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SupplyParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::supply_chain(&st.pool, &symbol, p.kind.as_deref(), p.limit).await?)))
}

#[derive(Deserialize)]
pub struct Lim20 {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }
pub async fn shares_float(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim20>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::shares_float(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct Lim30 {
    #[serde(default = "d30")]
    pub limit: i64,
}
fn d30() -> i64 { 30 }
pub async fn employee_count(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim30>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::employee_count(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct SymChangeParams {
    pub symbol: Option<String>,
    pub since: Option<NaiveDate>,
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }
pub async fn symbol_changes(
    State(st): State<AppState>,
    Query(p): Query<SymChangeParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(
        q::symbol_changes(&st.pool, p.symbol.as_deref(), p.since, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct HolidayParams {
    #[serde(rename = "from")]
    pub since: Option<NaiveDate>,
    #[serde(rename = "to")]
    pub until: Option<NaiveDate>,
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 { 200 }
pub async fn exchange_holidays(
    State(st): State<AppState>,
    Path(exchange): Path<String>,
    Query(p): Query<HolidayParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(
        q::exchange_holidays(&st.pool, &exchange, p.since, p.until, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct ExchangeParam {
    pub exchange: Option<String>,
}
pub async fn exchange_market_hours(
    State(st): State<AppState>,
    Query(p): Query<ExchangeParam>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::exchange_hours(&st.pool, p.exchange.as_deref()).await?)))
}
