//! Market-extras handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{Duration, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::market_extras as q;
use crate::state::AppState;

fn today() -> NaiveDate {
    Utc::now().date_naive()
}

#[derive(Deserialize)]
pub struct MoversParams {
    #[serde(default = "gainer")]
    pub kind: String,
    #[serde(default = "d20")]
    pub limit: i64,
}
fn gainer() -> String { "gainer".into() }
fn d20() -> i64 { 20 }

pub async fn market_movers(
    State(st): State<AppState>,
    Query(p): Query<MoversParams>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::market_movers(&st.pool, &p.kind, p.limit).await?);
    Ok(Json(json!({"kind": p.kind, "count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct CalParams {
    #[serde(rename = "from")]
    pub from_: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 { 200 }

pub async fn dividends_calendar(
    State(st): State<AppState>,
    Query(p): Query<CalParams>,
) -> ApiResult<Json<Value>> {
    let from = p.from_.unwrap_or_else(|| today() - Duration::days(7));
    let to = p.to.unwrap_or_else(|| today() + Duration::days(60));
    let data = strip_lineage_rows(q::dividends_calendar(&st.pool, from, to, p.limit).await?);
    Ok(Json(json!({"count": data.len(), "data": data})))
}

pub async fn splits_calendar(
    State(st): State<AppState>,
    Query(p): Query<CalParams>,
) -> ApiResult<Json<Value>> {
    let from = p.from_.unwrap_or_else(|| today() - Duration::days(30));
    let to = p.to.unwrap_or_else(|| today() + Duration::days(90));
    let data = strip_lineage_rows(q::splits_calendar(&st.pool, from, to, p.limit).await?);
    Ok(Json(json!({"count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct ExchangeParam {
    pub exchange: Option<String>,
}

macro_rules! snapshot_handler {
    ($name:ident, $qfn:ident) => {
        pub async fn $name(
            State(st): State<AppState>,
            Query(p): Query<ExchangeParam>,
        ) -> ApiResult<Json<Value>> {
            let data = strip_lineage_rows(q::$qfn(&st.pool, p.exchange.as_deref()).await?);
            Ok(Json(json!({"count": data.len(), "data": data})))
        }
    };
}
snapshot_handler!(sectors_pe, sectors_pe);
snapshot_handler!(sectors_perf, sectors_perf);
snapshot_handler!(industries_pe, industries_pe);
snapshot_handler!(industries_perf, industries_perf);

pub async fn exec_comp(
    State(st): State<AppState>,
    Path(industry): Path<String>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::exec_comp(&st.pool, &industry).await?);
    Ok(Json(json!({"count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct UniverseParams {
    #[serde(default = "d1000")]
    pub limit: i64,
}
fn d1000() -> i64 { 1000 }

pub async fn universe_active(
    State(st): State<AppState>,
    Query(p): Query<UniverseParams>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::universe_active(&st.pool, p.limit).await?);
    Ok(Json(json!({"count": data.len(), "data": data})))
}

#[derive(Deserialize)]
pub struct IndexParams {
    pub as_of: Option<NaiveDate>,
}

pub async fn index_constituents(
    State(st): State<AppState>,
    Path(index_symbol): Path<String>,
    Query(p): Query<IndexParams>,
) -> ApiResult<Json<Value>> {
    let as_of = p.as_of.unwrap_or_else(today);
    let data = strip_lineage_rows(q::index_constituents(&st.pool, &index_symbol, as_of).await?);
    Ok(Json(json!({
        "index_symbol": index_symbol.to_uppercase(),
        "as_of": as_of.to_string(),
        "count": data.len(),
        "data": data,
    })))
}
