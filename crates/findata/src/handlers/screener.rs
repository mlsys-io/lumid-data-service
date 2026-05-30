//! Screener handler.
use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::queries::screener::{self as q, Filters};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct ScreenParams {
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub country: Option<String>,
    pub exchange: Option<String>,
    pub is_etf: Option<bool>,
    pub is_fund: Option<bool>,
    pub market_cap_min: Option<f64>,
    pub market_cap_max: Option<f64>,
    pub symbol_prefix: Option<String>,
    #[serde(default = "d100")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}
fn d100() -> i64 { 100 }

pub async fn screener(
    State(st): State<AppState>,
    Query(p): Query<ScreenParams>,
) -> ApiResult<Json<Value>> {
    let f = Filters {
        sector: p.sector,
        industry: p.industry,
        country: p.country,
        exchange: p.exchange,
        is_etf: p.is_etf,
        is_fund: p.is_fund,
        market_cap_min: p.market_cap_min,
        market_cap_max: p.market_cap_max,
        symbol_prefix: p.symbol_prefix,
    };
    let hits = q::screen(&st.pool, &f, p.limit, p.offset).await?;
    let total = q::count(&st.pool, &f).await?;
    Ok(Json(json!({"count": total, "returned": hits.len(), "hits": hits})))
}
