//! Valuation handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::valuation as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct Lim {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }

#[derive(Deserialize)]
pub struct PeriodLim {
    #[serde(default = "quarter")]
    pub period: String,
    #[serde(default = "d20")]
    pub limit: i64,
}
fn quarter() -> String { "quarter".into() }

pub async fn dcf(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(q::dcf(&st.pool, &symbol, p.limit).await?)))
}

pub async fn enterprise_value(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLim>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::enterprise_values(&st.pool, &symbol, &p.period, p.limit).await?,
    )))
}

pub async fn financial_scores(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(q::financial_scores(&st.pool, &symbol, p.limit).await?)))
}

pub async fn owner_earnings(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLim>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::owner_earnings(&st.pool, &symbol, &p.period, p.limit).await?,
    )))
}
