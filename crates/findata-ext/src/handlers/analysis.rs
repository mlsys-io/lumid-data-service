//! Analysis handlers — ratios, key metrics, growth (4 variants).

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use findata::db::lineage::strip_lineage_rows;
use findata::error::ApiResult;
use crate::queries::analysis as q;
use findata::state::AppState;

#[derive(Deserialize)]
pub struct PeriodLimit {
    #[serde(default = "quarter")]
    pub period: String,
    #[serde(default = "d20")]
    pub limit: i64,
}
fn quarter() -> String {
    "quarter".into()
}
fn d20() -> i64 {
    20
}

pub async fn ratios(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::ratios(&st.pool, &symbol, &p.period, p.limit).await?,
    )))
}

pub async fn key_metrics(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::key_metrics(&st.pool, &symbol, &p.period, p.limit).await?,
    )))
}

async fn growth_route(
    st: &AppState,
    table: &str,
    symbol: &str,
    p: &PeriodLimit,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        q::growth(&st.pool, table, symbol, &p.period, p.limit).await?,
    )))
}

pub async fn financial_growth(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    growth_route(&st, "fundamentals.financial_growth", &symbol, &p).await
}

pub async fn income_statement_growth(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    growth_route(&st, "fundamentals.income_statement_growth", &symbol, &p).await
}

pub async fn balance_sheet_growth(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    growth_route(&st, "fundamentals.balance_sheet_growth", &symbol, &p).await
}

pub async fn cash_flow_growth(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<PeriodLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    growth_route(&st, "fundamentals.cash_flow_growth", &symbol, &p).await
}
