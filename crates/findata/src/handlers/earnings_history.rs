//! Earnings history + quality handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::earnings_history as q;
use crate::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct HistParams {
    #[serde(default = "d40")]
    pub limit: i64,
}
fn d40() -> i64 { 40 }
pub async fn history(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<HistParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::history(&st.pool, &symbol, p.limit).await?)))
}

#[derive(Deserialize)]
pub struct QualParams {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }
pub async fn quality(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<QualParams>,
) -> ApiResult<Rows> {
    Ok(Json(strip_lineage_rows(q::quality(&st.pool, &symbol, p.limit).await?)))
}
