//! Fundamentals handlers — port of `api/routes/fundamentals.py`.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::{strip_lineage, strip_lineage_rows};
use crate::error::{ApiError, ApiResult};
use crate::queries;
use crate::state::AppState;

pub async fn latest(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = queries::fundamentals::latest(&st.pool, &symbol)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no fundamentals snapshot for {symbol:?}")))?;
    Ok(Json(strip_lineage(row)))
}

#[derive(Deserialize)]
pub struct HistoryParams {
    #[serde(default = "default_statement")]
    pub statement: String,
    #[serde(default = "default_period")]
    pub period: String,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_statement() -> String {
    "income".to_string()
}
fn default_period() -> String {
    "quarter".to_string()
}
fn default_limit() -> i64 {
    40
}

pub async fn history(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<HistoryParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    let rows =
        queries::fundamentals::history(&st.pool, &symbol, &p.statement, &p.period, p.limit).await?;
    Ok(Json(strip_lineage_rows(rows)))
}
