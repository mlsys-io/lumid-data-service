//! Earnings-call transcript handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::{strip_lineage, strip_lineage_rows};
use crate::error::{ApiError, ApiResult};
use crate::queries::transcripts as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct ListParams {
    pub year: Option<i32>,
    pub quarter: Option<i32>,
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }

pub async fn list_transcripts(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<ListParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    let rows = q::list_for_symbol(&st.pool, &symbol, p.year, p.quarter, p.limit, false).await?;
    Ok(Json(strip_lineage_rows(rows)))
}

pub async fn one_transcript(
    State(st): State<AppState>,
    Path((symbol, year, quarter)): Path<(String, i32, i32)>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = q::one_full(&st.pool, &symbol, year, quarter)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no transcript for {symbol:?} {year}Q{quarter}")))?;
    Ok(Json(strip_lineage(row)))
}
