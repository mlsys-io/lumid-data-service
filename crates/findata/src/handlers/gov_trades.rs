//! Government trades handler — congressional / executive-branch trades.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::ApiResult;
use crate::queries::gov_trades as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SinceLimit {
    pub since: Option<NaiveDate>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 {
    50
}

pub async fn for_symbol(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SinceLimit>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(q::for_symbol(&st.pool, &symbol, p.since, p.limit).await?))
}
