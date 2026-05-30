//! Symbol handlers — port of `api/routes/symbols.py` (search shown; the rest
//! land in Phase 1/2). Thin: parse params → query → lineage-strip → JSON.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::{ApiError, ApiResult};
use crate::queries;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    20
}

pub async fn search(
    State(st): State<AppState>,
    Query(p): Query<SearchParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    if p.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q is required".into()));
    }
    let rows = queries::symbols::search(&st.pool, &p.q, p.limit).await?;
    Ok(Json(strip_lineage_rows(rows)))
}
