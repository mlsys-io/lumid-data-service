//! Symbol handlers — port of `api/routes/symbols.py` (search shown; the rest
//! land in Phase 1/2). Thin: parse params → query → lineage-strip → JSON.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};

use findata::db::lineage::{strip_lineage, strip_lineage_rows};
use findata::error::{ApiError, ApiResult};
use crate::queries;
use findata::state::AppState;

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

pub async fn get_one(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = queries::symbols::get_one(&st.pool, &symbol)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("symbol {symbol:?} not found")))?;
    Ok(Json(strip_lineage(row)))
}

pub async fn universe(State(st): State<AppState>) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(queries::symbols::universe(&st.pool).await?))
}
