//! Freshness handler — port of `api/routes/freshness.py` (JSON view only; the
//! HTML status board lands with the landing/metrics phase).

use axum::extract::State;
use axum::Json;
use serde_json::{Map, Value};

use crate::error::ApiResult;
use crate::queries;
use crate::state::AppState;

pub async fn freshness(State(st): State<AppState>) -> ApiResult<Json<Map<String, Value>>> {
    Ok(Json(queries::freshness::counts(&st.pool).await?))
}
