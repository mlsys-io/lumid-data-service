//! Health probes. `/health` is public (no auth); `/health/db` checks the pool.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "lumid-data-service"}))
}

pub async fn health_db(State(st): State<AppState>) -> ApiResult<Json<Value>> {
    let client = st.pool.get().await?;
    let row = client.query_one("SELECT 1 AS ok", &[]).await?;
    let ok: i32 = row.get("ok");
    Ok(Json(json!({"db": ok == 1})))
}
