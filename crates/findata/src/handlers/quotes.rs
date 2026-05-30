//! Quote snapshot, quote-stats, metrics-snapshot handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::queries::quotes as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct QuotesParams {
    pub symbols: String,
}

pub async fn quotes_snapshot(
    State(st): State<AppState>,
    Query(p): Query<QuotesParams>,
) -> ApiResult<Json<Vec<Value>>> {
    let syms: Vec<String> = p
        .symbols
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if syms.is_empty() {
        return Err(ApiError::BadRequest("symbols= must list at least one ticker".into()));
    }
    if syms.len() > st.settings.quotes_max_symbols {
        return Err(ApiError::BadRequest(format!(
            "max {} symbols per request",
            st.settings.quotes_max_symbols
        )));
    }
    Ok(Json(q::snapshot(&st, &syms).await))
}

pub async fn quote_stats(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Value>> {
    Ok(Json(q::stats(&st.pool, &symbol).await?))
}

pub async fn metrics_snapshot(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Value>> {
    let metrics = q::metrics_snapshot(&st.pool, &symbol).await?;
    Ok(Json(json!({"symbol": symbol.to_uppercase(), "metrics": metrics})))
}
