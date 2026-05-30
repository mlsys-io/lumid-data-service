//! Technical-indicator handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries::technical as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct TechParams {
    pub indicator: Option<String>,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    #[serde(default = "d500")]
    pub limit: i64,
}
fn d500() -> i64 { 500 }

pub async fn technical(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<TechParams>,
) -> ApiResult<Json<Value>> {
    let end = p.end.unwrap_or_else(Utc::now);
    let start = p.start.unwrap_or_else(|| end - Duration::days(365));
    let rows = q::indicators(&st.pool, &symbol, p.indicator.as_deref(), start, end, p.limit).await?;
    let data = strip_lineage_rows(rows);
    Ok(Json(json!({
        "symbol": symbol.to_uppercase(),
        "indicator": p.indicator.unwrap_or_else(|| "all".into()),
        "count": data.len(),
        "data": data,
    })))
}

pub async fn technical_latest(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::latest(&st.pool, &symbol).await?);
    Ok(Json(json!({"symbol": symbol.to_uppercase(), "count": data.len(), "data": data})))
}
