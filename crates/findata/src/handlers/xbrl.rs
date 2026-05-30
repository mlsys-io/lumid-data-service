//! XBRL handlers.
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::db::lineage::{strip_lineage, strip_lineage_rows};
use crate::error::{ApiError, ApiResult};
use crate::queries::xbrl as q;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct Lim100 {
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }

pub async fn xbrl_index(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<Lim100>,
) -> ApiResult<Json<Value>> {
    let data = strip_lineage_rows(q::index(&st.pool, &symbol, p.limit).await?);
    Ok(Json(json!({"symbol": symbol.to_uppercase(), "count": data.len(), "data": data})))
}

pub async fn xbrl_filing(
    State(st): State<AppState>,
    Path((symbol, accession)): Path<(String, String)>,
) -> ApiResult<Json<Map<String, Value>>> {
    let row = q::filing(&st.pool, &symbol, &accession)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no XBRL filing {accession:?} for {symbol:?}")))?;
    Ok(Json(strip_lineage(row)))
}
