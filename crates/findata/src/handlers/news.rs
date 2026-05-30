//! News handler — port of `api/routes/news.py:for_symbol`.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::db::lineage::strip_lineage_rows;
use crate::error::ApiResult;
use crate::queries;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct NewsParams {
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    50
}

pub async fn for_symbol(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<NewsParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    let rows = queries::news::for_symbol(&st.pool, &symbol, p.since, p.limit).await?;
    Ok(Json(strip_lineage_rows(rows)))
}
