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

#[derive(Deserialize)]
pub struct LatestParams {
    pub category: Option<String>,
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 {
    50
}

pub async fn latest(
    State(st): State<AppState>,
    Query(p): Query<LatestParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        queries::news::latest(&st.pool, p.category.as_deref(), p.since, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(rename = "q")]
    pub query: String,
    pub category: Option<String>,
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "d50")]
    pub limit: i64,
}

pub async fn search(
    State(st): State<AppState>,
    Query(p): Query<SearchParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        queries::news::search(&st.pool, &p.query, p.category.as_deref(), p.since, p.limit).await?,
    )))
}

pub async fn stats(State(st): State<AppState>) -> ApiResult<Json<Value>> {
    Ok(Json(queries::news::stats(&st.pool).await?))
}

#[derive(Deserialize)]
pub struct SocialParams {
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 {
    200
}
pub async fn social_sentiment(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SocialParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        queries::news::social_sentiment(&st.pool, &symbol, p.since, p.limit).await?,
    )))
}

#[derive(Deserialize)]
pub struct SymSentParams {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 {
    20
}
pub async fn symbol_sentiment(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<SymSentParams>,
) -> ApiResult<Json<Vec<Map<String, Value>>>> {
    Ok(Json(strip_lineage_rows(
        queries::news::symbol_sentiment(&st.pool, &symbol, p.limit).await?,
    )))
}
