//! KOL handlers — roster, Redis recall, durable archive.
//! (The /kols/tweets/stream SSE endpoint is served by the realtime sidecar.)
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use findata::error::ApiResult;
use crate::queries::kols as q;
use findata::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub include_inactive: bool,
}
pub async fn list_kols(State(st): State<AppState>, Query(p): Query<ListParams>) -> ApiResult<Rows> {
    Ok(Json(q::list_active(&st.pool, p.include_inactive).await?))
}

#[derive(Deserialize)]
pub struct RecallParams {
    #[serde(default = "d50")]
    pub limit: i64,
}
fn d50() -> i64 { 50 }

pub async fn recent_tweets(State(st): State<AppState>, Query(p): Query<RecallParams>) -> ApiResult<Json<Vec<Value>>> {
    let roster = q::list_active(&st.pool, false).await?;
    let handles: Vec<String> = roster
        .iter()
        .filter_map(|r| r.get("handle").and_then(|v| v.as_str()).map(|s| s.to_lowercase()))
        .collect();
    Ok(Json(q::project_recall(q::tweets_recent(&st, &handles, p.limit).await)))
}

pub async fn tweets_for_handle(State(st): State<AppState>, Path(handle): Path<String>, Query(p): Query<RecallParams>) -> ApiResult<Json<Vec<Value>>> {
    Ok(Json(q::project_recall(q::tweets_by_handle(&st, &handle, p.limit).await)))
}

pub async fn tweets_for_symbol(State(st): State<AppState>, Path(symbol): Path<String>, Query(p): Query<RecallParams>) -> ApiResult<Json<Vec<Value>>> {
    Ok(Json(q::project_recall(q::tweets_by_symbol(&st, &symbol, p.limit).await)))
}

#[derive(Deserialize)]
pub struct HandleHistoryParams {
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub cashtag: Option<String>,
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn history_for_handle(State(st): State<AppState>, Path(handle): Path<String>, Query(p): Query<HandleHistoryParams>) -> ApiResult<Rows> {
    let rows = q::history_by_handle(&st.pool, &handle, p.since, p.until, p.cashtag.as_deref(), p.limit).await?;
    Ok(Json(q::project_archive(q::attach_proxy_urls(rows))))
}

#[derive(Deserialize)]
pub struct SymbolHistoryParams {
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub handle: Option<String>,
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn history_for_symbol(State(st): State<AppState>, Path(symbol): Path<String>, Query(p): Query<SymbolHistoryParams>) -> ApiResult<Rows> {
    let rows = q::history_by_symbol(&st.pool, &symbol, p.since, p.until, p.handle.as_deref(), p.limit).await?;
    Ok(Json(q::project_archive(q::attach_proxy_urls(rows))))
}

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(rename = "q")]
    pub q: String,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn search_archive(State(st): State<AppState>, Query(p): Query<SearchParams>) -> ApiResult<Rows> {
    let rows = q::search(&st.pool, &p.q, p.since, p.until, p.limit).await?;
    Ok(Json(q::project_archive(q::attach_proxy_urls(rows))))
}

pub async fn archive_stats(State(st): State<AppState>) -> ApiResult<Json<Map<String, Value>>> {
    Ok(Json(q::archive_stats(&st.pool).await?))
}
