//! Prediction-markets handlers (prefix /prediction-markets).
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use findata::error::{ApiError, ApiResult};
use crate::queries::prediction_markets as q;
use findata::state::AppState;

type Rows = Json<Vec<Map<String, Value>>>;

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(rename = "q")]
    pub q: String,
    pub venue: Option<String>,
    #[serde(default = "all")]
    pub status: String,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn all() -> String { "all".into() }
fn d50() -> i64 { 50 }

pub async fn search_markets(State(st): State<AppState>, Query(p): Query<SearchParams>) -> ApiResult<Rows> {
    Ok(Json(q::search_markets(&st.pool, &p.q, p.venue.as_deref(), &p.status, p.limit).await?))
}

pub async fn get_polymarket_market(State(st): State<AppState>, Path(condition_id): Path<String>) -> ApiResult<Json<Map<String, Value>>> {
    q::get_polymarket_market(&st.pool, &condition_id).await?
        .map(Json).ok_or_else(|| ApiError::NotFound(format!("polymarket market {condition_id} not found")))
}

pub async fn get_kalshi_market(State(st): State<AppState>, Path(ticker): Path<String>) -> ApiResult<Json<Map<String, Value>>> {
    q::get_kalshi_market(&st.pool, &ticker).await?
        .map(Json).ok_or_else(|| ApiError::NotFound(format!("kalshi market {ticker:?} not found")))
}

#[derive(Deserialize)]
pub struct TimeWindow {
    #[serde(rename = "from")]
    pub since: Option<DateTime<Utc>>,
    #[serde(rename = "to")]
    pub until: Option<DateTime<Utc>>,
    #[serde(default = "d500")]
    pub limit: i64,
}
fn d500() -> i64 { 500 }

#[derive(Deserialize)]
pub struct TimeWindow100 {
    #[serde(rename = "from")]
    pub since: Option<DateTime<Utc>>,
    #[serde(rename = "to")]
    pub until: Option<DateTime<Utc>>,
    #[serde(default = "d100")]
    pub limit: i64,
}
fn d100() -> i64 { 100 }

pub async fn polymarket_trades(State(st): State<AppState>, Path(id): Path<String>, Query(p): Query<TimeWindow>) -> ApiResult<Rows> {
    Ok(Json(q::polymarket_trades(&st.pool, &id, p.since, p.until, p.limit).await?))
}
pub async fn kalshi_trades(State(st): State<AppState>, Path(id): Path<String>, Query(p): Query<TimeWindow>) -> ApiResult<Rows> {
    Ok(Json(q::kalshi_trades(&st.pool, &id, p.since, p.until, p.limit).await?))
}
pub async fn polymarket_orderbook(State(st): State<AppState>, Path(id): Path<String>, Query(p): Query<TimeWindow100>) -> ApiResult<Rows> {
    Ok(Json(q::polymarket_orderbook(&st.pool, &id, p.since, p.until, p.limit).await?))
}
pub async fn kalshi_orderbook(State(st): State<AppState>, Path(id): Path<String>, Query(p): Query<TimeWindow100>) -> ApiResult<Rows> {
    Ok(Json(q::kalshi_orderbook(&st.pool, &id, p.since, p.until, p.limit).await?))
}

#[derive(Deserialize)]
pub struct CandleParams {
    #[serde(default = "d1440")]
    pub interval: i64,
    #[serde(default = "d500")]
    pub limit: i64,
}
fn d1440() -> i64 { 1440 }

pub async fn candles(State(st): State<AppState>, Path((venue, market_id)): Path<(String, String)>, Query(p): Query<CandleParams>) -> ApiResult<Rows> {
    Ok(Json(q::candles(&st.pool, &venue, &market_id, p.interval, p.limit).await?))
}

#[derive(Deserialize)]
pub struct VenueLimit {
    #[serde(default = "d500")]
    pub limit: i64,
}
pub async fn open_interest(State(st): State<AppState>, Path((venue, market_id)): Path<(String, String)>, Query(p): Query<VenueLimit>) -> ApiResult<Rows> {
    Ok(Json(q::open_interest(&st.pool, &venue, &market_id, p.limit).await?))
}

#[derive(Deserialize)]
pub struct TopHoldersParams {
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn top_holders(State(st): State<AppState>, Path((venue, market_id)): Path<(String, String)>, Query(p): Query<TopHoldersParams>) -> ApiResult<Rows> {
    Ok(Json(q::top_holders(&st.pool, &venue, &market_id, p.limit).await?))
}

pub async fn wallet_profile(State(st): State<AppState>, Path(addr): Path<String>) -> ApiResult<Json<Map<String, Value>>> {
    q::wallet_profile(&st.pool, &addr).await?
        .map(Json).ok_or_else(|| ApiError::NotFound(format!("no profile for wallet {addr}")))
}

#[derive(Deserialize)]
pub struct PnlParams {
    #[serde(default = "day")]
    pub granularity: String,
    #[serde(default = "d365")]
    pub limit: i64,
}
fn day() -> String { "day".into() }
fn d365() -> i64 { 365 }
pub async fn wallet_pnl(State(st): State<AppState>, Path(addr): Path<String>, Query(p): Query<PnlParams>) -> ApiResult<Rows> {
    Ok(Json(q::wallet_pnl(&st.pool, &addr, &p.granularity, p.limit).await?))
}

#[derive(Deserialize)]
pub struct PosParams {
    #[serde(default = "d200")]
    pub limit: i64,
}
fn d200() -> i64 { 200 }
pub async fn wallet_positions(State(st): State<AppState>, Path(addr): Path<String>, Query(p): Query<PosParams>) -> ApiResult<Rows> {
    Ok(Json(q::wallet_positions(&st.pool, &addr, p.limit).await?))
}
pub async fn wallet_activity(State(st): State<AppState>, Path(addr): Path<String>, Query(p): Query<PosParams>) -> ApiResult<Rows> {
    Ok(Json(q::wallet_activity(&st.pool, &addr, p.limit).await?))
}

fn window_alias(w: &str) -> Option<&'static str> {
    match w.to_lowercase().as_str() {
        "all_time" | "all" | "alltime" => Some("all_time"),
        "30d" | "thirty_day" | "month" => Some("thirty_day"),
        "7d" | "seven_day" | "week" => Some("seven_day"),
        "24h" | "1d" | "one_day" | "day" => Some("one_day"),
        _ => None,
    }
}

#[derive(Deserialize)]
pub struct LeaderboardParams {
    #[serde(default = "all_time")]
    pub window: String,
    #[serde(default = "polymarket")]
    pub venue: String,
    #[serde(default = "d50")]
    pub limit: i64,
}
fn all_time() -> String { "all_time".into() }
fn polymarket() -> String { "polymarket".into() }

pub async fn leaderboard(State(st): State<AppState>, Query(p): Query<LeaderboardParams>) -> ApiResult<Rows> {
    let canon = window_alias(&p.window)
        .ok_or_else(|| ApiError::Validation(serde_json::json!(format!("unknown window {:?}", p.window))))?;
    let v = p.venue.to_lowercase();
    if v != "polymarket" && v != "kalshi" {
        return Err(ApiError::Validation(serde_json::json!(format!("unknown venue {:?}", p.venue))));
    }
    Ok(Json(q::leaderboard(&st.pool, p.limit, canon, &v).await?))
}

#[derive(Deserialize)]
pub struct MatchedParams {
    #[serde(default = "d20")]
    pub limit: i64,
}
fn d20() -> i64 { 20 }
pub async fn matched_pairs(State(st): State<AppState>, Path((venue, venue_id)): Path<(String, String)>, Query(p): Query<MatchedParams>) -> ApiResult<Rows> {
    Ok(Json(q::matched_pairs(&st.pool, &venue, &venue_id, p.limit).await?))
}

#[derive(Deserialize)]
pub struct EventsParams {
    #[serde(rename = "q")]
    pub q: Option<String>,
    #[serde(default = "all")]
    pub status: String,
    #[serde(default = "d50")]
    pub limit: i64,
}
pub async fn polymarket_events(State(st): State<AppState>, Query(p): Query<EventsParams>) -> ApiResult<Rows> {
    Ok(Json(q::polymarket_events(&st.pool, p.q.as_deref(), &p.status, p.limit).await?))
}
