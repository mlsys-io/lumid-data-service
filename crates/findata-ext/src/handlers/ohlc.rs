//! OHLC handler — port of `api/routes/ohlc.py`.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use findata::db::lineage::strip_lineage_rows;
use findata::error::{ApiError, ApiResult};
use crate::queries;
use findata::state::AppState;

#[derive(Deserialize)]
pub struct OhlcParams {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    #[serde(default = "default_interval")]
    pub interval: String,
}

fn default_interval() -> String {
    "1min".to_string()
}

/// Default lookback window per interval (mirrors `_DEFAULT_WINDOW`).
fn default_window(interval: &str) -> Duration {
    match interval {
        "1min" => Duration::days(1),
        "5min" => Duration::days(7),
        "15min" => Duration::days(30),
        "30min" => Duration::days(60),
        "1hour" => Duration::days(120),
        "4hour" => Duration::days(365),
        "1d" => Duration::days(365),
        _ => Duration::days(1),
    }
}

pub async fn ohlc(
    State(st): State<AppState>,
    Path(symbol): Path<String>,
    Query(p): Query<OhlcParams>,
) -> ApiResult<Json<Value>> {
    let end = p.end.unwrap_or_else(Utc::now);
    let start = p.start.unwrap_or_else(|| end - default_window(&p.interval));
    if end <= start {
        return Err(ApiError::BadRequest("end must be > start".into()));
    }
    let rows = queries::ohlc::query(
        &st.pool,
        &symbol,
        start,
        end,
        &p.interval,
        st.settings.ohlc_row_cap,
    )
    .await?;
    let count = rows.len();
    let bars = strip_lineage_rows(rows);
    Ok(Json(json!({
        "symbol": symbol.to_uppercase(),
        "interval": p.interval,
        "count": count,
        "bars": bars,
    })))
}
