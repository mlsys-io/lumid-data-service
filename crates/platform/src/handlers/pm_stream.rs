//! Live prediction-market event SSE — port of
//! `api/routes/prediction_markets_stream.py`.
//!
//! `GET /prediction-markets/stream` subscribes to the Redis pub/sub channel
//! `pm:events` (published by the Polymarket CLOB WS recorder) and streams each
//! event as SSE `event: tick`, with `event: heartbeat` every 30 s. Optional
//! `asset_ids` / `condition_ids` (comma-separated) filters. Independent of the
//! quote hub — it opens its own pub/sub connection per request.

use std::collections::HashSet;
use std::convert::Infallible;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use futures_util::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

const REDIS_CH: &str = "pm:events";

#[derive(Deserialize)]
pub struct PmStreamParams {
    asset_ids: Option<String>,
    condition_ids: Option<String>,
}

fn parse_set(s: &Option<String>) -> Option<HashSet<String>> {
    let set: HashSet<String> = s
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

struct PmState {
    stream: std::pin::Pin<Box<dyn Stream<Item = redis::Msg> + Send>>,
    aids: Option<HashSet<String>>,
    cids: Option<HashSet<String>>,
    last_hb: Instant,
    buf: std::collections::VecDeque<Event>,
}

pub async fn stream(
    State(st): State<AppState>,
    Query(p): Query<PmStreamParams>,
) -> ApiResult<impl IntoResponse> {
    let client = st
        .redis_client
        .clone()
        .ok_or_else(|| ApiError::Unavailable("redis_unavailable".into()))?;
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .map_err(|e| ApiError::Unavailable(format!("redis pubsub: {e}")))?;
    pubsub
        .subscribe(REDIS_CH)
        .await
        .map_err(|e| ApiError::Unavailable(format!("redis subscribe: {e}")))?;

    let aids = parse_set(&p.asset_ids);
    let cids = parse_set(&p.condition_ids);

    let open = Event::default()
        .event("open")
        .json_data(json!({
            "channel": REDIS_CH,
            "asset_filter": aids.as_ref().map(|s| s.iter().cloned().collect::<Vec<_>>()).unwrap_or_default(),
            "condition_filter": cids.as_ref().map(|s| s.iter().cloned().collect::<Vec<_>>()).unwrap_or_default(),
        }))
        .unwrap_or_else(|_| Event::default().event("open").data("{}"));

    let state = PmState {
        stream: Box::pin(pubsub.into_on_message()),
        aids,
        cids,
        last_hb: Instant::now(),
        buf: std::collections::VecDeque::new(),
    };

    let body = stream::unfold(state, |mut s| async move {
        loop {
            // Drain any buffered events first.
            if let Some(ev) = s.buf.pop_front() {
                return Some((Ok::<Event, Infallible>(ev), s));
            }
            let tick = tokio::time::sleep(Duration::from_secs(1));
            tokio::select! {
                maybe = s.stream.next() => {
                    match maybe {
                        None => return None,
                        Some(msg) => {
                            if let Ok(raw) = msg.get_payload::<String>() {
                                if let Ok(d) = serde_json::from_str::<serde_json::Value>(&raw) {
                                    let pass_a = s.aids.as_ref().map_or(true, |f|
                                        d.get("asset_id").and_then(|v| v.as_str()).map(|x| f.contains(x)).unwrap_or(false));
                                    let pass_c = s.cids.as_ref().map_or(true, |f|
                                        d.get("condition_id").and_then(|v| v.as_str()).map(|x| f.contains(x)).unwrap_or(false));
                                    if pass_a && pass_c {
                                        let ev = Event::default().event("tick").json_data(d)
                                            .unwrap_or_else(|_| Event::default().event("tick").data("{}"));
                                        s.buf.push_back(ev);
                                    }
                                }
                            }
                        }
                    }
                }
                _ = tick => {}
            }
            // Evaluate the heartbeat every iteration (so continuous message flow
            // can't starve it), matching the Python loop.
            if s.last_hb.elapsed() > Duration::from_secs(30) {
                s.last_hb = Instant::now();
                let ev = Event::default().event("heartbeat")
                    .json_data(json!({"ts": chrono::Utc::now().timestamp()}))
                    .unwrap_or_else(|_| Event::default().event("heartbeat").data("{}"));
                s.buf.push_back(ev);
            }
        }
    });

    let full = stream::once(async move { Ok::<Event, Infallible>(open) }).chain(body);
    Ok(Sse::new(full))
}
