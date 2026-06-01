//! SSE quote stream — port of `api/routes/sse_quotes.py`.
//!
//! `GET /quotes/stream?symbols=AAPL,NVDA` — Server-Sent Events data-frame
//! stream. Gated (the `gate` middleware injects the identity); subscribes the
//! connection to the requested symbols and emits one SSE event per frame, with
//! `event:` ∈ the app-configured data-frame kinds (`rt_channel_kinds`) plus the
//! control events {subscribed, heartbeat, error}.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Extension, Query, State};
use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use futures_util::stream::{self};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::realtime::hub::{gen_conn_id, now_iso, ConnKind, Connection, Hub};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct StreamParams {
    #[serde(default)]
    symbols: String,
}

/// Aborts the heartbeat task and unregisters the connection when the SSE
/// response stream is dropped (client disconnect).
struct CleanupGuard {
    hub: Arc<Hub>,
    conn: Arc<Connection>,
    heartbeat: tokio::task::JoinHandle<()>,
}
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        let hub = self.hub.clone();
        let conn = self.conn.clone();
        tokio::spawn(async move {
            hub.unregister(&conn).await;
            tracing::info!(
                "sse closed sub={} id={} dropped={}",
                conn.sub,
                conn.id,
                conn.dropped.load(std::sync::atomic::Ordering::Relaxed)
            );
        });
    }
}

struct StreamState {
    conn: Arc<Connection>,
    buf: VecDeque<Event>,
    _guard: CleanupGuard,
}

pub async fn quotes_stream(
    State(st): State<AppState>,
    Extension(ident): Extension<Identity>,
    Query(p): Query<StreamParams>,
) -> ApiResult<impl IntoResponse> {
    let hub = st
        .hub
        .clone()
        .ok_or_else(|| ApiError::Unavailable("realtime unavailable".into()))?;

    let sym_list: Vec<String> = p
        .symbols
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if sym_list.is_empty() {
        return Err(ApiError::BadRequest(
            "?symbols= must list at least one ticker".into(),
        ));
    }
    if sym_list.len() > st.settings.rt_sse_request_syms {
        return Err(ApiError::BadRequest(format!(
            "max {} symbols per request",
            st.settings.rt_sse_request_syms
        )));
    }

    let conn = Connection::new(
        gen_conn_id(),
        ident.sub.clone(),
        ConnKind::Sse,
        st.settings.rt_slowclient_queue,
    );
    let enforce_cl = !ident.sub.starts_with("internal:");
    hub.register(conn.clone(), enforce_cl).await;
    let tiers = hub.subscribe(&conn, &sym_list).await;
    tracing::info!("sse opened sub={} id={} symbols={}", ident.sub, conn.id, sym_list.len());

    // Initial `subscribed` frame — preserve request order (HashMap key order
    // is nondeterministic).
    let syms_out: Vec<&String> = sym_list.iter().filter(|s| tiers.contains_key(*s)).collect();
    let initial = Event::default()
        .event("subscribed")
        .json_data(json!({"symbols": syms_out, "tier": tiers}))
        .unwrap_or_else(|_| Event::default().event("subscribed").data("{}"));

    // Heartbeat pushes through the same connection queue.
    let hb_conn = conn.clone();
    let hb_secs = st.settings.rt_heartbeat_sec;
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(hb_secs)).await;
            hb_conn.push(json!({"type": "heartbeat", "ts": now_iso()})).await;
        }
    });

    let mut buf = VecDeque::new();
    buf.push_back(initial);
    let state = StreamState {
        conn: conn.clone(),
        buf,
        _guard: CleanupGuard { hub, conn, heartbeat },
    };

    let body = stream::unfold(state, |mut s| async move {
        loop {
            if let Some(ev) = s.buf.pop_front() {
                return Some((Ok::<Event, Infallible>(ev), s));
            }
            let frames = s.conn.drain().await;
            if frames.is_empty() {
                s.conn.notify.notified().await;
                continue;
            }
            for f in frames {
                s.buf.push_back(frame_to_event(f));
            }
        }
    });

    Ok(Sse::new(body))
}

fn frame_to_event(frame: Value) -> Event {
    let kind = frame.get("type").and_then(|v| v.as_str()).unwrap_or("message").to_string();
    match kind.as_str() {
        "heartbeat" => {
            let ts = frame.get("ts").cloned().unwrap_or_else(|| json!(now_iso()));
            Event::default()
                .event("heartbeat")
                .json_data(json!({"ts": ts}))
                .unwrap_or_else(|_| Event::default().event("heartbeat").data("{}"))
        }
        // Data frames carry a `data` payload (emit just that — any channel kind,
        // domain-agnostic); control frames (subscribed / error / tier_change)
        // carry their fields inline, so emit the whole frame.
        _ if frame.get("data").is_some() => {
            let k = kind.clone();
            Event::default()
                .event(kind)
                .json_data(frame.get("data").cloned().unwrap_or(json!({})))
                .unwrap_or_else(|_| Event::default().event(k).data("{}"))
        }
        _ => Event::default()
            .event(kind)
            .json_data(frame)
            .unwrap_or_else(|_| Event::default().data("{}")),
    }
}
