//! WebSocket realtime endpoints — port of `api/routes/ws_quotes.py` +
//! `ws_news.py`.
//!
//! `/ws/quotes` streams tick/news/kol frames; `/ws/news` is the same protocol
//! but the sender drops everything except news + control frames. Both
//! authenticate themselves (token from Authorization / x-api-key /
//! `Sec-WebSocket-Protocol: bearer.<tok>`) — they are mounted OUTSIDE the
//! `gate` middleware because the WS upgrade can't carry the gate's 401 body.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};

use crate::realtime::hub::{gen_conn_id, now_iso, ConnKind, Connection, Hub};
use crate::state::AppState;

fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some((scheme, tok)) = auth.split_once(' ') {
            if scheme.eq_ignore_ascii_case("bearer") && !tok.trim().is_empty() {
                return Some(tok.trim().to_string());
            }
        }
    }
    if let Some(k) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        if !k.trim().is_empty() {
            return Some(k.trim().to_string());
        }
    }
    if let Some(proto) = headers.get("sec-websocket-protocol").and_then(|v| v.to_str().ok()) {
        if let Some(tok) = proto.strip_prefix("bearer.") {
            if !tok.trim().is_empty() {
                return Some(tok.trim().to_string());
            }
        }
    }
    None
}


pub async fn quotes(
    State(st): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    upgrade(st, headers, ws, false).await
}

pub async fn news(
    State(st): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    upgrade(st, headers, ws, true).await
}

async fn upgrade(
    st: AppState,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    news_only: bool,
) -> Response {
    let hub = match &st.hub {
        Some(h) => h.clone(),
        None => {
            return (StatusCode::SERVICE_UNAVAILABLE, "realtime unavailable").into_response()
        }
    };
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => return (StatusCode::UNAUTHORIZED, "bearer token required").into_response(),
    };
    let ident = match crate::auth::resolve_bearer(&st, &token).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (StatusCode::UNAUTHORIZED, "invalid or unknown token").into_response()
        }
        Err(_) => {
            return (StatusCode::SERVICE_UNAVAILABLE, "auth service unreachable").into_response()
        }
    };
    let lifetime_cap = st.settings.rt_ws_lifetime_syms;
    let hb_secs = st.settings.rt_heartbeat_sec;
    let queue_cap = st.settings.rt_slowclient_queue;
    ws.on_upgrade(move |socket| async move {
        let enforce_cl = !ident.sub.starts_with("internal:");
        let conn = Connection::new(gen_conn_id(), ident.sub, ConnKind::Ws, queue_cap);
        serve(socket, hub, conn, lifetime_cap, hb_secs, news_only, enforce_cl).await;
    })
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    socket: WebSocket,
    hub: Arc<Hub>,
    conn: Arc<Connection>,
    lifetime_cap: u64,
    hb_secs: u64,
    news_only: bool,
    enforce_cl: bool,
) {
    hub.register(conn.clone(), enforce_cl).await;
    tracing::info!("ws opened sub={} id={} news_only={}", conn.sub, conn.id, news_only);

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Sender task: drains the connection queue, woken by notify.
    let send_conn = conn.clone();
    let sender = tokio::spawn(async move {
        loop {
            for frame in send_conn.drain().await {
                if news_only {
                    let t = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if t == "tick" || t == "kol" || t == "tier_change" {
                        continue;
                    }
                }
                if ws_tx.send(Message::Text(frame.to_string())).await.is_err() {
                    return;
                }
            }
            send_conn.notify.notified().await;
        }
    });

    // Heartbeat task.
    let hb_conn = conn.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(hb_secs)).await;
            hb_conn.push(json!({"type": "heartbeat", "ts": now_iso()})).await;
        }
    });

    // Receiver loop.
    while let Some(Ok(msg)) = ws_rx.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let parsed: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => {
                conn.push(json!({"type": "error", "code": "bad_json",
                                 "message": "frames must be valid JSON"}))
                    .await;
                continue;
            }
        };
        match parsed.get("type").and_then(|v| v.as_str()) {
            Some("subscribe") => {
                let syms = upper_symbols(&parsed);
                let have = conn.lifetime_symbols.load(Ordering::Relaxed);
                if have + syms.len() as u64 > lifetime_cap {
                    conn.push(json!({"type": "error", "code": "too_many_symbols",
                                     "message": format!("lifetime cap is {lifetime_cap}")}))
                        .await;
                    continue;
                }
                let tiers = hub.subscribe(&conn, &syms).await;
                // Preserve request order (HashMap key order is nondeterministic).
                let syms_out: Vec<&String> = syms.iter().filter(|s| tiers.contains_key(*s)).collect();
                if news_only {
                    conn.push(json!({"type": "subscribed", "symbols": syms_out})).await;
                } else {
                    conn.push(json!({"type": "subscribed", "symbols": syms_out, "tier": tiers}))
                        .await;
                }
            }
            Some("unsubscribe") => {
                let syms = upper_symbols(&parsed);
                let removed = hub.unsubscribe(&conn, &syms).await;
                conn.push(json!({"type": "unsubscribed", "symbols": removed})).await;
            }
            Some("ping") => {
                conn.push(json!({"type": "pong"})).await;
            }
            other => {
                conn.push(json!({"type": "error", "code": "bad_type",
                                 "message": format!("unknown type {other:?}")}))
                    .await;
            }
        }
    }

    heartbeat.abort();
    sender.abort();
    hub.unregister(&conn).await;
    tracing::info!("ws closed sub={} id={}", conn.sub, conn.id);
}

fn upper_symbols(msg: &Value) -> Vec<String> {
    msg.get("symbols")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_uppercase())
                .collect()
        })
        .unwrap_or_default()
}
