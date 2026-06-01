//! WebSocket realtime endpoints — port of `api/routes/ws_quotes.py` +
//! `ws_news.py`.
//!
//! `/ws/quotes` streams all data-frame kinds; `/ws/news` is the same protocol
//! but the sender drops everything except `news` data frames + control frames.
//! (The set of data-frame kinds is app-configured — see `rt_channel_kinds`.) Both
//! authenticate themselves (token from Authorization / x-api-key /
//! `Sec-WebSocket-Protocol: bearer.<tok>`) — they are mounted OUTSIDE the
//! `gate` middleware because the WS upgrade can't carry the gate's 401 body.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
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

#[derive(serde::Deserialize)]
pub struct PmWsParams {
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
    if set.is_empty() { None } else { Some(set) }
}

/// `/ws/prediction-markets` — WebSocket alternative to the SSE
/// `/prediction-markets/stream`. Same source (Redis `pm:events`, fed by the
/// CLOB recorder) and same optional `asset_ids`/`condition_ids` filters; pushes
/// each event as a WS text frame with a periodic ping. Self-authenticating
/// (token via Authorization / x-api-key / `Sec-WebSocket-Protocol: bearer.<tok>`).
pub async fn prediction_markets(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<PmWsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => return (StatusCode::UNAUTHORIZED, "bearer token required").into_response(),
    };
    match crate::auth::resolve_bearer(&st, &token).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::UNAUTHORIZED, "invalid or unknown token").into_response(),
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, "auth service unreachable").into_response(),
    };
    let client = match st.redis_client.clone() {
        Some(c) => c,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "realtime unavailable").into_response(),
    };
    let aids = parse_set(&p.asset_ids);
    let cids = parse_set(&p.condition_ids);
    let hb = st.settings.rt_heartbeat_sec.max(5);
    ws.on_upgrade(move |socket| pm_serve(socket, client, aids, cids, hb))
}

async fn pm_serve(
    socket: WebSocket,
    client: redis::Client,
    aids: Option<HashSet<String>>,
    cids: Option<HashSet<String>>,
    hb_secs: u64,
) {
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(p) => p,
        Err(_) => return,
    };
    if pubsub.subscribe("pm:events").await.is_err() {
        return;
    }
    let (mut tx, mut rx) = socket.split();
    let _ = tx
        .send(Message::Text(json!({"type": "open", "channel": "pm:events"}).to_string()))
        .await;
    let mut msgs = pubsub.into_on_message();
    let mut hb = tokio::time::interval(Duration::from_secs(hb_secs));
    hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            m = msgs.next() => match m {
                None => break,
                Some(msg) => {
                    if let Ok(raw) = msg.get_payload::<String>() {
                        if let Ok(d) = serde_json::from_str::<Value>(&raw) {
                            let pass_a = aids.as_ref().map_or(true, |f|
                                d.get("asset_id").and_then(|v| v.as_str()).map(|x| f.contains(x)).unwrap_or(false));
                            let pass_c = cids.as_ref().map_or(true, |f|
                                d.get("condition_id").and_then(|v| v.as_str()).map(|x| f.contains(x)).unwrap_or(false));
                            if pass_a && pass_c && tx.send(Message::Text(raw)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            },
            _ = hb.tick() => {
                if tx.send(Message::Ping(Vec::new())).await.is_err() { break; }
            }
            cm = rx.next() => match cm {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            }
        }
    }
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
                    // The /ws/news variant carries only `news` data frames (+
                    // heartbeat/control). Drop any other data frame (domain-agnostic:
                    // a data frame carries a `data` payload) and tier-change notices.
                    let t = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    let is_data = frame.get("data").is_some();
                    if (is_data && t != "news") || t == "tier_change" {
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
