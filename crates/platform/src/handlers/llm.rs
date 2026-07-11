//! LLM reverse proxy — OpenAI + Anthropic compatible.
//!
//! Two composed planes select the upstream for every `/v1/*` request:
//!
//!  1. **Federation (F1 mesh core) — the OUTER switch.** When
//!     `LUMID_LLM_FEDERATE` names a configured peer, ALL `/v1/*` traffic is
//!     forwarded to that peer's base URL (the peer serves LLM from ITS own
//!     backends), authenticated with the peer bearer + `X-Lumid-Origin-*`
//!     attribution headers. This precedes and wraps the local plane.
//!
//!  2. **Local backend pool (LLM backend pool) — the local-selection path.**
//!     When `LUMID_LLM_FEDERATE` is NOT set, requests route through the
//!     health-aware, least-loaded `BackendPool`. Non-streaming endpoints retry
//!     across backends on connect failure or HTTP 503; streaming retries on
//!     connect failure only (once the first byte is in flight we can't replay
//!     the SSE stream). An unknown explicit model falls through to the
//!     OpenRouter catch-all when configured.
//!
//! | path                            | shape       | streaming |
//! |---------------------------------|-------------|-----------|
//! | GET  /v1/models                 | OpenAI      | —         |
//! | POST /v1/chat/completions       | OpenAI      | SSE       |
//! | POST /v1/completions            | OpenAI      | SSE       |
//! | POST /v1/embeddings             | OpenAI      | —         |
//! | POST /v1/messages               | Anthropic   | SSE       |
//! | POST /v1/messages/count_tokens  | Anthropic   | —         |
//!
//! When `LUMID_LLM_API_KEY` is set the platform injects `Authorization: Bearer
//! <key>` on all outbound LOCAL upstream calls, enabling use of hosted endpoints
//! like `https://api.anthropic.com` that require a bearer token. For
//! private-network backends leave the key unset and the header is not injected.
//! Federated (peer) hops use the peer bearer instead.

use axum::body::Body;
use axum::extract::{Extension, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::auth::Identity;
use crate::config::Peer;
use crate::error::ApiError;
use crate::federation::{OriginIdentity, HDR_APP, HDR_ORIGIN_ROLE, HDR_ORIGIN_SUB};
use crate::state::AppState;

/// SSE comment injected every 15 s while the upstream is silent (queue wait or
/// long reasoning phase). Keeps client-side idle timeouts from firing before the
/// model produces its first content token.
const KEEPALIVE_FRAME: &[u8] = b": keep-alive\n\n";
const KEEPALIVE_INTERVAL_S: u64 = 15;

// ─────────────────────────────────────────── helpers

/// The model named in a (post-default) request body, if non-empty.
fn model_of(body: &Value) -> Option<String> {
    body.get("model")
        .and_then(|m| m.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Inject the server-configured default `model` when the caller omits it (or
/// leaves it empty/null). Mirrors `_apply_default_model`.
fn apply_default_model(st: &AppState, mut body: Value) -> Value {
    let needs_default = match body.get("model") {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(_) => false,
    };
    if needs_default {
        let dm = &st.settings.llm_default_model;
        if !dm.is_empty() {
            if let Value::Object(map) = &mut body {
                map.insert("model".into(), Value::String(dm.clone()));
            }
        }
    }
    body
}

/// `stream: true` → SSE.
fn wants_stream(body: &Value) -> bool {
    matches!(body.get("stream"), Some(Value::Bool(true)))
}

fn require_object(body: Value) -> Result<Value, ApiError> {
    if body.is_object() {
        Ok(body)
    } else {
        Err(ApiError::BadRequest("request body must be a JSON object".into()))
    }
}

/// Inject the local `llm_api_key` bearer (if set) on an outbound upstream call.
fn add_auth(st: &AppState, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if st.settings.llm_api_key.is_empty() {
        req
    } else {
        req.header("Authorization", format!("Bearer {}", st.settings.llm_api_key))
    }
}

fn sse_response(body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// The caller's origin identity for a forwarded (federated) `/v1/*` request,
/// from the gated `Identity` in request extensions. Default (empty) when absent
/// — the peer authenticates on the peer bearer regardless; the origin headers
/// are attribution/separation groundwork (F3).
fn origin_of(ident: Option<Extension<Identity>>) -> OriginIdentity {
    ident
        .map(|Extension(i)| OriginIdentity { sub: i.sub, role: i.role })
        .unwrap_or_default()
}

/// Resolve the federation peer for `/v1/*` traffic, if `LUMID_LLM_FEDERATE` is
/// set. `Ok(Some(peer))` ⇒ forward to that peer; `Ok(None)` ⇒ not federating,
/// use the local backend pool; `Err` ⇒ misconfigured (names an unknown peer).
fn federation_peer(st: &AppState) -> Result<Option<Peer>, ApiError> {
    match st.settings.llm_federate.as_deref() {
        None => Ok(None),
        Some(pid) => match st.federation.peer(pid) {
            Some(peer) => Ok(Some(peer.clone())),
            None => Err(ApiError::Unavailable(format!(
                "LUMID_LLM_FEDERATE={pid} names no configured peer (check LUMID_PEERS)"
            ))),
        },
    }
}

// ───────────────────────────────── federation (F1) proxy — peer forward

/// Apply outbound auth for a federated peer hop: the peer bearer + origin
/// headers.
fn apply_peer_auth(
    mut req: reqwest::RequestBuilder,
    st: &AppState,
    peer: &Peer,
    origin: &OriginIdentity,
) -> reqwest::RequestBuilder {
    if !peer.token.is_empty() {
        req = req.header("Authorization", format!("Bearer {}", peer.token));
    }
    req.header(HDR_ORIGIN_SUB, origin.sub.clone())
        .header(HDR_ORIGIN_ROLE, origin.role.clone())
        .header(HDR_APP, st.settings.app_id.clone())
}

/// One-shot proxy to a federation peer (non-streaming). Faithfully relays the
/// peer's status + JSON body (or wraps a non-JSON body).
async fn proxy_json_peer(
    st: &AppState,
    peer: &Peer,
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
    origin: &OriginIdentity,
) -> Response {
    let base = peer.base_url.trim_end_matches('/');
    let url = format!("{base}{path}");
    let mut req = st.http.request(method.clone(), &url);
    if let Some(b) = body {
        req = req.json(b);
    }
    req = apply_peer_auth(req, st, peer, origin);
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            let status = if e.is_timeout() {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::BAD_GATEWAY
            };
            let detail = if e.is_timeout() {
                "upstream LLM timed out"
            } else {
                tracing::warn!("peer {method} {path} → {} failed: {e}", peer.id);
                "upstream LLM unreachable"
            };
            return (status, Json(json!({ "detail": detail }))).into_response();
        }
    };
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("reading peer {path} body failed: {e}");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "detail": "upstream LLM unreachable" })),
            )
                .into_response();
        }
    };
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(payload) => (status, Json(payload)).into_response(),
        Err(_) => {
            let raw: String = String::from_utf8_lossy(&bytes).chars().take(1024).collect();
            (status, Json(json!({ "error": "non-json upstream response", "raw": raw }))).into_response()
        }
    }
}

/// Streaming proxy to a federation peer. Uses `http_stream` (connect timeout
/// only — no total timeout) and injects SSE keep-alive frames during silence.
async fn proxy_stream_peer(
    st: &AppState,
    peer: &Peer,
    path: &str,
    body: &Value,
    origin: &OriginIdentity,
) -> Response {
    let base = peer.base_url.trim_end_matches('/');
    let url = format!("{base}{path}");
    let mut req = st.http_stream.post(&url).json(body);
    req = apply_peer_auth(req, st, peer, origin);
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("peer POST {path} → {} stream failed: {e}", peer.id);
            let frame = format!("data: {}\n\n", json!({ "error": "upstream unreachable" }));
            return sse_response(Body::from(frame));
        }
    };
    if resp.status().as_u16() >= 400 {
        let code = resp.status().as_u16();
        let err_text = resp.text().await.unwrap_or_default();
        let err_text: String = err_text.chars().take(1024).collect();
        let frame = format!("data: {}\n\n", json!({ "error": err_text, "status": code }));
        return sse_response(Body::from(frame));
    }
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    tokio::spawn(async move {
        let mut ka = interval(Duration::from_secs(KEEPALIVE_INTERVAL_S));
        ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ka.tick().await;
        let mut upstream = Box::pin(resp.bytes_stream());
        loop {
            tokio::select! {
                biased;
                chunk = upstream.next() => {
                    match chunk {
                        Some(Ok(b)) => { if tx.send(b).is_err() { break; } }
                        _ => break,
                    }
                }
                _ = ka.tick() => {
                    if tx.send(Bytes::from_static(KEEPALIVE_FRAME)).is_err() { break; }
                }
            }
        }
    });
    let body_stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
    });
    sse_response(Body::from_stream(body_stream))
}

// ───────────────────────────────── local backend pool — non-streaming (retry)

/// Resolve backends for `model`. Returns `Err` (503) when pool is empty and
/// there's no OpenRouter catch-all. Returns `Ok(None)` when OpenRouter should
/// handle the request (model unknown + openrouter configured).
fn resolve(
    st: &AppState,
    model: Option<&str>,
) -> Result<Option<Vec<std::sync::Arc<crate::llm_pool::BackendHandle>>>, ApiError> {
    let backends = st.llm_pool.backends_for(model);
    if !backends.is_empty() {
        return Ok(Some(backends));
    }
    // Unknown explicit model → OpenRouter catch-all.
    if model.is_some() && !st.llm_pool.openrouter_url.is_empty() {
        return Ok(None); // caller will proxy to openrouter_url
    }
    Err(ApiError::Unavailable(
        "LLM backend not configured (LUMID_LLM_BACKEND_URL is empty)".into(),
    ))
}

/// Forward a non-streaming request. Tries backends in least-loaded-first order;
/// retries on connect failure or HTTP 503 (overloaded / not-yet-ready).
/// Short-circuits on any other status (4xx, 5xx except 503).
async fn proxy_json(
    st: &AppState,
    backends: &[std::sync::Arc<crate::llm_pool::BackendHandle>],
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
) -> Response {
    let mut last_err: Option<Response> = None;

    for handle in backends {
        let _guard = handle.acquire();
        let url = format!("{}{path}", handle.url);
        let mut req = st.http.request(method.clone(), &url);
        if let Some(b) = body {
            req = req.json(b);
        }
        req = add_auth(st, req);

        let upstream = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                handle.on_connect_err();
                tracing::warn!("llm {method} {path} → {} connect failed: {e}", handle.url);
                last_err = Some((
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "detail": "upstream LLM unreachable" })),
                )
                    .into_response());
                continue; // try next backend
            }
        };

        let status = upstream.status();
        if status.as_u16() == 503 {
            // Overloaded — try next backend before giving up.
            handle.on_connect_err();
            tracing::warn!("llm {method} {path} → {} 503, retrying", handle.url);
            last_err = Some((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "detail": "upstream LLM overloaded" })),
            )
                .into_response());
            continue;
        }

        handle.on_connect_ok();
        let ax_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let bytes = match upstream.bytes().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("llm reading body from {}: {e}", handle.url);
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "detail": "upstream LLM body truncated" })),
                )
                    .into_response();
            }
        };
        return match serde_json::from_slice::<Value>(&bytes) {
            Ok(payload) => (ax_status, Json(payload)).into_response(),
            Err(_) => {
                let raw: String = String::from_utf8_lossy(&bytes).chars().take(1024).collect();
                (ax_status, Json(json!({ "error": "non-json upstream response", "raw": raw })))
                    .into_response()
            }
        };
    }

    last_err.unwrap_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "detail": "all LLM backends unavailable" })),
        )
            .into_response()
    })
}

/// Forward to the OpenRouter catch-all (non-streaming).
async fn proxy_json_openrouter(st: &AppState, method: reqwest::Method, path: &str, body: Option<&Value>) -> Response {
    let base = &st.llm_pool.openrouter_url;
    let url = format!("{base}{path}");
    let mut req = st.http.request(method.clone(), &url);
    if let Some(b) = body {
        req = req.json(b);
    }
    req = add_auth(st, req);
    match req.send().await {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            match r.bytes().await {
                Ok(b) => match serde_json::from_slice::<Value>(&b) {
                    Ok(v) => (status, Json(v)).into_response(),
                    Err(_) => {
                        let raw: String = String::from_utf8_lossy(&b).chars().take(1024).collect();
                        (status, Json(json!({ "error": "non-json", "raw": raw }))).into_response()
                    }
                },
                Err(_) => (StatusCode::BAD_GATEWAY, Json(json!({ "detail": "openrouter body error" }))).into_response(),
            }
        }
        Err(e) => {
            tracing::warn!("openrouter {method} {path} failed: {e}");
            (StatusCode::BAD_GATEWAY, Json(json!({ "detail": "openrouter unreachable" }))).into_response()
        }
    }
}

// ───────────────────────────── local backend pool — streaming (retry on connect)

/// Forward a streaming request. Tries backends in order; retries on connect
/// failure before any bytes have been sent to the client. Once streaming starts,
/// no retry is possible.
async fn proxy_stream(
    st: &AppState,
    backends: &[std::sync::Arc<crate::llm_pool::BackendHandle>],
    path: &str,
    body: &Value,
) -> Response {
    for handle in backends {
        let guard = handle.acquire();
        let url = format!("{}{path}", handle.url);
        let req = add_auth(st, st.http_stream.post(&url).json(body));

        let upstream = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                handle.on_connect_err();
                tracing::warn!("llm stream POST {path} → {} connect failed: {e}", handle.url);
                continue;
            }
        };

        if upstream.status().as_u16() == 503 {
            handle.on_connect_err();
            tracing::warn!("llm stream POST {path} → {} 503, retrying", handle.url);
            continue;
        }

        if upstream.status().as_u16() >= 400 {
            handle.on_connect_ok();
            let code = upstream.status().as_u16();
            let text = upstream.text().await.unwrap_or_default();
            let text: String = text.chars().take(1024).collect();
            let frame = format!("data: {}\n\n", json!({ "error": text, "status": code }));
            return sse_response(Body::from(frame));
        }

        handle.on_connect_ok();
        // Connected successfully — stream. The guard is moved into the spawn
        // so inflight stays incremented until the upstream is exhausted.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        tokio::spawn(async move {
            let _guard = guard; // holds inflight until stream ends
            let mut ka = interval(Duration::from_secs(KEEPALIVE_INTERVAL_S));
            ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ka.tick().await;
            let mut upstream = Box::pin(upstream.bytes_stream());
            loop {
                tokio::select! {
                    biased;
                    chunk = upstream.next() => {
                        match chunk {
                            Some(Ok(b)) => { if tx.send(b).is_err() { break; } }
                            _ => break,
                        }
                    }
                    _ = ka.tick() => {
                        if tx.send(Bytes::from_static(KEEPALIVE_FRAME)).is_err() { break; }
                    }
                }
            }
        });

        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
        });
        return sse_response(Body::from_stream(stream));
    }

    // All backends failed at connect.
    let frame = format!("data: {}\n\n", json!({ "error": "all LLM backends unavailable" }));
    sse_response(Body::from(frame))
}

async fn proxy_stream_openrouter(st: &AppState, path: &str, body: &Value) -> Response {
    let base = &st.llm_pool.openrouter_url;
    let url = format!("{base}{path}");
    let req = add_auth(st, st.http_stream.post(&url).json(body));
    match req.send().await {
        Ok(upstream) if upstream.status().as_u16() < 400 => {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            tokio::spawn(async move {
                let mut ka = interval(Duration::from_secs(KEEPALIVE_INTERVAL_S));
                ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
                ka.tick().await;
                let mut stream = Box::pin(upstream.bytes_stream());
                loop {
                    tokio::select! {
                        biased;
                        chunk = stream.next() => match chunk {
                            Some(Ok(b)) => { if tx.send(b).is_err() { break; } }
                            _ => break,
                        },
                        _ = ka.tick() => {
                            if tx.send(Bytes::from_static(KEEPALIVE_FRAME)).is_err() { break; }
                        }
                    }
                }
            });
            let body_stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
            });
            sse_response(Body::from_stream(body_stream))
        }
        Ok(r) => {
            let code = r.status().as_u16();
            let text = r.text().await.unwrap_or_default();
            let frame = format!("data: {}\n\n", json!({ "error": text, "status": code }));
            sse_response(Body::from(frame))
        }
        Err(e) => {
            tracing::warn!("openrouter stream {path} failed: {e}");
            let frame = format!("data: {}\n\n", json!({ "error": "openrouter unreachable" }));
            sse_response(Body::from(frame))
        }
    }
}

// ─────────────────────────────────────────── dispatch (outer federation switch)

/// Dispatch a non-streaming `/v1/*` request. Federation is the outer switch:
/// forward to the peer when `llm_federate` is set, else use the local pool /
/// OpenRouter catch-all.
async fn dispatch_json(
    st: &AppState,
    ident: Option<Extension<Identity>>,
    method: reqwest::Method,
    path: &str,
    body: &Value,
) -> Response {
    match federation_peer(st) {
        Err(e) => e.into_response(),
        Ok(Some(peer)) => {
            proxy_json_peer(st, &peer, method, path, Some(body), &origin_of(ident)).await
        }
        Ok(None) => match resolve(st, model_of(body).as_deref()) {
            Err(e) => e.into_response(),
            Ok(None) => proxy_json_openrouter(st, method, path, Some(body)).await,
            Ok(Some(backends)) => proxy_json(st, &backends, method, path, Some(body)).await,
        },
    }
}

/// Dispatch a streaming `/v1/*` request. Same outer federation switch as
/// `dispatch_json`.
async fn dispatch_stream(
    st: &AppState,
    ident: Option<Extension<Identity>>,
    path: &str,
    body: &Value,
) -> Response {
    match federation_peer(st) {
        Err(e) => e.into_response(),
        Ok(Some(peer)) => proxy_stream_peer(st, &peer, path, body, &origin_of(ident)).await,
        Ok(None) => match resolve(st, model_of(body).as_deref()) {
            Err(e) => e.into_response(),
            Ok(None) => proxy_stream_openrouter(st, path, body).await,
            Ok(Some(backends)) => proxy_stream(st, &backends, path, body).await,
        },
    }
}

// ─────────────────────────────────────────── route handlers

/// GET /v1/models — federated: forward to the peer verbatim. Local: aggregate
/// the `data` list across every configured backend (primary + pool +
/// openrouter), deduped by model id. Best-effort: a backend that errors is
/// skipped. 503 only when nothing is configured.
pub async fn list_models(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
) -> Response {
    // Federation default-route: forward `/v1/models` to the peer verbatim.
    match federation_peer(&st) {
        Err(e) => return e.into_response(),
        Ok(Some(peer)) => {
            return proxy_json_peer(
                &st,
                &peer,
                reqwest::Method::GET,
                "/v1/models",
                None,
                &origin_of(ident),
            )
            .await;
        }
        Ok(None) => {}
    }

    // Distinct backend base URLs, primary first.
    let mut bases: Vec<String> = Vec::new();
    let primary = st.settings.llm_backend_url.trim_end_matches('/').to_string();
    if !primary.is_empty() {
        bases.push(primary);
    }
    for h in &st.llm_pool.all {
        if !bases.contains(&h.url) {
            bases.push(h.url.clone());
        }
    }
    if !st.llm_pool.openrouter_url.is_empty() {
        let or = st.llm_pool.openrouter_url.trim_end_matches('/').to_string();
        if !bases.contains(&or) {
            bases.push(or);
        }
    }
    if bases.is_empty() {
        return ApiError::Unavailable("LLM backend not configured".into()).into_response();
    }
    if bases.len() == 1 {
        return proxy_json(
            &st,
            &st.llm_pool.backends_for(None),
            reqwest::Method::GET,
            "/v1/models",
            None,
        )
        .await;
    }
    let mut data: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    for base in &bases {
        let mut req = st.http.get(format!("{base}/v1/models"));
        if !st.settings.llm_api_key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", st.settings.llm_api_key));
        }
        let r = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("list_models: {base} unreachable: {e}");
                continue;
            }
        };
        let v: Value = match r.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("list_models: {base} json error: {e}");
                continue;
            }
        };
        if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
            for m in arr {
                let id = m.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
                if seen.insert(id) {
                    let mut m = m.clone();
                    // Normalize across serving stacks: vLLM advertises
                    // `max_model_len`; llama.cpp reports the per-slot window
                    // as `meta.n_ctx` instead. Surface the same field for
                    // both so catalog consumers get one shape.
                    // llama.cpp emits the key as an explicit JSON null — treat
                    // null the same as absent.
                    if m.get("max_model_len").map_or(true, |v| v.is_null()) {
                        if let Some(n_ctx) = m
                            .get("meta")
                            .and_then(|meta| meta.get("n_ctx"))
                            .and_then(|n| n.as_i64())
                        {
                            if let Some(obj) = m.as_object_mut() {
                                obj.insert("max_model_len".into(), json!(n_ctx));
                            }
                        }
                    }
                    data.push(m);
                }
            }
        }
    }
    Json(json!({ "object": "list", "data": data })).into_response()
}

/// POST /v1/chat/completions
pub async fn chat_completions(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => apply_default_model(&st, b),
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        dispatch_stream(&st, ident, "/v1/chat/completions", &body).await
    } else {
        dispatch_json(&st, ident, reqwest::Method::POST, "/v1/chat/completions", &body).await
    }
}

/// POST /v1/completions
pub async fn completions(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => apply_default_model(&st, b),
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        dispatch_stream(&st, ident, "/v1/completions", &body).await
    } else {
        dispatch_json(&st, ident, reqwest::Method::POST, "/v1/completions", &body).await
    }
}

/// POST /v1/embeddings (non-streaming)
pub async fn embeddings(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => apply_default_model(&st, b),
        Err(e) => return e.into_response(),
    };
    dispatch_json(&st, ident, reqwest::Method::POST, "/v1/embeddings", &body).await
}

// -------------------------------------------------------------- Anthropic

/// POST /v1/messages
pub async fn messages(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => apply_default_model(&st, b),
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        dispatch_stream(&st, ident, "/v1/messages", &body).await
    } else {
        dispatch_json(&st, ident, reqwest::Method::POST, "/v1/messages", &body).await
    }
}

/// POST /v1/messages/count_tokens (non-streaming)
pub async fn count_tokens(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => apply_default_model(&st, b),
        Err(e) => return e.into_response(),
    };
    dispatch_json(&st, ident, reqwest::Method::POST, "/v1/messages/count_tokens", &body).await
}
