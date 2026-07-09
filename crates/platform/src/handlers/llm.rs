//! LLM reverse proxy — OpenAI + Anthropic compatible. Port of `api/routes/llm.py`.
//!
//! A thin stateless reverse proxy in front of one or more upstream
//! OpenAI/Anthropic-compatible inference servers. The request's `model` selects
//! the backend: a model listed in `settings.llm_backends` (`LUMID_LLM_BACKENDS`)
//! routes to that server; everything else (incl. no model → the default) routes
//! to the primary `settings.llm_backend_url`. `/v1/models` aggregates all
//! backends. Same auth model as every other route — the `gate` runs at router level.
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
//! <key>` on all outbound upstream calls, enabling use of hosted endpoints like
//! `https://api.anthropic.com` that require a bearer token. For private-network
//! backends leave the key unset and the header is not injected.

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

/// The model named in a (post-default) request body, if non-empty.
fn model_of(body: &Value) -> Option<String> {
    body.get("model")
        .and_then(|m| m.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// The resolved upstream for a `/v1/*` request: a base URL plus, when the
/// request is federated (`LUMID_LLM_FEDERATE`), the peer whose bearer + origin
/// headers authenticate the hop. `peer: None` ⇒ a local LLM backend (unchanged
/// behavior: `llm_api_key` is injected if set).
struct LlmTarget {
    base: String,
    peer: Option<Peer>,
}

/// Resolve the upstream for a request's `model`.
///
/// Federation default-route (F1): when `LUMID_LLM_FEDERATE` names a configured
/// peer, ALL `/v1/*` traffic targets that peer's base URL (the peer serves LLM
/// from ITS `llm_backends`), authenticated with the peer bearer. When unset,
/// behavior is unchanged: a model listed in `llm_backends` routes to that
/// backend; anything else (incl. no model) routes to the primary
/// `llm_backend_url`; 503 when nothing is configured.
fn resolve_llm_target(st: &AppState, model: Option<&str>) -> Result<LlmTarget, ApiError> {
    if let Some(pid) = st.settings.llm_federate.as_deref() {
        match st.federation.peer(pid) {
            Some(peer) => {
                return Ok(LlmTarget {
                    base: peer.base_url.trim_end_matches('/').to_string(),
                    peer: Some(peer.clone()),
                });
            }
            None => {
                return Err(ApiError::Unavailable(format!(
                    "LUMID_LLM_FEDERATE={pid} names no configured peer (check LUMID_PEERS)"
                )));
            }
        }
    }
    if let Some(m) = model {
        if let Some((_, url)) = st.settings.llm_backends.iter().find(|(bm, _)| bm == m) {
            return Ok(LlmTarget {
                base: url.trim_end_matches('/').to_string(),
                peer: None,
            });
        }
    }
    let primary = st.settings.llm_backend_url.trim_end_matches('/');
    if primary.is_empty() {
        return Err(ApiError::Unavailable(
            "LLM backend not configured (LUMID_LLM_BACKEND_URL is empty)".into(),
        ));
    }
    Ok(LlmTarget { base: primary.to_string(), peer: None })
}

/// Apply outbound auth to a `/v1/*` upstream request: the peer bearer + origin
/// headers when federating, else the local `llm_api_key` (if set). Keeps the two
/// auth modes in one place so every proxy helper stays consistent.
fn apply_upstream_auth(
    mut req: reqwest::RequestBuilder,
    st: &AppState,
    peer: Option<&Peer>,
    origin: &OriginIdentity,
) -> reqwest::RequestBuilder {
    match peer {
        Some(p) => {
            if !p.token.is_empty() {
                req = req.header("Authorization", format!("Bearer {}", p.token));
            }
            req = req
                .header(HDR_ORIGIN_SUB, origin.sub.clone())
                .header(HDR_ORIGIN_ROLE, origin.role.clone())
                .header(HDR_APP, st.settings.app_id.clone());
        }
        None => {
            if !st.settings.llm_api_key.is_empty() {
                req = req.header(
                    "Authorization",
                    format!("Bearer {}", st.settings.llm_api_key),
                );
            }
        }
    }
    req
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

// NB: ApiError has no GatewayTimeout/BadGateway variant; the Python uses 504/502.
// To preserve those exact statuses we return a hand-built Response from the
// proxy helpers instead of leaning on ApiError's status mapping (which would
// collapse both into 503). `require_backend` still uses ApiError for the 503.

/// One-shot proxy for non-streaming endpoints. Faithfully relays upstream
/// status + JSON body (or wraps a non-JSON body).
#[allow(clippy::too_many_arguments)]
async fn proxy_json(
    st: &AppState,
    base: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
    peer: Option<&Peer>,
    origin: &OriginIdentity,
) -> Response {
    let url = format!("{base}{path}");
    let mut req = st.http.request(method.clone(), &url);
    if let Some(b) = body {
        req = req.json(&b);
    }
    req = apply_upstream_auth(req, st, peer, origin);
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
                tracing::warn!("upstream {method} {path} failed: {e}");
                "upstream LLM unreachable"
            };
            return (status, Json(json!({ "detail": detail }))).into_response();
        }
    };
    let status = StatusCode::from_u16(resp.status().as_u16())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("reading upstream {path} body failed: {e}");
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
            // Non-JSON upstream response: wrap (truncate to 1024 chars like Python).
            let raw = String::from_utf8_lossy(&bytes);
            let raw: String = raw.chars().take(1024).collect();
            (
                status,
                Json(json!({ "error": "non-json upstream response", "raw": raw })),
            )
                .into_response()
        }
    }
}

/// Streaming proxy. Upstream is expected to return SSE (`text/event-stream`) or
/// chunked JSONLs. Uses `http_stream` (connect timeout only — no total timeout)
/// so multi-minute reasoning generations aren't cut off at 120 s.
///
/// Injects SSE comment keep-alive frames every `KEEPALIVE_INTERVAL_S` seconds of
/// upstream silence (queue wait before first token, or long thinking phase).
async fn proxy_stream(
    st: &AppState,
    base: &str,
    path: &str,
    body: Value,
    peer: Option<&Peer>,
    origin: &OriginIdentity,
) -> Response {
    let url = format!("{base}{path}");
    let mut req = st.http_stream.post(&url).json(&body);
    req = apply_upstream_auth(req, st, peer, origin);
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            // Forward a single SSE error frame (matches the Python behaviour).
            tracing::warn!("upstream POST {path} stream failed: {e}");
            let frame = format!(
                "data: {}\n\n",
                json!({ "error": "upstream unreachable" })
            );
            return sse_response(Body::from(frame));
        }
    };
    if resp.status().as_u16() >= 400 {
        let code = resp.status().as_u16();
        let err_text = resp.text().await.unwrap_or_default();
        let err_text: String = err_text.chars().take(1024).collect();
        let frame = format!(
            "data: {}\n\n",
            json!({ "error": err_text, "status": code })
        );
        return sse_response(Body::from(frame));
    }

    // Spawn a task that pipes upstream bytes to a channel, injecting SSE
    // keep-alive comments during any silent gaps >= KEEPALIVE_INTERVAL_S.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    tokio::spawn(async move {
        let mut ka = interval(Duration::from_secs(KEEPALIVE_INTERVAL_S));
        ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ka.tick().await; // discard the first immediate tick
        let mut upstream = Box::pin(resp.bytes_stream());
        loop {
            tokio::select! {
                biased;
                chunk = upstream.next() => {
                    match chunk {
                        Some(Ok(b)) => { if tx.send(b).is_err() { break; } }
                        // On upstream error or end, close the channel (drops tx).
                        _ => break,
                    }
                }
                _ = ka.tick() => {
                    if tx.send(Bytes::from_static(KEEPALIVE_FRAME)).is_err() { break; }
                }
            }
        }
    });

    // Convert the mpsc receiver to a Stream for axum Body.
    let body_stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
    });
    sse_response(Body::from_stream(body_stream))
}

fn sse_response(body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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

/// The caller's origin identity for a forwarded (federated) `/v1/*` request,
/// from the gated `Identity` in request extensions. Default (empty) when absent
/// — the peer authenticates on the peer bearer regardless; the origin headers
/// are attribution/separation groundwork (F3).
fn origin_of(ident: Option<Extension<Identity>>) -> OriginIdentity {
    ident
        .map(|Extension(i)| OriginIdentity { sub: i.sub, role: i.role })
        .unwrap_or_default()
}

// ----------------------------------------------------------------- OpenAI

/// GET /v1/models — aggregate the `data` list across every configured backend
/// (primary + `llm_backends`), deduped by model id. Best-effort: a backend that
/// errors is skipped. 503 only when nothing is configured.
pub async fn list_models(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
) -> Response {
    // Federation default-route: forward `/v1/models` to the peer verbatim.
    if st.settings.llm_federate.is_some() {
        let target = match resolve_llm_target(&st, None) {
            Ok(t) => t,
            Err(e) => return e.into_response(),
        };
        return proxy_json(
            &st,
            &target.base,
            reqwest::Method::GET,
            "/v1/models",
            None,
            target.peer.as_ref(),
            &origin_of(ident),
        )
        .await;
    }
    // Distinct backend base URLs, primary first.
    let mut bases: Vec<String> = Vec::new();
    let primary = st.settings.llm_backend_url.trim_end_matches('/').to_string();
    if !primary.is_empty() {
        bases.push(primary);
    }
    for (_, url) in &st.settings.llm_backends {
        let u = url.trim_end_matches('/').to_string();
        if !bases.contains(&u) {
            bases.push(u);
        }
    }
    if bases.is_empty() {
        return ApiError::Unavailable(
            "LLM backend not configured (LUMID_LLM_BACKEND_URL is empty)".into(),
        )
        .into_response();
    }
    // Single backend → relay verbatim (preserves the upstream's exact shape).
    if bases.len() == 1 {
        return proxy_json(
            &st,
            &bases[0],
            reqwest::Method::GET,
            "/v1/models",
            None,
            None,
            &OriginIdentity::default(),
        )
        .await;
    }
    let mut data: Vec<Value> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for base in &bases {
        let resp = match st.http.get(format!("{base}/v1/models")).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("list_models: backend {base} unreachable: {e}");
                continue;
            }
        };
        let v: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
            for m in arr {
                let id = m.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
                if seen.insert(id) {
                    data.push(m.clone());
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
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let target = match resolve_llm_target(&st, model_of(&body).as_deref()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let origin = origin_of(ident);
    if wants_stream(&body) {
        proxy_stream(&st, &target.base, "/v1/chat/completions", body, target.peer.as_ref(), &origin).await
    } else {
        proxy_json(&st, &target.base, reqwest::Method::POST, "/v1/chat/completions", Some(body), target.peer.as_ref(), &origin).await
    }
}

/// POST /v1/completions
pub async fn completions(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let target = match resolve_llm_target(&st, model_of(&body).as_deref()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let origin = origin_of(ident);
    if wants_stream(&body) {
        proxy_stream(&st, &target.base, "/v1/completions", body, target.peer.as_ref(), &origin).await
    } else {
        proxy_json(&st, &target.base, reqwest::Method::POST, "/v1/completions", Some(body), target.peer.as_ref(), &origin).await
    }
}

/// POST /v1/embeddings (non-streaming)
pub async fn embeddings(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let target = match resolve_llm_target(&st, model_of(&body).as_deref()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    proxy_json(&st, &target.base, reqwest::Method::POST, "/v1/embeddings", Some(body), target.peer.as_ref(), &origin_of(ident)).await
}

// -------------------------------------------------------------- Anthropic

/// POST /v1/messages
pub async fn messages(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let target = match resolve_llm_target(&st, model_of(&body).as_deref()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let origin = origin_of(ident);
    if wants_stream(&body) {
        proxy_stream(&st, &target.base, "/v1/messages", body, target.peer.as_ref(), &origin).await
    } else {
        proxy_json(&st, &target.base, reqwest::Method::POST, "/v1/messages", Some(body), target.peer.as_ref(), &origin).await
    }
}

/// POST /v1/messages/count_tokens (non-streaming)
pub async fn count_tokens(
    ident: Option<Extension<Identity>>,
    State(st): State<AppState>,
    body: Json<Value>,
) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let target = match resolve_llm_target(&st, model_of(&body).as_deref()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    proxy_json(&st, &target.base, reqwest::Method::POST, "/v1/messages/count_tokens", Some(body), target.peer.as_ref(), &origin_of(ident)).await
}
