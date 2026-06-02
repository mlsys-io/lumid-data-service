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
//! We do NOT inject auth into upstream calls — the backend is on the private
//! network and trusts its caller. Lineage-stripping doesn't apply (the upstream
//! is an LLM, not Postgres); payloads pass through verbatim.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::state::AppState;

/// The model named in a (post-default) request body, if non-empty.
fn model_of(body: &Value) -> Option<String> {
    body.get("model")
        .and_then(|m| m.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Resolve the upstream base URL for a request's `model`. A model listed in
/// `llm_backends` routes to that backend; anything else (incl. no model) routes
/// to the primary `llm_backend_url`. 503 when nothing is configured.
fn backend_for_model(st: &AppState, model: Option<&str>) -> Result<String, ApiError> {
    if let Some(m) = model {
        if let Some((_, url)) = st.settings.llm_backends.iter().find(|(bm, _)| bm == m) {
            return Ok(url.trim_end_matches('/').to_string());
        }
    }
    let primary = st.settings.llm_backend_url.trim_end_matches('/');
    if primary.is_empty() {
        return Err(ApiError::Unavailable(
            "LLM backend not configured (LUMID_LLM_BACKEND_URL is empty)".into(),
        ));
    }
    Ok(primary.to_string())
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
async fn proxy_json(
    st: &AppState,
    base: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Response {
    let url = format!("{base}{path}");
    let mut req = st.http.request(method.clone(), &url);
    if let Some(b) = body {
        req = req.json(&b);
    }
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
/// chunked JSONLs; bytes pass through unchanged with no buffering.
async fn proxy_stream(st: &AppState, base: &str, path: &str, body: Value) -> Response {
    let url = format!("{base}{path}");
    let resp = match st.http.post(&url).json(&body).send().await {
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
    // Stream raw upstream bytes through unchanged.
    let stream = resp.bytes_stream();
    sse_response(Body::from_stream(stream))
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

// ----------------------------------------------------------------- OpenAI

/// GET /v1/models — aggregate the `data` list across every configured backend
/// (primary + `llm_backends`), deduped by model id. Best-effort: a backend that
/// errors is skipped. 503 only when nothing is configured.
pub async fn list_models(State(st): State<AppState>) -> Response {
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
        return proxy_json(&st, &bases[0], reqwest::Method::GET, "/v1/models", None).await;
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
pub async fn chat_completions(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let base = match backend_for_model(&st, model_of(&body).as_deref()) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        proxy_stream(&st, &base, "/v1/chat/completions", body).await
    } else {
        proxy_json(&st, &base, reqwest::Method::POST, "/v1/chat/completions", Some(body)).await
    }
}

/// POST /v1/completions
pub async fn completions(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let base = match backend_for_model(&st, model_of(&body).as_deref()) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        proxy_stream(&st, &base, "/v1/completions", body).await
    } else {
        proxy_json(&st, &base, reqwest::Method::POST, "/v1/completions", Some(body)).await
    }
}

/// POST /v1/embeddings (non-streaming)
pub async fn embeddings(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let base = match backend_for_model(&st, model_of(&body).as_deref()) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    proxy_json(&st, &base, reqwest::Method::POST, "/v1/embeddings", Some(body)).await
}

// -------------------------------------------------------------- Anthropic

/// POST /v1/messages
pub async fn messages(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let base = match backend_for_model(&st, model_of(&body).as_deref()) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    if wants_stream(&body) {
        proxy_stream(&st, &base, "/v1/messages", body).await
    } else {
        proxy_json(&st, &base, reqwest::Method::POST, "/v1/messages", Some(body)).await
    }
}

/// POST /v1/messages/count_tokens (non-streaming)
pub async fn count_tokens(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match require_object(body.0) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let body = apply_default_model(&st, body);
    let base = match backend_for_model(&st, model_of(&body).as_deref()) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    proxy_json(&st, &base, reqwest::Method::POST, "/v1/messages/count_tokens", Some(body)).await
}
