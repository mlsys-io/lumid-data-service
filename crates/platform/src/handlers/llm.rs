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

/// Map Anthropic's `thinking` control onto the knob the on-prem backend
/// actually reads.
///
/// The Anthropic shape is `{"thinking": {"type": "disabled"}}`, and vLLM
/// IGNORES it — measured against the GB10 head on /v1/messages: 116s and an
/// 11k-character thinking block, byte-identical to not sending it at all. What
/// vLLM does read is `chat_template_kwargs.thinking`, which suppresses the
/// preamble entirely (same prompt: 1s, no thinking block). So the documented
/// control silently did nothing, which is worse than not offering one — a
/// caller has no way to tell "ignored" from "had no effect".
///
/// Only `disabled` is mapped. `enabled` is already the default, and Anthropic's
/// `budget_tokens` has no vLLM equivalent, so forwarding it as-is is honest:
/// upstreams that understand it still get it.
///
/// The original `thinking` field is deliberately LEFT IN the body. Not every
/// backend behind this gateway is vLLM — OpenRouter and a real Anthropic
/// endpoint honour it natively, and stripping it would break the callers this
/// is meant to serve.
///
/// An explicit `chat_template_kwargs.thinking` from the caller WINS: it is the
/// backend-level control, so someone who set it deliberately is not overridden
/// by the higher-level alias.
///
/// NOTE FOR CALLERS: this changes ANSWERS, not just latency. Measured on a
/// scoring task, suppressing the preamble moved the verdict (2/13 -> 0/13) and
/// widened run-to-run spread on identical input. It is a per-request opt-in for
/// work that does not need deliberation (classification, extraction, routing),
/// never a default, and never for anything whose output is compared over time.
fn apply_thinking_control(mut body: Value) -> Value {
    let disabled = body
        .get("thinking")
        .and_then(|t| t.get("type"))
        .and_then(|t| t.as_str())
        .is_some_and(|t| t.eq_ignore_ascii_case("disabled"));
    if !disabled {
        return body;
    }
    let Value::Object(map) = &mut body else {
        return body;
    };
    let ctk = map
        .entry("chat_template_kwargs")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Value::Object(ctk) = ctk {
        if !ctk.contains_key("thinking") {
            ctk.insert("thinking".into(), Value::Bool(false));
        }
    }
    body
}

/// `stream: true` → SSE.
fn wants_stream(body: &Value) -> bool {
    matches!(body.get("stream"), Some(Value::Bool(true)))
}

/// Rewrite `model` per `llm_openrouter_model_map` before forwarding to
/// OpenRouter. Local pool ids (e.g. `deepseek-v4-flash`) are not valid
/// OpenRouter ids; sending them verbatim 400s there. Only applied on the
/// OpenRouter path — local-backend requests never see this. A model not in
/// the map (kimi-k3, GLM-5.2, an already-OpenRouter-shaped id) passes through
/// unchanged, since those are already the correct external id.
fn rewrite_model_for_openrouter(st: &AppState, body: &Value) -> Value {
    let Some(local) = model_of(body) else {
        return body.clone();
    };
    let Some(or_id) = st.settings.llm_openrouter_model_map.get(&local) else {
        return body.clone();
    };
    let mut b = body.clone();
    if let Value::Object(map) = &mut b {
        map.insert("model".into(), Value::String(or_id.clone()));
    }
    b
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

/// Inject auth for the OpenRouter catch-all path. Uses `llm_openrouter_key` when
/// set; falls back to `llm_api_key`. Local tailnet backends always use `add_auth`.
fn add_openrouter_auth(st: &AppState, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let key = if !st.settings.llm_openrouter_key.is_empty() {
        &st.settings.llm_openrouter_key
    } else {
        &st.settings.llm_api_key
    };
    if key.is_empty() {
        req
    } else {
        req.header("Authorization", format!("Bearer {key}"))
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

/// A short, non-PII-ish label for the caller, for cost attribution in logs.
///
/// Email when we have it (that is what an operator recognises), else the sub,
/// else "anon". Deliberately not the whole Identity: this lands on every
/// metered request, and a log line is not an audit record.
fn caller_label(ident: &Option<Extension<Identity>>) -> String {
    match ident {
        Some(Extension(i)) => i
            .email
            .clone()
            .unwrap_or_else(|| if i.sub.is_empty() { "anon".into() } else { i.sub.clone() }),
        None => "anon".into(),
    }
}

/// The caller's role for `resolve()`'s admin-only-metered-model gate. `gate`
/// (auth/mod.rs) already rejects an anonymous request on every data route
/// before a handler runs, so `None` here in practice means "local key" — the
/// same identity-less-but-trusted caller `Identity::role == "local"` would
/// otherwise carry; treat it as at least as privileged as admin rather than
/// denying an operator's own local-key tooling.
fn caller_role(ident: &Option<Extension<Identity>>) -> String {
    match ident {
        Some(Extension(i)) => i.role.clone(),
        None => "local".into(),
    }
}

// ───────────────────────────────── local backend pool — non-streaming (retry)

/// Resolve backends for `model`. Returns `Err` (503) when pool is empty and
/// there's no OpenRouter catch-all. Returns `Ok(None)` when OpenRouter should
/// handle the request (model unknown + openrouter configured).
/// Whether `model` is served BY OpenRouter rather than merely overflowed to it.
///
/// True only for a model an operator explicitly listed in
/// `LUMID_LLM_OPENROUTER_MODEL_MAP` while OpenRouter is configured. That map is
/// the allowlist, which is the whole safety property: a typo, a hallucinated
/// id, or a model we simply do not carry is NOT in it and is refused with our
/// own error instead of being forwarded to a metered upstream and billed.
fn openrouter_serves(
    map: &std::collections::HashMap<String, String>,
    openrouter_url: &str,
    model: &str,
) -> bool {
    !openrouter_url.is_empty() && map.contains_key(model)
}

/// Whether `resolve()`'s roof/health-overflow branch may hand this request to
/// OpenRouter. `model: None` can never overflow — there is nothing to check
/// against the allowlist, so the safe default is "stay local, fail honestly
/// if every local backend is down" rather than guessing. Pulled out of
/// `resolve()` as its own function so THIS gating condition — not just
/// `openrouter_serves` in isolation — has a direct regression test; see
/// `resolve()`'s call site comment for the incident this closes.
fn can_overflow_to_openrouter(
    map: &std::collections::HashMap<String, String>,
    openrouter_url: &str,
    model: Option<&str>,
) -> bool {
    model.map_or(false, |m| openrouter_serves(map, openrouter_url, m))
}

/// Roles allowed to reach a model that has NO local backend (i.e. every
/// request is a real, metered OpenRouter charge — see the "OPENROUTER-SERVED
/// MODELS" branch below). Mirrors claude-proxy's `denyExternalModelForRole`
/// policy (self-hosted models open to everyone, every other non-Anthropic
/// model admin-only) — pulled into `lumid-llm` itself 2026-09-01 after an
/// audit found that policy was enforced ONLY at the claude-proxy door.
/// Calling `lumid-llm` directly (lum.id/llm, or any other caller holding a
/// Lumid PAT) bypassed it entirely: two `role=user` accounts were observed
/// live racking up real `qwen/qwen3.6-27b` charges with zero gate. Local
/// (self-hosted) models are unaffected — the roof/health-overflow branch
/// above, which only ever fires for a model that already has a local
/// backend, deliberately keeps rescuing role=user's on-prem traffic during
/// saturation; only "no local backend at all" is role-gated.
fn role_may_use_metered_openrouter_model(role: &str) -> bool {
    // "local" = a caller authenticated via a local API key (auth/mod.rs),
    // e.g. an operator's own tooling or another in-cluster service — same
    // trust tier as super_admin elsewhere in this crate (ingest.rs's
    // require_admin, blobs.rs's local-key bypass).
    role == "admin" || role == "super_admin" || role == "local"
}

fn resolve(
    st: &AppState,
    model: Option<&str>,
    caller: &str,
    caller_role: &str,
) -> Result<Option<Vec<std::sync::Arc<crate::llm_pool::BackendHandle>>>, ApiError> {
    // CLAUDE NEVER GOES TO OPENROUTER. Claude models (claude-sonnet-*, claude-haiku-*,
    // claude-opus-*, claude-fable-*) are proprietary pooled-account models served by
    // claude-proxy against the Anthropic subscription — they must NEVER fall through
    // to the metered OpenRouter catch-all. claude-proxy already rewrites ordinary
    // users' sonnet/haiku to deepseek-v4-flash before routing, so a `claude-*` reaching
    // lumid-llm is either a direct call or an admin's genuine pooled request — neither
    // belongs on OpenRouter. Refuse it outright (a claude id has no local backend here;
    // the only thing it could ever resolve to is the catch-all). This is what was
    // silently billing metered sonnet on OpenRouter.
    let is_claude = model.map_or(false, |m| m.to_ascii_lowercase().starts_with("claude-"));
    if is_claude {
        return Err(ApiError::Unavailable(
            "Claude models are served by the Anthropic pool via claude-proxy, not here — refusing (never OpenRouter)".into(),
        ));
    }

    let backends = st.llm_pool.backends_for(model);
    if !backends.is_empty() {
        // Overflow to OpenRouter when EVERY local backend for this model is at its
        // concurrency roof (healthy but saturated) AND OpenRouter is configured.
        // Rationale: piling onto the saturated on-prem GB10 pushes it into the
        // saturation-tipping-into-prefill-stall regime (many concurrent users evict
        // each other's prefix cache and every turn pays cold prefill). Sending the
        // overflow to the metered OpenRouter version is a deliberate availability
        // trade: the on-prem roof is the guardrail, OpenRouter absorbs the peak.
        let all_at_roof = st.settings.llm_backend_max_concurrency > 0
            && backends.iter().all(|h| h.at_roof());
        // Health must be an EXPLICIT spill trigger. It never was: an outage only
        // reached OpenRouter by accident, because a backend that died under load
        // left a last-scraped queue depth >= roof and `at_roof()` stayed true off
        // that stale value. Now that the scraper clears a skipped (unhealthy)
        // backend's depth to -1, the accident is gone -- and without this arm a
        // circuit-open GB10 resolves to itself (`backends_for` sorts unhealthy
        // last but never drops it, and nothing else here filters on health), so
        // every request would fail `all LLM backends unavailable` with OpenRouter
        // sitting configured and able to serve.
        let all_down = backends.iter().all(|h| !h.is_healthy());
        // Gate on the SAME allowlist every other OpenRouter path in this
        // function uses (openrouter_serves, i.e. the model must be in
        // LUMID_LLM_OPENROUTER_MODEL_MAP) — not just "OpenRouter is
        // configured at all". Before this fix, a model with local backends
        // but NO map entry (qwen3-emb-0.6b/4b: on-prem-only by design, no
        // OpenRouter fallback ever existed for them) would still overflow
        // here on roof/health saturation, forwarding the bare LOCAL model id
        // to real OpenRouter via rewrite_model_for_openrouter's no-op
        // fallthrough (it returns the body unchanged when the map has no
        // entry) — an id OpenRouter was never asked to serve, so the request
        // either 404s from OpenRouter or, if a colliding id exists there,
        // becomes an unaccountable charge. Found 2026-09-01 auditing the
        // routing config after the GX10 tier bug. Same failure shape
        // `llm-0d342a8`/`85acad7` already closed for the UNKNOWN-model catch
        // -all; this closes it for the roof/health-overflow path too.
        let can_overflow = can_overflow_to_openrouter(
            &st.settings.llm_openrouter_model_map,
            &st.llm_pool.openrouter_url,
            model,
        );
        if (all_at_roof || all_down) && can_overflow {
            // Name the ACTUAL reason. "at concurrency roof" was printed for every
            // spill; a saturated pool and a dead pool are different incidents with
            // different remedies, and the operator reads this line first.
            let why = if all_down { "circuit-open (unhealthy)" } else { "at concurrency roof" };
            tracing::warn!(
                "llm resolve: model={:?} all {} local backends {} — overflowing to OpenRouter",
                model,
                backends.len(),
                why
            );
            return Ok(None); // caller will proxy to openrouter_url
        }
        return Ok(Some(backends));
    }
    // OPENROUTER-SERVED MODELS: a KNOWN id with no local backend (yet).
    //
    // A model listed in LUMID_LLM_OPENROUTER_MODEL_MAP is one an operator
    // explicitly configured, so serving it from OpenRouter is a deliberate
    // routing choice -- not the "a typo became money" catch-all that
    // llm-0d342a8 removed and 85acad7 removed again. The distinction that
    // matters is CONFIGURED vs UNRECOGNISED, not local vs remote: the map IS
    // the allowlist, and an id in neither LUMID_LLM_BACKENDS nor the map is
    // still refused below, with our own error.
    //
    // This is what lets a model be OpenRouter-only today and on-prem-first
    // tomorrow with NO code change: add it to LUMID_LLM_BACKENDS and the
    // branch above takes over automatically -- concurrency roof, queue roof
    // and the hedge all apply, with OpenRouter demoted from sole server to
    // bounded overflow. Claude ids can never reach here (refused at the top).
    if let Some(m) = model {
        if openrouter_serves(&st.settings.llm_openrouter_model_map, &st.llm_pool.openrouter_url, m) {
            // ROLE GATE (2026-09-01). A model with NO local backend is a pure
            // OpenRouter pass-through — every single request is a real charge,
            // with no on-prem "free at the margin" floor under it (unlike the
            // roof-overflow branch above, which only ever fires for a model
            // that ALSO has a local backend). Restricting this to admin+ is
            // the exact policy claude-proxy already enforces for its own
            // callers (denyExternalModelForRole); this closes the same door
            // for anyone calling lumid-llm directly. Refused, not silently
            // downgraded to a local backend — there is no local backend for
            // these ids to fall back to, and pretending otherwise would be
            // more confusing than an honest 403.
            if !role_may_use_metered_openrouter_model(caller_role) {
                tracing::warn!(
                    "llm resolve: model={m} DENIED role={caller_role} caller={caller} — \
                     metered OpenRouter-only model requires admin+"
                );
                return Err(ApiError::Forbidden(format!(
                    "model {m:?} requires admin access on this platform (metered, no local backend)"
                )));
            }
            // THIS BRANCH SPENDS MONEY AND USED TO SAY NOTHING.
            //
            // Only the roof-overflow branch above warned, so the logs described
            // the path that was NOT billing and stayed silent on the one that
            // was. On 2026-08-24 that cost an hour of archaeology: OpenRouter
            // spend continued at $0.20-0.43/hr through windows with zero
            // overflow and zero hedges, and nothing in this service could say
            // which model or whose request it was. The answer had to be
            // reconstructed from the edge nginx access log.
            //
            // INFO, not WARN: being served by OpenRouter is the CONFIGURED
            // behaviour for a mapped id with no local backend, not a fault.
            // Volume is bounded by real traffic (~200/h when this was written),
            // which is two orders of magnitude under the old RUST_LOG=debug.
            tracing::info!(
                "llm resolve: model={m} served BY OpenRouter (mapped, no local backend) \
                 — metered, caller={caller}"
            );
            return Ok(None); // caller will proxy to openrouter_url
        }
    }
    // UNKNOWN MODEL IDS ARE REFUSED, NEVER FORWARDED TO OPENROUTER.
    //
    // There used to be a catch-all here: "unknown explicit model -> OpenRouter".
    // It was removed in llm-0d342a8 after an e2e test caught it silently
    // forwarding a request for a NONEXISTENT model id to real OpenRouter and
    // being billed for it -- a typo became money, and an outage became an
    // invisible bill instead of an obvious error.
    //
    // It was then reintroduced by accident in 70fc036, which stopped
    // backends_for() falling back to the primary for a named unknown model and
    // so made this arm reachable again. Verified at the time by watching
    // `z-ai/glm-5.2` come back from OpenRouter (provider Baidu) -- which was a
    // reproduction of the billing hole, not a fix.
    //
    // OpenRouter is reached ONLY as a bounded overflow for a KNOWN, configured
    // model: the concurrency/queue roofs above, and the 90s hedge in
    // proxy_stream. Both require the model to be in the roster and in
    // LUMID_LLM_OPENROUTER_MODEL_MAP. An unrecognised id gets our own error, so
    // a real outage 503s honestly.
    if let Some(m) = model {
        return Err(ApiError::Unavailable(format!(
            "unknown model {m:?} — not in LUMID_LLM_BACKENDS nor LUMID_LLM_OPENROUTER_MODEL_MAP (unrecognised ids are never forwarded to OpenRouter)"
        )));
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

    // EXHAUSTED: every local backend failed at connect. Spill to OpenRouter
    // rather than returning the stored error.
    //
    // resolve() already routes here when `all_down`, but that is BREAKER-GATED:
    // a backend is only "down" after CIRCUIT_OPEN_AFTER (3) consecutive
    // failures, and on a SUDDEN total outage the handles are still flagged
    // healthy. So the first ~3 requests of a hard outage ran this loop, failed
    // every backend, and returned 502 while a healthy paid offload sat unused —
    // the exact defect llm-68bf556 set out to remove, surviving in the window
    // before the breakers latch. Measured 2026-08-26 when the relay died:
    // `upstream LLM unreachable` instead of a spill.
    //
    // Gated on openrouter_serves(), NOT merely on the URL being set, so the
    // allowlist property holds: a typo or an id we do not carry is still
    // refused with our own error and never forwarded to a metered upstream.
    if let Some(m) = body.and_then(model_of) {
        if openrouter_serves(
            &st.settings.llm_openrouter_model_map,
            &st.llm_pool.openrouter_url,
            &m,
        ) {
            tracing::warn!(
                "llm {method} {path}: all {} local backends failed at connect — spilling to OpenRouter (model={m})",
                backends.len()
            );
            let rewritten = body.map(|b| rewrite_model_for_openrouter(st, b));
            return proxy_json_openrouter(st, method, path, rewritten.as_ref()).await;
        }
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
    req = add_openrouter_auth(st, req);
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

/// Which side of a hedged stream won the race to the first data frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StreamSide {
    Local,
    Hedge,
}

/// Whether a raw SSE chunk carries a real `data:` frame, as opposed to only
/// keepalive comments or whitespace. This is what decides a hedge: a side that
/// has merely opened a connection has not answered.
fn chunk_has_data_frame(b: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(b) else {
        // Non-UTF8 bytes are real payload, not a keepalive comment.
        return true;
    };
    text.lines()
        .any(|l| l.starts_with("data:") && !l.trim_start_matches("data:").trim().is_empty())
}

/// Whether this request's model has an OpenRouter mapping. Hedging an unmapped
/// model would send a local-only id upstream and simply 404.
fn model_is_mapped(st: &AppState, body: &Value) -> bool {
    body.get("model")
        .and_then(|m| m.as_str())
        .map(|m| st.settings.llm_openrouter_model_map.contains_key(m))
        .unwrap_or(false)
}

async fn proxy_stream(
    st: &AppState,
    backends: &[std::sync::Arc<crate::llm_pool::BackendHandle>],
    path: &str,
    body: &Value,
) -> Response {
    let path_owned = path.to_string();
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

        // HEDGE SETUP. A cold prefill on a large context can take minutes while
        // the backend is perfectly healthy and nowhere near its roof, so neither
        // the health check nor the queue roof fires. Measured: a 250k-token turn
        // waited 180s (12 keepalives) before its first token.
        //
        // After `hedge_after_s` with no DATA frame, issue the SAME request to
        // OpenRouter as well and forward whichever side produces a data frame
        // first. Deliberately a hedge and not a switch: the local request keeps
        // running, so if OpenRouter is unreachable or slower the local answer
        // still lands. The previous switch-style guard abandoned the local
        // backend, and when its fallback failed the turn came back as an empty
        // stream that poisoned the session transcript.
        //
        // Only keepalive COMMENTS have reached the client at the hedge point, so
        // adopting either side is invisible to it.
        let hedge_after = st.settings.llm_hedge_after_s;
        let hedge_plan = if hedge_after > 0 {
            let rewritten = rewrite_model_for_openrouter(st, body);
            let base = st.llm_pool.openrouter_url.clone();
            let key = if !st.settings.llm_openrouter_key.is_empty() {
                st.settings.llm_openrouter_key.clone()
            } else {
                st.settings.llm_api_key.clone()
            };
            // offload is only meaningful when the model is actually mapped —
            // sending a local-only id to OpenRouter would just 404.
            if base.is_empty() || !model_is_mapped(st, body) {
                None
            } else {
                Some((format!("{base}{path}"), key, rewritten, st.http_stream.clone()))
            }
        } else {
            None
        };

        tokio::spawn(async move {
            let _guard = guard; // holds inflight until stream ends
            let mut ka = interval(Duration::from_secs(KEEPALIVE_INTERVAL_S));
            ka.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ka.tick().await;
            let mut upstream = Box::pin(upstream.bytes_stream());

            // `hx_tx` is an Option so the parent can MOVE it into the hedge task
            // rather than clone it. Cloning left a live sender owned by this task
            // forever, which made `hx_rx.recv()` in the drain arm below never
            // return None (and `hx_rx.is_closed()` permanently false): the drain
            // hung for good, holding `_guard` and leaking an in-flight slot on
            // every hedged turn. That is the fast leak -- one per hedge, and under
            // saturation OpenRouter wins essentially every hedge.
            let (hx_tx, mut hx_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            let mut hx_tx = Some(hx_tx);
            let mut hedge_started = false;
            let mut winner: Option<StreamSide> = None;
            let hedge_at = tokio::time::sleep(Duration::from_secs(hedge_after.max(1)));
            tokio::pin!(hedge_at);

            loop {
                tokio::select! {
                    biased;
                    chunk = upstream.next() => {
                        match chunk {
                            Some(Ok(b)) => {
                                if winner.is_none() && chunk_has_data_frame(&b) {
                                    winner = Some(StreamSide::Local);
                                }
                                if winner != Some(StreamSide::Hedge) && tx.send(b).is_err() {
                                    break;
                                }
                            }
                            // Local ended. If a hedge is still in flight and has
                            // not lost, keep the turn alive and let it answer.
                            _ => {
                                if winner == Some(StreamSide::Local) || !hedge_started {
                                    break;
                                }
                                if hx_rx.is_closed() && hx_rx.is_empty() {
                                    break;
                                }
                                // Drain the hedge to completion.
                                while let Some(b) = hx_rx.recv().await {
                                    if tx.send(b).is_err() {
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                    Some(b) = hx_rx.recv(), if hedge_started => {
                        if winner.is_none() && chunk_has_data_frame(&b) {
                            winner = Some(StreamSide::Hedge);
                            tracing::warn!(
                                "llm stream {path_owned}: OpenRouter hedge answered first after {hedge_after}s — local prefill still running"
                            );
                        }
                        if winner == Some(StreamSide::Hedge) && tx.send(b).is_err() {
                            break;
                        }
                    }
                    // `hedge_plan.is_some()` is part of the GUARD, not just the body.
                    // Without it the timer still armed (`hedge_after.max(1)`, so 1s
                    // even when hedging is disabled) and set `hedge_started` with no
                    // hedge task ever spawned -- arming the drain arm below for a
                    // hedge that could never answer.
                    _ = &mut hedge_at, if !hedge_started && winner.is_none() && hedge_plan.is_some() => {
                        hedge_started = true;
                        if let (Some(hx_tx), Some((url, key, body, http))) = (hx_tx.take(), hedge_plan.clone()) {
                            tracing::warn!(
                                "llm stream {path_owned}: no data from local backend in {hedge_after}s — hedging to OpenRouter"
                            );
                            tokio::spawn(async move {
                                let req = http.post(&url).bearer_auth(&key).json(&body);
                                match req.send().await {
                                    Ok(r) if r.status().as_u16() < 400 => {
                                        let mut s = Box::pin(r.bytes_stream());
                                        while let Some(Ok(b)) = s.next().await {
                                            if hx_tx.send(b).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    Ok(r) => tracing::warn!("llm hedge to OpenRouter returned HTTP {}", r.status()),
                                    Err(e) => tracing::warn!("llm hedge to OpenRouter failed: {e}"),
                                }
                            });
                        }
                    }
                    _ = ka.tick() => {
                        // LIVENESS MUST BE PROBED ON EVERY TICK.
                        //
                        // This check used to live inside `winner.is_none()`. Once a data
                        // frame arrived (`winner = Some(Local)`) the send was skipped, so
                        // `tx.send(..).is_err()` -- the ONLY way this loop notices a
                        // disconnected client on a quiet stream -- stopped being evaluated.
                        // A local upstream that stalled after its first frame then left
                        // this task alive forever holding the InFlightGuard, pinning
                        // `inflight`. Six leaked slots reached `max_concurrency` and every
                        // later request overflowed to metered OpenRouter while the GB10 sat
                        // idle -- ~$40/day, and it never self-healed without a restart
                        // (2026-08-24). Chatbox children reaped mid-turn are the source.
                        if tx.is_closed() {
                            break;
                        }
                        if winner.is_none() && tx.send(Bytes::from_static(KEEPALIVE_FRAME)).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
        });
        return sse_response(Body::from_stream(stream));
    }

    // All backends failed at connect. Same breaker-gate gap as proxy_json: on a
    // SUDDEN outage the handles are still flagged healthy, so resolve()'s
    // `all_down` arm has not fired yet and we would otherwise emit a one-frame
    // error stream while a healthy offload sits unused. Allowlist-gated, so an
    // unknown id is still refused rather than billed.
    if openrouter_serves(
        &st.settings.llm_openrouter_model_map,
        &st.llm_pool.openrouter_url,
        &model_of(body).unwrap_or_default(),
    ) {
        tracing::warn!(
            "llm stream {path}: all {} local backends failed at connect — spilling to OpenRouter",
            backends.len()
        );
        let rewritten = rewrite_model_for_openrouter(st, body);
        return proxy_stream_openrouter(st, path, &rewritten).await;
    }
    let frame = format!("data: {}\n\n", json!({ "error": "all LLM backends unavailable" }));
    sse_response(Body::from(frame))
}

async fn proxy_stream_openrouter(st: &AppState, path: &str, body: &Value) -> Response {
    let base = &st.llm_pool.openrouter_url;
    let url = format!("{base}{path}");
    let req = add_openrouter_auth(st, st.http_stream.post(&url).json(body));
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
        Ok(None) => match resolve(st, model_of(body).as_deref(), &caller_label(&ident), &caller_role(&ident)) {
            Err(e) => e.into_response(),
            Ok(None) => {
                let body = rewrite_model_for_openrouter(st, body);
                proxy_json_openrouter(st, method, path, Some(&body)).await
            }
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
        Ok(None) => match resolve(st, model_of(body).as_deref(), &caller_label(&ident), &caller_role(&ident)) {
            Err(e) => e.into_response(),
            Ok(None) => {
                let body = rewrite_model_for_openrouter(st, body);
                proxy_stream_openrouter(st, path, &body).await
            }
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
    // The OpenRouter leg is FILTERED to the configured map and relabelled to
    // the local id, rather than merged verbatim.
    //
    // Merging it verbatim advertised OpenRouter's entire catalog -- measured
    // 2026-08-23: 423 models, of which resolve() would actually serve ONE.
    // Every other id 503'd as an unknown model, so /v1/models was a menu of
    // things the gateway refuses to cook. A client must send the LOCAL id, so
    // that is what the catalog lists; the upstream metadata (context length,
    // pricing) is preserved by rewriting only the `id` field.
    let or_base = st.llm_pool.openrouter_url.trim_end_matches('/').to_string();
    let or_reverse: std::collections::HashMap<&str, &str> = st
        .settings
        .llm_openrouter_model_map
        .iter()
        .map(|(local, or)| (or.as_str(), local.as_str()))
        .collect();

    let mut data: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    for base in &bases {
        let is_or = !or_base.is_empty() && base.as_str() == or_base.as_str();
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
                let raw_id = m.get("id").and_then(|i| i.as_str()).unwrap_or("");
                // On the OpenRouter leg: skip anything not explicitly mapped,
                // and advertise the local id the client must actually send.
                let id = if is_or {
                    match or_reverse.get(raw_id) {
                        Some(local) => (*local).to_string(),
                        None => continue,
                    }
                } else {
                    raw_id.to_string()
                };
                if seen.insert(id.clone()) {
                    let mut m = m.clone();
                    if is_or {
                        if let Some(obj) = m.as_object_mut() {
                            obj.insert("id".into(), json!(id));
                        }
                    }
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
        Ok(b) => apply_thinking_control(apply_default_model(&st, b)),
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

#[cfg(test)]
mod hedge_tests {
    use super::*;

    // A hedge is decided by the first side to emit a real data frame. Keepalive
    // comments must NOT count: both sides open a connection immediately, so
    // treating a comment as an answer would always hand the race to whichever
    // side connected first — which is exactly the side we are trying to escape.
    #[test]
    fn keepalive_comments_do_not_win_the_race() {
        assert!(!chunk_has_data_frame(b": keep-alive\n\n"));
        assert!(!chunk_has_data_frame(b"\n\n"));
        assert!(!chunk_has_data_frame(b""));
        assert!(!chunk_has_data_frame(b": ping\n\n: ping\n\n"));
    }

    #[test]
    fn real_data_frames_win_the_race() {
        assert!(chunk_has_data_frame(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
        ));
        assert!(chunk_has_data_frame(b"data: [DONE]\n\n"));
        // Mixed chunk: a keepalive followed by a real frame still counts.
        assert!(chunk_has_data_frame(b": keep-alive\n\ndata: {\"x\":1}\n\n"));
    }

    // An empty `data:` line is a framing artifact, not an answer.
    #[test]
    fn empty_data_line_is_not_an_answer() {
        assert!(!chunk_has_data_frame(b"data:\n\n"));
        assert!(!chunk_has_data_frame(b"data: \n\n"));
    }

    // Binary payload is real content, not a comment.
    #[test]
    fn non_utf8_counts_as_data() {
        assert!(chunk_has_data_frame(&[0xff, 0xfe, 0x00]));
    }
}


#[cfg(test)]
mod openrouter_roster_tests {
    use super::{can_overflow_to_openrouter, openrouter_serves};
    use std::collections::HashMap;

    fn map() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("qwen/qwen3-coder".to_string(), "qwen/qwen3-coder".to_string());
        m.insert("deepseek-v4-flash".to_string(), "deepseek/deepseek-v4-flash-0731".to_string());
        m
    }

    // A configured model with no local backend is SERVED by OpenRouter. This is
    // what lets a model be OpenRouter-only today and on-prem-first later with no
    // code change -- adding it to LUMID_LLM_BACKENDS moves it to the local
    // branch of resolve() and demotes OpenRouter to bounded overflow.
    #[test]
    fn configured_model_is_served_by_openrouter() {
        assert!(openrouter_serves(&map(), "https://openrouter.ai/api", "qwen/qwen3-coder"));
    }

    // THE billing property. An id nobody configured must never reach a metered
    // upstream -- llm-0d342a8 closed exactly this hole after an e2e test caught
    // a nonexistent model being forwarded to real OpenRouter and billed, and
    // 70fc036 reopened it by accident. The map, not "is it unknown", is the gate.
    #[test]
    fn unconfigured_model_is_never_served() {
        for m in ["z-ai/glm-5.2", "qwen/qwen3-codr", "gpt-9", ""] {
            assert!(
                !openrouter_serves(&map(), "https://openrouter.ai/api", m),
                "unconfigured id {m:?} must not be routed to OpenRouter"
            );
        }
    }

    // With OpenRouter switched off, even a mapped model is not served -- the
    // request must fail honestly rather than silently find another route.
    #[test]
    fn no_openrouter_url_serves_nothing() {
        assert!(!openrouter_serves(&map(), "", "qwen/qwen3-coder"));
    }

    // Regression, found 2026-09-01 auditing routing config after the GX10
    // tier bug: the roof/health-overflow branch in resolve() used to check
    // only "!openrouter_url.is_empty()" -- not this same allowlist -- so a
    // model with local backends but NO map entry (qwen3-emb-0.6b/4b: on-prem
    // only by design) would still overflow to OpenRouter under its bare,
    // unmapped local id on roof saturation or a full outage.
    #[test]
    fn roof_overflow_requires_the_model_to_be_mapped() {
        // Mapped model: overflow permitted, same as the direct-dispatch path.
        assert!(can_overflow_to_openrouter(
            &map(),
            "https://openrouter.ai/api",
            Some("qwen/qwen3-coder")
        ));
        // A local-only backend with NO map entry (e.g. the embeddings ids)
        // must NEVER overflow, even with OpenRouter fully configured --
        // exactly the case that reached real OpenRouter under a local id
        // before this fix.
        assert!(!can_overflow_to_openrouter(
            &map(),
            "https://openrouter.ai/api",
            Some("qwen3-emb-0.6b")
        ));
    }

    // model==None must never overflow -- there's nothing to check against
    // the allowlist, so guessing is not an option. This is the ONE case
    // can_overflow_to_openrouter must decide without delegating to
    // openrouter_serves (which requires a &str).
    #[test]
    fn no_model_never_overflows() {
        assert!(!can_overflow_to_openrouter(&map(), "https://openrouter.ai/api", None));
    }
}

#[cfg(test)]
mod openrouter_role_gate_tests {
    use super::role_may_use_metered_openrouter_model;

    // Regression, found 2026-09-01 auditing OpenRouter spend: claude-proxy's
    // admin-only policy for metered non-self-hosted models
    // (denyExternalModelForRole) was enforced ONLY at the claude-proxy door.
    // Calling lumid-llm directly bypassed it entirely -- confirmed live, two
    // role=user accounts racking up real qwen/qwen3.6-27b charges with no
    // gate at all. This is the policy ported into lumid-llm itself.
    #[test]
    fn admin_and_super_admin_may_use_metered_models() {
        assert!(role_may_use_metered_openrouter_model("admin"));
        assert!(role_may_use_metered_openrouter_model("super_admin"));
    }

    #[test]
    fn plain_user_is_denied() {
        assert!(!role_may_use_metered_openrouter_model("user"));
    }

    // An empty/unrecognised role string must deny, not default-allow --
    // "unknown" must never be more privileged than "user".
    #[test]
    fn unknown_role_is_denied() {
        for role in ["", "guest", "banned", "USER", "Admin"] {
            assert!(
                !role_may_use_metered_openrouter_model(role),
                "role {role:?} must not be treated as admin -- exact match only, no case-folding"
            );
        }
    }

    // "local" (a caller authenticated via a local API key, e.g. this
    // service's own tooling or another in-cluster service) is treated as
    // at least as privileged as admin -- same convention as ingest.rs's
    // require_admin and blobs.rs's local-key bypass elsewhere in this crate.
    #[test]
    fn local_key_caller_is_treated_as_privileged() {
        assert!(role_may_use_metered_openrouter_model("local"));
    }
}

#[cfg(test)]
mod thinking_control_tests {
    use super::apply_thinking_control;
    use serde_json::json;

    fn ctk_thinking(v: &serde_json::Value) -> Option<&serde_json::Value> {
        v.get("chat_template_kwargs")?.get("thinking")
    }

    #[test]
    fn disabled_maps_to_the_knob_vllm_actually_reads() {
        let out = apply_thinking_control(json!({
            "model": "deepseek-v4-flash",
            "thinking": {"type": "disabled"},
        }));
        assert_eq!(ctk_thinking(&out), Some(&json!(false)));
        // Left in place: OpenRouter and Anthropic honour it natively.
        assert_eq!(out["thinking"]["type"], "disabled");
    }

    #[test]
    fn enabled_is_left_alone() {
        let out = apply_thinking_control(json!({
            "thinking": {"type": "enabled", "budget_tokens": 4096},
        }));
        assert!(out.get("chat_template_kwargs").is_none());
        assert_eq!(out["thinking"]["budget_tokens"], 4096);
    }

    #[test]
    fn absent_thinking_changes_nothing() {
        let out = apply_thinking_control(json!({"model": "m", "max_tokens": 8}));
        assert!(out.get("chat_template_kwargs").is_none());
    }

    // The backend-level control is more specific than the alias, so a caller who
    // set it deliberately must not be overridden.
    #[test]
    fn explicit_chat_template_kwargs_wins() {
        let out = apply_thinking_control(json!({
            "thinking": {"type": "disabled"},
            "chat_template_kwargs": {"thinking": true},
        }));
        assert_eq!(ctk_thinking(&out), Some(&json!(true)));
    }

    // Merging into an existing map must not drop the caller's other keys.
    #[test]
    fn other_chat_template_kwargs_are_preserved() {
        let out = apply_thinking_control(json!({
            "thinking": {"type": "disabled"},
            "chat_template_kwargs": {"custom_flag": "keep-me"},
        }));
        assert_eq!(ctk_thinking(&out), Some(&json!(false)));
        assert_eq!(out["chat_template_kwargs"]["custom_flag"], "keep-me");
    }

    #[test]
    fn malformed_thinking_is_ignored_not_fatal() {
        for bad in [json!({"thinking": "disabled"}), json!({"thinking": null}), json!({"thinking": {}})] {
            let out = apply_thinking_control(bad);
            assert!(out.get("chat_template_kwargs").is_none());
        }
    }
}
