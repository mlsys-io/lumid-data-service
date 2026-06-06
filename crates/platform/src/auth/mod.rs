//! Auth + rate-limit gate — port of `api/auth.py`.
//!
//! Token sources (in order): local env keys (`LUMID_API_KEYS`), then Lumid
//! introspection. The `gate` middleware requires an identity (bite #38: all
//! data routes need a PAT/local key; only `/health` is public), then applies
//! the tiered rate limit. Resolved `Identity` is inserted into request
//! extensions for downstream handlers (ACL on writes, audit).

pub mod lumid;
pub mod ratelimit;

use std::collections::HashMap;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;

use crate::error::ApiError;
use crate::state::AppState;

pub use lumid::Identity;
use lumid::LumidError;

/// Parse `LUMID_API_KEYS=key:label,key:label` → {key: label}.
pub fn parse_local_keys(raw: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for chunk in raw.split(',') {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        match chunk.split_once(':') {
            Some((k, label)) => out.insert(k.trim().to_string(), label.trim().to_string()),
            None => out.insert(chunk.to_string(), "unnamed".to_string()),
        };
    }
    out
}

fn extract_token(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(k) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        let k = k.trim();
        if !k.is_empty() {
            return Some(k.to_string());
        }
    }
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some((scheme, token)) = auth.split_once(' ') {
            if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() {
                return Some(token.trim().to_string());
            }
        }
    }
    None
}

/// Extract a bearer token for the self-authenticating WebSocket / SSE realtime
/// routes (which authenticate outside the `gate`). Superset of the gate's
/// header check: Authorization / x-api-key plus `Sec-WebSocket-Protocol:
/// bearer.<tok>` (the only way a browser WebSocket can pass a credential).
/// Lives in `auth` so the platform's WS handlers and app-layer realtime
/// handlers share one extractor.
pub fn extract_ws_token(headers: &axum::http::HeaderMap) -> Option<String> {
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

fn client_ip(headers: &axum::http::HeaderMap) -> String {
    for h in ["x-forwarded-for", "x-real-ip"] {
        if let Some(v) = headers.get(h).and_then(|v| v.to_str().ok()) {
            if let Some(first) = v.split(',').next() {
                let first = first.trim();
                if !first.is_empty() {
                    return first.to_string();
                }
            }
        }
    }
    "unknown".to_string()
}

/// Resolve an Identity from headers. Ok(Some)=authed, Ok(None)=anonymous,
/// Err=invalid token (401) or auth service unreachable (503).
async fn resolve_identity(
    st: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<Option<Identity>, ApiError> {
    let token = match extract_token(headers) {
        Some(t) => t,
        None => return Ok(None),
    };
    if let Some(label) = st.local_keys.get(&token) {
        return Ok(Some(Identity {
            sub: format!("local:{label}"),
            role: "local".to_string(),
            email: None,
            active: true,
            scopes: Vec::new(),
        }));
    }
    match st.lumid.introspect(&token).await {
        Ok(Some(id)) => Ok(Some(id)),
        Ok(None) => Err(ApiError::Unauthorized("invalid or unknown token".into())),
        Err(LumidError::Unreachable(e)) => {
            tracing::warn!("lumid unreachable while validating bearer: {e}");
            Err(ApiError::Unavailable("auth service unreachable".into()))
        }
    }
}

/// Resolve an Identity from a bare token (no headers). Used by the WebSocket
/// handlers, which extract the token themselves (Authorization / x-api-key /
/// `Sec-WebSocket-Protocol: bearer.<tok>`) and authenticate outside the `gate`
/// middleware. Mirrors `resolve_identity`'s local-key-then-Lumid order.
pub async fn resolve_bearer(
    st: &AppState,
    token: &str,
) -> Result<Option<Identity>, ApiError> {
    let token = token.trim();
    if token.is_empty() {
        return Ok(None);
    }
    if let Some(label) = st.local_keys.get(token) {
        return Ok(Some(Identity {
            sub: format!("local:{label}"),
            role: "local".to_string(),
            email: None,
            active: true,
            scopes: Vec::new(),
        }));
    }
    match st.lumid.introspect(token).await {
        Ok(Some(id)) => Ok(Some(id)),
        Ok(None) => Ok(None),
        Err(LumidError::Unreachable(e)) => {
            tracing::warn!("lumid unreachable while validating ws bearer: {e}");
            Err(ApiError::Unavailable("auth service unreachable".into()))
        }
    }
}

/// Middleware applied to the gated (data) router group. Requires identity and
/// rate-limits. `/health` is mounted outside this layer.
pub async fn gate(
    State(st): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let method = req.method().to_string();
    let tmpl = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let ident = resolve_identity(&st, &headers).await?;

    // Rate-limit key + tier (mirrors _rate_limit_key / _dynamic_limit).
    let key = match &ident {
        Some(id) => format!("id:{}", id.sub),
        None => {
            if headers.contains_key("authorization") || headers.contains_key("x-api-key") {
                format!("presented:{}", client_ip(&headers))
            } else {
                format!("ip:{}", client_ip(&headers))
            }
        }
    };
    let decision = st.rate.check(&key);
    let (rl_limit, rl_remaining, rl_reset) = (decision.limit, decision.remaining, decision.reset_s);
    if !decision.allowed {
        if let Some(c) = st.redis.clone() {
            let sub = ident.as_ref().map(|i| i.sub.clone()).unwrap_or_else(|| "anon".into());
            tokio::spawn(crate::handlers::usage::record(c, sub, method, tmpl, 429, 0));
        }
        return Err(ApiError::RateLimited {
            retry_after_s: decision.retry_after_s,
            limit: decision.limit_spec,
        });
    }

    // require_identity: anonymous is rejected on data routes.
    let ident = ident.ok_or_else(|| {
        ApiError::Unauthorized(
            "authentication required — present a Lumid PAT as 'Authorization: Bearer <token>'".into(),
        )
    })?;
    let sub = ident.sub.clone();
    req.extensions_mut().insert(ident);
    let mut resp = next.run(req).await;
    // Rate-limit headers so consumers can track headroom.
    let h = resp.headers_mut();
    if let Ok(v) = axum::http::HeaderValue::from_str(&rl_limit.to_string()) {
        h.insert("x-ratelimit-limit", v);
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(&rl_remaining.to_string()) {
        h.insert("x-ratelimit-remaining", v);
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(&rl_reset.to_string()) {
        h.insert("x-ratelimit-reset", v);
    }
    // Fire-and-forget usage recording (per-request global + per-sub counters).
    if let Some(c) = st.redis.clone() {
        let bytes = resp
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        tokio::spawn(crate::handlers::usage::record(c, sub, method, tmpl, resp.status().as_u16(), bytes));
    }
    Ok(resp)
}
