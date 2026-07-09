//! Federation client — the F1 mesh-core peer-forward plane.
//!
//! A `lumid-data` instance can forward a request it doesn't serve locally to a
//! **peer** (another instance) and relay the peer's response verbatim. This is
//! the transport under the read-forward (`read/exec.rs`) and LLM-forward
//! (`handlers/llm.rs`) default routes.
//!
//! The forwarder calls the peer's **identical endpoint** (same method + path +
//! query) — reusing the peer's public HTTP API as the remote-exec surface, so
//! there is no new SQL-over-wire protocol. It authenticates with the peer's
//! bearer (`Peer::token`, a local key on the peer) and propagates the ORIGIN
//! identity via `X-Lumid-Origin-Sub` / `X-Lumid-Origin-Role` + the app tag
//! `X-Lumid-App`, so authz + separation can hold end-to-end once F3 enforces
//! them. MVP: the header contract is laid; no cross-hop RBAC is enforced yet.
//!
//! F2 (catalog routing) and F3 (parent/child hierarchy, hop-guard, full
//! separation) build on this — the interface is intentionally minimal but
//! extensible (peer registry + a single verbatim forward).

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::config::Peer;

/// Header names carrying the caller's identity + app across a federation hop.
pub const HDR_ORIGIN_SUB: &str = "x-lumid-origin-sub";
pub const HDR_ORIGIN_ROLE: &str = "x-lumid-origin-role";
pub const HDR_APP: &str = "x-lumid-app";
/// This instance's id, stamped on every hop (loop-detection groundwork for F3).
pub const HDR_VIA: &str = "x-lumid-via";

/// The origin identity propagated to a peer on a forwarded request. A minimal
/// `(sub, role)` — enough for the peer to attribute + (later) authorize the
/// call. Cloned from the gated `auth::Identity` at the forward site.
#[derive(Clone, Debug, Default)]
pub struct OriginIdentity {
    pub sub: String,
    pub role: String,
}

/// Federation client: the peer registry + a shared HTTP client + this
/// instance's identity, held in `AppState`. Cheap to clone (the reqwest client
/// is internally ref-counted; the peer list is small and shared).
#[derive(Clone)]
pub struct Federation {
    peers: HashMap<String, Peer>,
    http: reqwest::Client,
    instance_id: String,
    app_id: String,
}

impl Federation {
    /// Build from settings-derived peers + a reused reqwest client (share the
    /// platform's `http` client so connection pools/timeouts are consistent).
    pub fn new(
        peers: &[Peer],
        http: reqwest::Client,
        instance_id: String,
        app_id: String,
    ) -> Self {
        let peers = peers
            .iter()
            .map(|p| (p.id.clone(), p.clone()))
            .collect::<HashMap<_, _>>();
        Self { peers, http, instance_id, app_id }
    }

    /// Look up a peer by id.
    pub fn peer(&self, id: &str) -> Option<&Peer> {
        self.peers.get(id)
    }

    /// Whether any peers are configured.
    pub fn has_peers(&self) -> bool {
        !self.peers.is_empty()
    }

    /// Forward a request to `peer`'s identical endpoint and relay its response
    /// verbatim: status, body, and the response headers that matter for
    /// caching/relay (`Content-Type`, `ETag`, `Cache-Control`). The peer bearer
    /// authenticates the hop; the origin identity + app id ride as
    /// `X-Lumid-Origin-*` / `X-Lumid-App` headers.
    ///
    /// `path` is the request path (must start with `/`); `query` is the raw
    /// query string (no leading `?`), if any; `body` is the raw request body
    /// (empty for GETs). On transport failure a `502 Bad Gateway` is returned.
    pub async fn forward(
        &self,
        peer: &Peer,
        method: reqwest::Method,
        path: &str,
        query: Option<&str>,
        body: Vec<u8>,
        origin: &OriginIdentity,
    ) -> Response {
        let mut url = format!("{}{}", peer.base_url, path);
        if let Some(q) = query {
            if !q.is_empty() {
                url.push('?');
                url.push_str(q);
            }
        }

        let mut req = self.http.request(method.clone(), &url);
        if !peer.token.is_empty() {
            req = req.header(header::AUTHORIZATION, format!("Bearer {}", peer.token));
        }
        req = req
            .header(HDR_ORIGIN_SUB, sanitize_header(&origin.sub))
            .header(HDR_ORIGIN_ROLE, sanitize_header(&origin.role))
            .header(HDR_APP, sanitize_header(&self.app_id))
            .header(HDR_VIA, sanitize_header(&self.instance_id));
        if !body.is_empty() {
            req = req
                .header(header::CONTENT_TYPE, "application/json")
                .body(body);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("federation forward to {} ({url}) failed: {e}", peer.id);
                return (
                    StatusCode::BAD_GATEWAY,
                    axum::Json(serde_json::json!({
                        "detail": "federation peer unreachable"
                    })),
                )
                    .into_response();
            }
        };

        let status = StatusCode::from_u16(resp.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);
        // Preserve the relay-relevant response headers before consuming the body.
        let mut out_headers = HeaderMap::new();
        for name in [header::CONTENT_TYPE, header::ETAG, header::CACHE_CONTROL] {
            if let Some(v) = resp.headers().get(&name) {
                out_headers.insert(name, v.clone());
            }
        }
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("federation read body from {} failed: {e}", peer.id);
                return (
                    StatusCode::BAD_GATEWAY,
                    axum::Json(serde_json::json!({
                        "detail": "federation peer unreachable"
                    })),
                )
                    .into_response();
            }
        };

        let mut response = Response::builder().status(status);
        if let Some(h) = response.headers_mut() {
            *h = out_headers;
        }
        response
            .body(Body::from(bytes))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
    }
}

/// Strip control chars that would make a header value invalid; empty → a single
/// space so `HeaderValue::from_str` never fails on a legitimate-but-empty field.
fn sanitize_header(s: &str) -> HeaderValue {
    let cleaned: String = s
        .chars()
        .filter(|c| !c.is_control())
        .take(1024)
        .collect();
    HeaderValue::from_str(&cleaned).unwrap_or_else(|_| HeaderValue::from_static(""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fed() -> Federation {
        Federation::new(
            &[
                Peer {
                    id: "primary".into(),
                    base_url: "http://primary:8088".into(),
                    token: "t".into(),
                },
            ],
            reqwest::Client::new(),
            "shadow".into(),
            "findata".into(),
        )
    }

    #[test]
    fn peer_lookup() {
        let f = fed();
        assert!(f.peer("primary").is_some());
        assert!(f.peer("nope").is_none());
        assert!(f.has_peers());
    }

    #[test]
    fn empty_registry_has_no_peers() {
        let f = Federation::new(&[], reqwest::Client::new(), "x".into(), "findata".into());
        assert!(!f.has_peers());
        assert!(f.peer("primary").is_none());
    }

    #[test]
    fn sanitize_strips_control_chars() {
        let v = sanitize_header("abc\r\ndef");
        assert_eq!(v.to_str().unwrap(), "abcdef");
    }
}
