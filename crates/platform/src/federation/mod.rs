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
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use moka::future::Cache;

use crate::auth::Identity;
use crate::config::Peer;
use crate::state::AppState;

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
        match self.forward_parts(peer, method, path, query, body, origin).await {
            Ok((status, headers, bytes)) => {
                let mut response = Response::builder().status(status);
                if let Some(h) = response.headers_mut() {
                    *h = headers;
                }
                response
                    .body(Body::from(bytes))
                    .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
            }
            Err(resp) => resp,
        }
    }

    /// Like [`forward`], but returns the peer response as decomposed parts
    /// `(status, relay-headers, body_bytes)` on success — so a caller (the shadow
    /// catch-all middleware) can inspect the status and cache the body before
    /// reconstructing a `Response`. On transport / body-read failure returns
    /// `Err(Response)` carrying a `502 Bad Gateway` ready to relay.
    pub async fn forward_parts(
        &self,
        peer: &Peer,
        method: reqwest::Method,
        path: &str,
        query: Option<&str>,
        body: Vec<u8>,
        origin: &OriginIdentity,
    ) -> Result<(StatusCode, HeaderMap, bytes::Bytes), Response> {
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
                return Err(bad_gateway());
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
                return Err(bad_gateway());
            }
        };
        Ok((status, out_headers, bytes))
    }
}

/// A ready-to-relay `502 Bad Gateway` for a federation transport failure.
fn bad_gateway() -> Response {
    (
        StatusCode::BAD_GATEWAY,
        axum::Json(serde_json::json!({ "detail": "federation peer unreachable" })),
    )
        .into_response()
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

// ─────────────────────────────────────────── shadow catch-all forward
//
// A **shadow** instance owns no data: it transparently forwards EVERY data
// request to its configured peer (the on-prem primary) and caches the result,
// so it is a drop-in for the public API. This middleware supersedes the F1
// per-read forward in `read/exec.rs` for shadow instances — it short-circuits
// BEFORE any local route handler runs, so ext-handler routes (e.g.
// `/lqt/markets`, `/prediction-markets/*`) and path-param declaratives that
// would otherwise hit the empty local DB and 500 are served from the peer too.
//
// Active only in shadow mode: `settings.read_federate` names a configured peer.
// Otherwise it is a pure passthrough (`next.run(req)`), byte-identical to a
// non-shadow instance. Only `GET`/`HEAD` are forwarded (the shadow owns no
// ingest — writes stay local / 404). A small local-only allowlist is never
// forwarded.

/// Path prefixes / exact paths served LOCALLY even in shadow mode (never
/// forwarded to the peer): liveness/metadata + the LLM plane (handled by the
/// existing `llm_federate` path in `handlers/llm.rs` — must not be
/// double-handled here).
const LOCAL_ONLY_EXACT: &[&str] = &["/health", "/metrics", "/openapi.json"];
const LOCAL_ONLY_PREFIX: &[&str] = &[
    "/health/", // /health/db, /health/ready
    "/v1/",     // LLM reverse proxy (its own federation switch)
];

/// True when `path` must be served locally (not forwarded) even in shadow mode.
fn is_local_only(path: &str) -> bool {
    if LOCAL_ONLY_EXACT.contains(&path) || path == "/v1" {
        return true;
    }
    LOCAL_ONLY_PREFIX.iter().any(|pfx| path.starts_with(pfx))
}

/// A memoized peer response for a forwarded `GET`: the parts needed to rebuild a
/// `Response` on a cache hit. Only 200 responses are stored.
#[derive(Clone)]
struct CachedForward {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

/// Byte-weighted, per-entry-TTL moka cache of forwarded `GET` 200 responses,
/// keyed by `METHOD path?query`. Mirrors the `read/cache.rs` L1 style (weigher +
/// `time_to_live`) but single-tier (no Redis/invalidation — the shadow's whole
/// dataset is remote and short-TTL is the freshness contract).
pub struct ShadowCache {
    l1: Cache<String, Arc<CachedForward>>,
    ttl: Duration,
}

impl ShadowCache {
    /// Build with a per-entry `ttl` (from `Settings.shadow_cache_ttl_s`). Capped
    /// at 128 MiB total body bytes (LRU eviction under the cap).
    pub fn new(ttl: Duration) -> Arc<Self> {
        let l1 = Cache::builder()
            .max_capacity(128 * 1024 * 1024)
            .weigher(|_k: &String, v: &Arc<CachedForward>| {
                v.body.len().min(u32::MAX as usize) as u32
            })
            .time_to_live(ttl)
            .build();
        Arc::new(Self { l1, ttl })
    }

    fn key(method: &Method, path: &str, query: Option<&str>) -> String {
        match query {
            Some(q) if !q.is_empty() => format!("{method} {path}?{q}"),
            _ => format!("{method} {path}"),
        }
    }

    async fn get(&self, key: &str) -> Option<Arc<CachedForward>> {
        self.l1.get(key).await
    }

    async fn put(&self, key: String, val: Arc<CachedForward>) {
        self.l1.insert(key, val).await;
    }

    /// Test/introspection helper: current entry count (after pending tasks run).
    pub async fn entry_count(&self) -> u64 {
        self.l1.run_pending_tasks().await;
        self.l1.entry_count()
    }

    /// TTL these entries were built with.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

/// Rebuild a `Response` from a `CachedForward` (cache hit) or fresh peer parts.
fn response_from_parts(status: StatusCode, headers: &HeaderMap, body: Bytes) -> Response {
    let mut resp = Response::builder().status(status);
    if let Some(h) = resp.headers_mut() {
        *h = headers.clone();
    }
    resp.body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// Axum middleware: the shadow catch-all forward. Applied to the main router so
/// it runs AFTER auth (the `Identity` extension is present) but before route
/// handlers. Passthrough (`next.run(req)`) whenever not in shadow mode, so a
/// non-shadow instance is unchanged.
pub async fn shadow_forward(State(st): State<AppState>, req: Request, next: Next) -> Response {
    // Not shadowing (no read_federate) or the peer id doesn't resolve → local.
    let peer = match st.settings.read_federate.as_deref() {
        None => return next.run(req).await,
        Some(pid) => match st.federation.peer(pid) {
            Some(p) => p.clone(),
            None => return next.run(req).await,
        },
    };

    // Only GET/HEAD are forwarded; everything else (writes) stays local.
    let method = req.method().clone();
    if method != Method::GET && method != Method::HEAD {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    if is_local_only(&path) {
        return next.run(req).await;
    }
    let query = req.uri().query().map(str::to_string);

    // Origin identity (from the gated `Identity` extension) → attribution headers.
    let origin = req
        .extensions()
        .get::<Identity>()
        .map(|i| OriginIdentity { sub: i.sub.clone(), role: i.role.clone() })
        .unwrap_or_default();

    // Cache: serve a warm GET 200 without a peer round-trip. HEAD is never
    // cached (no body) but is still forwarded.
    let cache_key = ShadowCache::key(&method, &path, query.as_deref());
    if method == Method::GET {
        if let Some(hit) = st.shadow_cache.get(&cache_key).await {
            return response_from_parts(hit.status, &hit.headers, hit.body.clone());
        }
    }

    // Forward to the peer's identical endpoint (GET/HEAD → no request body).
    let (status, headers, body) = match st
        .federation
        .forward_parts(&peer, method.clone(), &path, query.as_deref(), Vec::new(), &origin)
        .await
    {
        Ok(parts) => parts,
        Err(resp) => return resp, // 502, already a Response
    };

    // Cache only successful GETs; errors and non-200 are never memoized.
    if method == Method::GET && status == StatusCode::OK {
        st.shadow_cache
            .put(
                cache_key,
                Arc::new(CachedForward {
                    status,
                    headers: headers.clone(),
                    body: body.clone(),
                }),
            )
            .await;
    }

    response_from_parts(status, &headers, body)
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

    #[test]
    fn local_only_allowlist_covers_health_metrics_openapi_and_v1() {
        assert!(is_local_only("/health"));
        assert!(is_local_only("/health/db"));
        assert!(is_local_only("/health/ready"));
        assert!(is_local_only("/metrics"));
        assert!(is_local_only("/openapi.json"));
        assert!(is_local_only("/v1"));
        assert!(is_local_only("/v1/chat/completions"));
        assert!(is_local_only("/v1/models"));
    }

    #[test]
    fn local_only_allowlist_does_not_catch_data_routes() {
        assert!(!is_local_only("/lqt/markets"));
        assert!(!is_local_only("/prediction-markets/foo"));
        assert!(!is_local_only("/analyst-estimates/AAPL"));
        assert!(!is_local_only("/dividends/AAPL"));
        assert!(!is_local_only("/fundamentals/AAPL"));
        // A route that merely *contains* v1 but isn't the /v1 plane is forwarded.
        assert!(!is_local_only("/v1beta/whatever"));
        assert!(!is_local_only("/healthz"));
    }

    #[tokio::test]
    async fn shadow_cache_key_includes_method_path_query() {
        assert_eq!(
            ShadowCache::key(&Method::GET, "/x/y", Some("a=1")),
            "GET /x/y?a=1"
        );
        assert_eq!(ShadowCache::key(&Method::GET, "/x/y", None), "GET /x/y");
        assert_eq!(ShadowCache::key(&Method::GET, "/x/y", Some("")), "GET /x/y");
        assert_eq!(ShadowCache::key(&Method::HEAD, "/x", None), "HEAD /x");
    }
}
