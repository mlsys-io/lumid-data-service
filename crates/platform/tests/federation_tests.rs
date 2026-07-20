//! Integration tests for the F1 federation mesh core.
//!
//! Stands up a stub "peer" HTTP server (a real in-process axum server) and
//! exercises the two invariants the read-forward path relies on:
//!
//!  1. `Federation::forward` calls the peer's identical endpoint, presenting the
//!     peer bearer + origin/app headers, and relays the peer's status + body.
//!  2. When the forward is wrapped in the read-cache (exactly as
//!     `read/exec.rs::run_spec` does — `CacheManager::get_or_compute` keyed on
//!     spec-id+params), a SECOND identical read is served from cache with NO
//!     second peer round-trip.
//!
//! No Postgres / no LLM: the federated read path never touches the DB pool.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::Router;

use lumid_platform::config::Peer;
use lumid_platform::federation::{Federation, OriginIdentity, HDR_APP, HDR_ORIGIN_SUB};
use lumid_platform::read::cache::{CacheKey, CacheManager, CachedBody};

/// Shared state for the stub peer: a hit counter + the last-seen headers, so a
/// test can assert both the number of round-trips and the propagated identity.
#[derive(Clone, Default)]
struct PeerState {
    hits: Arc<AtomicUsize>,
    last_auth: Arc<std::sync::Mutex<Option<String>>>,
    last_origin_sub: Arc<std::sync::Mutex<Option<String>>>,
    last_app: Arc<std::sync::Mutex<Option<String>>>,
}

async fn peer_handler(State(st): State<PeerState>, headers: HeaderMap) -> axum::Json<serde_json::Value> {
    st.hits.fetch_add(1, Ordering::SeqCst);
    let get = |h: &str| headers.get(h).and_then(|v| v.to_str().ok()).map(str::to_string);
    *st.last_auth.lock().unwrap() = get("authorization");
    *st.last_origin_sub.lock().unwrap() = get(HDR_ORIGIN_SUB);
    *st.last_app.lock().unwrap() = get(HDR_APP);
    axum::Json(serde_json::json!({ "ok": true, "rows": [1, 2, 3] }))
}

async fn peer_notfound() -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "detail": "no such key" })),
    )
}

/// Spin up the stub peer on an ephemeral port; return (base_url, state).
async fn spawn_peer() -> (String, PeerState) {
    let state = PeerState::default();
    let app = Router::new()
        .route("/fundamentals/:key", get(peer_handler))
        .route("/missing/:key", get(peer_notfound))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

fn federation_to(base_url: &str, token: &str) -> Federation {
    Federation::new(
        &[Peer { id: "primary".into(), base_url: base_url.into(), token: token.into() }],
        reqwest::Client::new(),
        "shadow".into(),
        "findata".into(),
    )
}

#[tokio::test]
async fn forward_reaches_peer_with_bearer_and_origin_headers() {
    let (base, peer_state) = spawn_peer().await;
    let fed = federation_to(&base, "peer-token-xyz");
    let peer = fed.peer("primary").unwrap().clone();
    let origin = OriginIdentity { sub: "local:tester".into(), role: "local".into() };

    let resp = fed
        .forward(
            &peer,
            reqwest::Method::GET,
            "/fundamentals/AAPL",
            None,
            Vec::new(),
            &origin,
        )
        .await;

    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(peer_state.hits.load(Ordering::SeqCst), 1);
    // Peer bearer presented.
    assert_eq!(
        peer_state.last_auth.lock().unwrap().as_deref(),
        Some("Bearer peer-token-xyz")
    );
    // Origin identity + app tag propagated.
    assert_eq!(
        peer_state.last_origin_sub.lock().unwrap().as_deref(),
        Some("local:tester")
    );
    assert_eq!(peer_state.last_app.lock().unwrap().as_deref(), Some("findata"));

    // Body relayed verbatim.
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["ok"], serde_json::json!(true));
}

/// The core read-forward invariant: wrap the forward in the read-cache (as
/// `run_spec` does) and assert the second identical read is a cache hit — no
/// second peer round-trip.
#[tokio::test]
async fn cached_read_forward_hits_peer_only_once() {
    let (base, peer_state) = spawn_peer().await;
    let fed = Arc::new(federation_to(&base, "tok"));
    let peer = fed.peer("primary").unwrap().clone();
    let origin = OriginIdentity { sub: "s".into(), role: "r".into() };

    let cache = CacheManager::new(
        16 * 1024 * 1024,
        Duration::from_secs(3600),
        None, // no L2 redis in the test
        HashMap::new(),
    );

    // The compute closure mirrors `produce_federated`: forward → body bytes.
    let compute = |fed: Arc<Federation>, peer: Peer, origin: OriginIdentity| async move {
        let resp = fed
            .forward(&peer, reqwest::Method::GET, "/fundamentals/AAPL", None, Vec::new(), &origin)
            .await;
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        Ok::<Vec<u8>, lumid_platform::error::ApiError>(bytes.to_vec())
    };

    let id: Arc<str> = Arc::from("fundamentals");
    let ttl = Duration::from_secs(3600);
    let key1 = CacheKey::new(id.clone(), 0, "key=AAPL".to_string());
    let key2 = CacheKey::new(id.clone(), 0, "key=AAPL".to_string());

    // First read → cold → forwards to peer.
    let b1: Arc<CachedBody> = cache
        .get_or_compute(key1, ttl, false, || compute(fed.clone(), peer.clone(), origin.clone()))
        .await
        .unwrap();
    assert_eq!(peer_state.hits.load(Ordering::SeqCst), 1);

    // Second identical read → warm → served from cache, NO peer round-trip.
    let b2: Arc<CachedBody> = cache
        .get_or_compute(key2, ttl, false, || compute(fed.clone(), peer.clone(), origin.clone()))
        .await
        .unwrap();
    assert_eq!(
        peer_state.hits.load(Ordering::SeqCst),
        1,
        "cache hit must not re-hit the peer"
    );

    // Both reads return the same bytes + ETag.
    assert_eq!(b1.bytes, b2.bytes);
    assert_eq!(b1.etag, b2.etag);

    // A DIFFERENT params key is a fresh miss → a second peer round-trip.
    let key3 = CacheKey::new(id.clone(), 0, "key=MSFT".to_string());
    let _ = cache
        .get_or_compute(key3, ttl, false, || compute(fed.clone(), peer.clone(), origin.clone()))
        .await
        .unwrap();
    assert_eq!(peer_state.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn forward_relays_peer_error_status() {
    let (base, _peer_state) = spawn_peer().await;
    let fed = federation_to(&base, "tok");
    let peer = fed.peer("primary").unwrap().clone();

    let resp = fed
        .forward(
            &peer,
            reqwest::Method::GET,
            "/missing/AAPL",
            None,
            Vec::new(),
            &OriginIdentity::default(),
        )
        .await;
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn forward_to_unreachable_peer_is_bad_gateway() {
    // Point at a closed port (nothing listening).
    let fed = federation_to("http://127.0.0.1:1", "tok");
    let peer = fed.peer("primary").unwrap().clone();
    let resp = fed
        .forward(
            &peer,
            reqwest::Method::GET,
            "/fundamentals/AAPL",
            None,
            Vec::new(),
            &OriginIdentity::default(),
        )
        .await;
    assert_eq!(resp.status(), axum::http::StatusCode::BAD_GATEWAY);
}

// ─────────────────────────────────── shadow catch-all forward (middleware)
//
// Exercises `federation::shadow_forward` end-to-end through a real axum router
// built with a full `AppState`. The DB pool is built lazily (deadpool defers
// connecting), and the middleware short-circuits BEFORE any handler touches it,
// so no live Postgres is needed. The stub peer stands in for the on-prem primary.

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use lumid_platform::config::Settings;
use lumid_platform::federation::{shadow_forward, ShadowCache};
use lumid_platform::state::AppState;
use tower::ServiceExt; // for `oneshot`

/// Build an `AppState` whose `read_federate` optionally points at `peer_base`.
/// Everything is real; the pool is lazy (never connected in these tests).
fn app_state_shadow(peer_base: Option<&str>) -> AppState {
    use std::sync::Arc;

    let mut settings = Settings::from_env();
    // Deterministic — don't inherit the test host's env.
    settings.lumid_enabled = false;
    settings.api_keys_raw = "test-key:tester".into();
    settings.rate_limit_anon = "100000/minute".into();
    settings.rate_limit_authed = "100000/minute".into();
    settings.max_concurrency_per_key = 0;
    settings.max_concurrency_per_ip = 0;
    settings.shadow_cache_ttl_s = 30;
    settings.redis_url = String::new();
    settings.ch_url = String::new();
    settings.llm_backend_url = String::new();

    if let Some(base) = peer_base {
        settings.peers = vec![Peer {
            id: "primary".into(),
            base_url: base.into(),
            token: "peer-tok".into(),
        }];
        settings.read_federate = Some("primary".into());
    } else {
        settings.peers = Vec::new();
        settings.read_federate = None;
    }
    let settings = Arc::new(settings);

    let pool = lumid_platform::db::build_pool(&settings).unwrap();
    let lumid = Arc::new(lumid_platform::auth::lumid::LumidClient::new(&settings));
    let local_keys = Arc::new(lumid_platform::auth::parse_local_keys(&settings.api_keys_raw));
    let rate = Arc::new(lumid_platform::auth::ratelimit::RateLimiter::new(
        &settings.rate_limit_anon,
        &settings.rate_limit_authed,
    ));
    let read_cache = lumid_platform::read::cache::CacheManager::new(
        16 * 1024 * 1024,
        Duration::from_secs(3600),
        None,
        HashMap::new(),
    );
    let llm_pool = Arc::new(lumid_platform::llm_pool::BackendPool::from_settings(&settings));
    let backends = Arc::new(lumid_platform::backend::Registry::new_postgres_only(pool.clone()));
    let card_store = Arc::new(lumid_platform::retrieve::card_store::CardStore::new(
        pool.clone(),
        settings.retrieval_card_ttl_s,
        settings.retrieval_sample_rows,
    ));
    let federation = Arc::new(Federation::new(
        &settings.peers,
        reqwest::Client::new(),
        settings.instance_id.clone(),
        settings.app_id.clone(),
    ));
    let shadow_cache = ShadowCache::new(Duration::from_secs(settings.shadow_cache_ttl_s.max(1)));
    let blob_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let feed_liveness: Arc<dyn lumid_platform::realtime::FeedLiveness> =
        Arc::new(lumid_platform::realtime::DefaultFeedLiveness);

    AppState {
        pool,
        settings,
        lumid,
        local_keys,
        rate,
        concurrency: None,
        redis: None,
        redis_client: None,
        hub: None,
        read_cache,
        http: reqwest::Client::new(),
        http_stream: reqwest::Client::new(),
        llm_pool,
        blob_store,
        backends,
        feed_liveness,
        card_store,
        federation,
        shadow_cache,
    }
}

/// A router that mimics `app.rs`'s gated group: an `Identity` is injected (so the
/// middleware sees it) and the middleware is layered over a fallback handler that
/// stands in for the local routes. If the middleware forwards, the local handler
/// never runs; if it passes through, the local handler answers `599 LOCAL`.
fn shadow_router(state: AppState) -> Router {
    use axum::extract::Request;
    use axum::middleware::{from_fn, from_fn_with_state, Next};
    use axum::response::Response;
    use lumid_platform::auth::Identity;

    async fn inject_identity(mut req: Request, next: Next) -> Response {
        req.extensions_mut().insert(Identity {
            sub: "local:tester".into(),
            role: "local".into(),
            email: None,
            active: true,
            scopes: Vec::new(),
        });
        next.run(req).await
    }

    // Fallback local handler: a sentinel status that would never come from the
    // peer stub, so a test can tell "served locally" from "forwarded".
    async fn local_fallback() -> Response {
        Response::builder()
            .status(599)
            .body(Body::from("LOCAL"))
            .unwrap()
    }

    Router::new()
        .fallback(get(local_fallback))
        // /v1/* is a local route in the real app; add one so the allowlist test
        // can prove it is NOT forwarded.
        .route("/v1/models", get(local_fallback))
        .route("/health", get(local_fallback))
        .layer(from_fn_with_state(state.clone(), shadow_forward))
        .layer(from_fn(inject_identity))
        .with_state(state)
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8_lossy(&bytes).to_string()
}

#[tokio::test]
async fn shadow_forwards_arbitrary_route_and_caches_second_call() {
    let (base, peer_state) = spawn_peer().await;
    // The stub peer answers `/fundamentals/:key`. In shadow mode this arbitrary
    // route (no local DB) is forwarded rather than 500ing locally.
    let state = app_state_shadow(Some(&base));
    let app = shadow_router(state);

    // First GET → cold → forwarded to peer.
    let resp1 = app
        .clone()
        .oneshot(HttpRequest::builder().uri("/fundamentals/AAPL").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(peer_state.hits.load(Ordering::SeqCst), 1);
    let b1 = body_string(resp1).await;
    assert!(b1.contains("\"ok\":true"), "peer body relayed: {b1}");

    // Second identical GET → warm → served from cache, NO second peer round-trip.
    let resp2 = app
        .clone()
        .oneshot(HttpRequest::builder().uri("/fundamentals/AAPL").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(
        peer_state.hits.load(Ordering::SeqCst),
        1,
        "cache hit must not re-hit the peer"
    );
    assert_eq!(body_string(resp2).await, b1);
}

#[tokio::test]
async fn shadow_does_not_forward_health_or_v1() {
    let (base, peer_state) = spawn_peer().await;
    let state = app_state_shadow(Some(&base));
    let app = shadow_router(state);

    for path in ["/health", "/v1/models"] {
        let resp = app
            .clone()
            .oneshot(HttpRequest::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        // Served locally (sentinel), never forwarded.
        assert_eq!(resp.status().as_u16(), 599, "{path} must be local");
        assert_eq!(body_string(resp).await, "LOCAL");
    }
    assert_eq!(
        peer_state.hits.load(Ordering::SeqCst),
        0,
        "allowlisted paths must never reach the peer"
    );
}

#[tokio::test]
async fn shadow_does_not_forward_writes() {
    let (base, peer_state) = spawn_peer().await;
    let state = app_state_shadow(Some(&base));
    let app = shadow_router(state);

    // POST is not GET/HEAD → passthrough to the local handler (404/599 sentinel).
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/fundamentals/AAPL")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The middleware passed the POST through to local routing (the test router
    // has no POST handler → 405). The point: it was NOT forwarded to the peer.
    assert_ne!(resp.status(), StatusCode::OK, "write must not return a peer 200");
    assert_eq!(
        peer_state.hits.load(Ordering::SeqCst),
        0,
        "writes must never reach the peer"
    );
}

#[tokio::test]
async fn no_shadow_when_read_federate_unset_forwards_nothing() {
    let (_base, peer_state) = spawn_peer().await;
    // read_federate = None (no peer) → pure passthrough.
    let state = app_state_shadow(None);
    let app = shadow_router(state);

    let resp = app
        .oneshot(HttpRequest::builder().uri("/fundamentals/AAPL").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 599, "no shadow → served locally");
    assert_eq!(
        peer_state.hits.load(Ordering::SeqCst),
        0,
        "nothing forwarded when read_federate unset"
    );
}
