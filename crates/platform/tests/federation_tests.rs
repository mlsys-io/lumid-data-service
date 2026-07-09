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
