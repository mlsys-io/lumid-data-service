//! End-to-end dispatch to Lumilake over REAL network hops:
//! hub (real router on a TCP port) → stub lum.id (mint + introspect) → stub
//! Lumilake, which — like the real `lumid_job` support — calls BACK into the
//! hub for the job token with its runtime key and reads data with it.
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://… \
//!     cargo test -p lumid-platform --test jobs_dispatch_tests -- --ignored --test-threads=1

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use lumid_platform::config::Settings;
use lumid_platform::state::AppState;
use serde_json::{json, Value};

const BOB: &str = "lm_pat_live_bob_dispatch";
const RUNTIME_KEY: &str = "rt-lumilake-dispatch";
const BRIDGE: &str = "test-bridge-secret";

#[derive(Default)]
struct Seen {
    submits: Vec<(String, Value)>, // (Authorization, body)
    worker_reads: Vec<u16>,        // status of the stub worker's data read
    job_token: Option<String>,
    status: String,
    cancels: Vec<String>,
}
type Shared = Arc<Mutex<Seen>>;

async fn serve(app: Router) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

/// lum.id: PAT/minted-token introspection and the bridge mint.
async fn stub_lumid() -> String {
    async fn introspect(Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
        let t = b["token"].as_str().unwrap_or("");
        let sub = match t {
            BOB => Some("bob-sub".to_string()),
            t if t.starts_with("minted.") => Some(t.trim_start_matches("minted.").to_string()),
            _ => None,
        };
        match sub {
            Some(s) => (
                StatusCode::OK,
                Json(json!({"sub": s, "role": "user", "active": true})),
            ),
            None => (StatusCode::UNAUTHORIZED, Json(json!({}))),
        }
    }
    async fn mint(headers: HeaderMap, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
        if headers.get("x-bridge-secret").and_then(|v| v.to_str().ok()) != Some(BRIDGE) {
            return (StatusCode::UNAUTHORIZED, Json(json!({})));
        }
        // A JWT-shaped token so the platform's client would introspect it.
        let sub = b["sub"].as_str().unwrap_or("");
        (
            StatusCode::OK,
            Json(json!({"data": {"token": format!("minted.{sub}"), "expires_at": "x"}})),
        )
    }
    serve(
        Router::new()
            .route("/api/v1/identity/introspect", post(introspect))
            .route("/api/v1/internal/mint-user-token", post(mint)),
    )
    .await
}

/// Lumilake: accepts a submit as the owner, fetches the job token from the hub,
/// reads the job's data with it (as a worker would), reports status on GET.
async fn stub_lumilake(seen: Shared, hub: Arc<Mutex<String>>) -> String {
    async fn submit(
        State((seen, hub)): State<(Shared, Arc<Mutex<String>>)>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        seen.lock()
            .unwrap()
            .submits
            .push((auth.clone(), body.clone()));
        if body["data"][0]["name"] == "reject-me" {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail": "Graph compilation failed: nope"})),
            );
        }
        let job_id = body["lumid_job"]["job_id"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let hub = hub.lock().unwrap().clone();
        let http = reqwest::Client::new();
        let tok: Value = http
            .post(format!("{hub}/jobs/v1/jobs/{job_id}/token"))
            .bearer_auth(RUNTIME_KEY)
            // The user this runtime authenticated: the hub minted the
            // dispatch bearer as `minted.<sub>` (see the identity stub).
            .json(&json!({"for_sub": auth.trim_start_matches("Bearer ").trim_start_matches("minted.")}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ljt = tok["token"].as_str().unwrap_or("").to_string();
        let read = http
            .post(format!("{hub}/retrieve"))
            .bearer_auth(&ljt)
            .json(&json!({"sql": "select count(*) from ukb_demo.phenotype"}))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16();
        let mut s = seen.lock().unwrap();
        s.worker_reads.push(read);
        s.job_token = Some(ljt);
        s.status = "pending".into();
        (
            StatusCode::OK,
            Json(json!({"ok": true, "data": {"job_id": "req-ll-1", "status": "pending"}})),
        )
    }
    async fn status(
        State((seen, _)): State<(Shared, Arc<Mutex<String>>)>,
        Path(id): Path<String>,
    ) -> (StatusCode, Json<Value>) {
        if id != "req-ll-1" {
            return (StatusCode::NOT_FOUND, Json(json!({})));
        }
        let s = seen.lock().unwrap().status.clone();
        (
            StatusCode::OK,
            Json(json!({"ok": true, "data": {"job_id": id, "status": s}})),
        )
    }
    async fn cancel(
        State((seen, _)): State<(Shared, Arc<Mutex<String>>)>,
        Path(id): Path<String>,
    ) -> Json<Value> {
        seen.lock().unwrap().cancels.push(id);
        Json(json!({"ok": true}))
    }
    async fn openapi() -> Json<Value> {
        Json(json!({"components": {"schemas": {"LumidJobBinding": {"type": "object"}}}}))
    }
    serve(
        Router::new()
            .route("/openapi.json", get(openapi))
            .route("/api/v1/jobs", post(submit))
            .route("/api/v1/jobs/:id", get(status))
            .route("/api/v1/jobs/:id/cancel", post(cancel))
            .with_state((seen, hub)),
    )
    .await
}

async fn boot() -> Option<(AppState, String, Shared)> {
    if std::env::var("LUMID_JOBS_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_JOBS_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let seen: Shared = Arc::new(Mutex::new(Seen::default()));
    let hub_slot = Arc::new(Mutex::new(String::new()));
    let lumilake = stub_lumilake(seen.clone(), hub_slot.clone()).await;
    let url = reqwest::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    let mut s = Settings::from_env();
    s.db_host = url.host_str().unwrap().into();
    s.db_port = url.port().unwrap_or(5432);
    s.db_user = url.username().into();
    s.db_password = url.password().unwrap_or("").into();
    s.db_name = url.path().trim_start_matches('/').into();
    s.lumid_enabled = true;
    s.lumid_url = stub_lumid().await;
    s.api_keys_raw = format!("{RUNTIME_KEY}:runtime:lumilake");
    s.rate_limit_anon = "100000/minute".into();
    s.rate_limit_authed = "100000/minute".into();
    s.max_concurrency_per_key = 0;
    s.max_concurrency_per_ip = 0;
    s.redis_url = String::new();
    s.ch_url = String::new();
    s.llm_backend_url = String::new();
    s.peers = Vec::new();
    s.read_federate = None;
    s.retrieval_db_role = String::new();
    s.blob_root = "/unused-in-memory".into();
    s.jobs_enabled = true;
    s.hub_id = "home".into();
    s.hub_site = "home".into();
    s.jobs_estate = "lumid-data".into();
    s.jobs_instance = "lumid-data".into();
    s.jobs_routes_json = r#"[{"route_id":"home-lan-ld","from_site":"home","source_id":"lumid-data","kind":"lan","url":"http://192.168.7.155:5101","pref":10}]"#.into();
    s.jobs_sites_json = r#"[{"site":"home","runtimes":["lumilake"]}]"#.into();
    s.jobs_runtimes_json = format!(r#"{{"lumilake": {{"home": "{lumilake}"}}}}"#);
    s.bridge_secret = BRIDGE.into();
    // The mint endpoint is reached on its own (private) URL.
    s.bridge_url = s.lumid_url.clone();
    let settings = Arc::new(s);
    let pool = lumid_platform::db::build_pool(&settings).unwrap();
    let st = AppState {
        pool: pool.clone(),
        xpio_pool: None,
        lumid: Arc::new(lumid_platform::auth::lumid::LumidClient::new(&settings)),
        local_keys: Arc::new(lumid_platform::auth::parse_local_keys(
            &settings.api_keys_raw,
        )),
        rate: Arc::new(lumid_platform::auth::ratelimit::RateLimiter::new(
            &settings.rate_limit_anon,
            &settings.rate_limit_authed,
        )),
        concurrency: None,
        redis: None,
        redis_client: None,
        hub: None,
        read_cache: lumid_platform::read::cache::CacheManager::new(
            16 << 20,
            Duration::from_secs(3600),
            None,
            HashMap::new(),
        ),
        http: reqwest::Client::new(),
        http_stream: reqwest::Client::new(),
        llm_pool: Arc::new(lumid_platform::llm_pool::BackendPool::from_settings(
            &settings,
        )),
        blob_store: Arc::new(object_store::memory::InMemory::new()),
        backends: Arc::new(lumid_platform::backend::Registry::new_postgres_only(
            pool.clone(),
        )),
        feed_liveness: Arc::new(lumid_platform::realtime::DefaultFeedLiveness),
        card_store: Arc::new(lumid_platform::retrieve::card_store::CardStore::new(
            pool.clone(),
            settings.retrieval_card_ttl_s,
            settings.retrieval_sample_rows,
        )),
        federation: Arc::new(lumid_platform::federation::Federation::new(
            &[],
            reqwest::Client::new(),
            settings.instance_id.clone(),
            settings.app_id.clone(),
        )),
        shadow_cache: lumid_platform::federation::ShadowCache::new(Duration::from_secs(30)),
        settings,
    };
    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "DROP SCHEMA IF EXISTS jobs CASCADE; DROP SCHEMA IF EXISTS ukb_demo CASCADE;
             CREATE SCHEMA ukb_demo; CREATE TABLE ukb_demo.phenotype (id int);
             INSERT INTO ukb_demo.phenotype SELECT generate_series(1, 10);",
        )
        .await
        .unwrap();
    lumid_platform::platform_tables::migrate(&st.pool)
        .await
        .unwrap();
    lumid_platform::handlers::me_blobs::migrate(&st.pool)
        .await
        .unwrap();
    lumid_platform::handlers::retrieve::migrate(&st.pool, "")
        .await
        .unwrap();
    lumid_platform::jobs::boot(&st.pool, &st.settings)
        .await
        .unwrap();
    lumid_platform::jobs::token::invalidate_all();
    let app = lumid_platform::app::build_router(
        st.clone(),
        Router::new(),
        lumid_platform::jobs::api::routes(),
        Router::new(),
        Router::new(),
        Router::new(),
    );
    let hub = serve(app).await;
    *hub_slot.lock().unwrap() = hub.clone();
    Some((st, hub, seen))
}

async fn submit(hub: &str, name: &str) -> Value {
    let spec = json!({
        "app": "e2e", "runtime": "lumilake",
        "sources": [{"ref": "lumid-data://ukb_demo.phenotype"}],
        "payload": {"data": [{"name": name, "workflow": "{}", "inputs": {"x": ["1"]},
                               "output_location": {"type": "s3", "prefix": "out/"}}],
                    "workflow_format": "yaml"}
    });
    reqwest::Client::new()
        .post(format!("{hub}/jobs/v1/jobs"))
        .bearer_auth(BOB)
        .json(&json!({"spec": spec}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn job(hub: &str, id: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{hub}/jobs/v1/jobs/{id}"))
        .bearer_auth(BOB)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_status(hub: &str, id: &str, want: &str) -> Value {
    for _ in 0..100 {
        let j = job(hub, id).await;
        if j["status"] == want {
            return j;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job {id} never reached {want}: {}", job(hub, id).await);
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn lumilake_dispatch_as_owner_with_job_token_and_reconcile() {
    let Some((st, hub, seen)) = boot().await else {
        return;
    };

    let created = submit(&hub, "panel").await;
    let id = created["job_id"].as_str().unwrap().to_string();
    let j = wait_status(&hub, &id, "submitted").await;
    assert_eq!(j["runtime_ref"], "req-ll-1");
    {
        let s = seen.lock().unwrap();
        let (auth, body) = &s.submits[0];
        // Submitted AS THE OWNER, not with an operator credential.
        assert_eq!(auth, "Bearer minted.bob-sub");
        // The binding carries the job id and the PLANNED (LAN) data URL …
        assert_eq!(body["lumid_job"]["job_id"], id.as_str());
        assert_eq!(body["lumid_job"]["data_url"], "http://192.168.7.155:5101");
        // … the runtime-native body is passed through, format moved to the header.
        assert_eq!(body["data"][0]["name"], "panel");
        assert!(body.get("workflow_format").is_none());
        // … and no token travels in the submit.
        assert!(!body.to_string().contains("ljt_"));
        // Lumilake got its job token from the hub and the worker read succeeded.
        assert_eq!(s.worker_reads, vec![200]);
    }

    // Reconciler: running, then completed → succeeded; the token dies at the end.
    seen.lock().unwrap().status = "running".into();
    assert_eq!(
        lumid_platform::jobs::dispatch::reconcile_once(&st)
            .await
            .unwrap(),
        1
    );
    wait_status(&hub, &id, "running").await;
    assert_eq!(
        lumid_platform::jobs::dispatch::reconcile_once(&st)
            .await
            .unwrap(),
        0
    );
    seen.lock().unwrap().status = "completed".into();
    lumid_platform::jobs::dispatch::reconcile_once(&st)
        .await
        .unwrap();
    wait_status(&hub, &id, "succeeded").await;
    let ljt = seen.lock().unwrap().job_token.clone().unwrap();
    // Past the grace window the token is dead (revoked on terminal status).
    lumid_platform::jobs::token::invalidate_all();
    let code = reqwest::Client::new()
        .post(format!("{hub}/retrieve"))
        .bearer_auth(&ljt)
        .json(&json!({"sql": "select 1 from ukb_demo.phenotype"}))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(code, reqwest::StatusCode::UNAUTHORIZED);

    // A runtime refusal fails the job with the runtime's reason.
    let bad = submit(&hub, "reject-me").await;
    let b = wait_status(&hub, bad["job_id"].as_str().unwrap(), "failed").await;
    assert!(
        b["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Graph compilation failed"),
        "{b}"
    );

    // Cancel is forwarded to the runtime.
    let c = submit(&hub, "panel").await;
    let cid = c["job_id"].as_str().unwrap().to_string();
    wait_status(&hub, &cid, "submitted").await;
    reqwest::Client::new()
        .post(format!("{hub}/jobs/v1/jobs/{cid}/cancel"))
        .bearer_auth(BOB)
        .send()
        .await
        .unwrap();
    for _ in 0..100 {
        if !seen.lock().unwrap().cancels.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen.lock().unwrap().cancels, vec!["req-ll-1".to_string()]);
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn service_key_owned_jobs_fail_dispatch_clearly() {
    let Some((st, hub, _seen)) = boot().await else {
        return;
    };
    // A job owned by a local key (no lum.id user to act as).
    let id: uuid::Uuid = st
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, site, status, spec)
             VALUES ('home', 'local:svc', 'e2e', 'lumilake', 'home', 'planned', '{\"payload\":{\"data\":[]}}')
             RETURNING job_id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    lumid_platform::jobs::dispatch::dispatch(&st, id).await;
    let row = st
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT status, error FROM jobs.jobs WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "failed");
    let err: Value = row.get("error");
    assert!(
        err["message"].as_str().unwrap().contains("service key"),
        "{err}"
    );
    let _ = hub;
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn a_lumilake_without_lumid_job_support_is_refused_not_used_unscoped() {
    let Some((st, _hub, seen)) = boot().await else {
        return;
    };
    // An older Lumilake: healthy, accepts jobs, but its schema has no
    // LumidJobBinding — it would silently drop the binding.
    async fn old_openapi() -> Json<Value> {
        Json(json!({"components": {"schemas": {"JobSubmitRequest": {}}}}))
    }
    async fn accept() -> Json<Value> {
        Json(json!({"ok": true, "data": {"job_id": "req-old", "status": "pending"}}))
    }
    let old = serve(
        Router::new()
            .route("/openapi.json", get(old_openapi))
            .route("/api/v1/jobs", post(accept)),
    )
    .await;
    let mut settings = (*st.settings).clone();
    settings.jobs_runtimes_json = format!(r#"{{"lumilake": {{"home": "{old}"}}}}"#);
    let st2 = AppState {
        settings: Arc::new(settings),
        ..st.clone()
    };
    let id: uuid::Uuid = st2
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, site, status, spec)
             VALUES ('home', 'bob-sub', 'e2e', 'lumilake', 'home', 'planned',
                     '{\"payload\":{\"data\":[{\"name\":\"x\"}]}}')
             RETURNING job_id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    lumid_platform::jobs::dispatch::dispatch(&st2, id).await;
    let row = st2
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT status, error, runtime_ref FROM jobs.jobs WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "failed");
    assert!(
        row.get::<_, Option<String>>("runtime_ref").is_none(),
        "it was submitted anyway"
    );
    let err: Value = row.get("error");
    assert!(
        err["message"].as_str().unwrap().contains("Lumilake#187"),
        "{err}"
    );
    // The supporting stub was never involved.
    assert!(seen.lock().unwrap().submits.is_empty());
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn owner_tokens_are_minted_on_the_bridge_url_not_the_auth_url() {
    let Some((st, _hub, _seen)) = boot().await else {
        return;
    };
    // Auth (introspection) still works on lumid_url; only the mint URL is dead.
    let mut settings = (*st.settings).clone();
    settings.bridge_url = "http://127.0.0.1:9".into();
    let st2 = AppState {
        settings: Arc::new(settings),
        ..st.clone()
    };
    let id: uuid::Uuid = st2
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, site, status, spec)
             VALUES ('home', 'bob-sub', 'e2e', 'lumilake', 'home', 'planned',
                     '{\"payload\":{\"data\":[{\"name\":\"x\"}]}}')
             RETURNING job_id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    lumid_platform::jobs::dispatch::dispatch(&st2, id).await;
    let row = st2
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT status, error FROM jobs.jobs WHERE job_id = $1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "failed");
    let err: Value = row.get("error");
    assert!(
        err["message"].as_str().unwrap().contains("minting"),
        "{err}"
    );
}
