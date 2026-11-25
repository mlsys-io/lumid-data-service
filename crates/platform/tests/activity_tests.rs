//! Table activity end to end: a run's reads land under its run, app and table;
//! another user sees only a count.
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://... cargo test -p lumid-platform --test activity_tests -- --ignored --test-threads=1
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use lumid_platform::config::Settings;
use lumid_platform::state::AppState;
use serde_json::{json, Value};
use tower::ServiceExt;

const ALICE: &str = "lm_pat_live_alice_test_token";
const BOB: &str = "lm_pat_live_bob_test_token";
const RUNTIME: &str = "rt-lumilake-key";
const PROBER: &str = "prober-home-key";

/// A stub of lum.id's introspection endpoint.
async fn stub_lumid() -> String {
    async fn introspect(Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
        match b["token"].as_str() {
            Some(ALICE) => (
                StatusCode::OK,
                Json(json!({"sub": "alice-sub", "role": "user", "active": true})),
            ),
            Some(BOB) => (
                StatusCode::OK,
                Json(json!({"sub": "bob-sub", "role": "user", "active": true})),
            ),
            _ => (StatusCode::UNAUTHORIZED, Json(json!({}))),
        }
    }
    let app = Router::new().route("/api/v1/identity/introspect", post(introspect));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

async fn state() -> Option<AppState> {
    if std::env::var("LUMID_JOBS_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_JOBS_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let url = reqwest::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    let mut s = Settings::from_env();
    s.db_host = url.host_str().unwrap().into();
    s.db_port = url.port().unwrap_or(5432);
    s.db_user = url.username().into();
    s.db_password = url.password().unwrap_or("").into();
    s.db_name = url.path().trim_start_matches('/').into();
    s.lumid_enabled = true;
    s.lumid_url = stub_lumid().await;
    s.api_keys_raw = format!("{RUNTIME}:runtime:lumilake,{PROBER}:prober:home");
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
    s.jobs_routes_json = r#"[
      {"route_id":"home-lan-ld","from_site":"home","source_id":"lumid-data","kind":"lan","url":"http://192.168.7.155:5101","pref":10},
      {"route_id":"home-pub-ld","from_site":"home","source_id":"lumid-data","kind":"public","url":"https://lum.id/data","pref":90},
      {"route_id":"office-tail-ld","from_site":"office","source_id":"lumid-data","kind":"tailnet","url":"http://100.115.66.10:5101","pref":20}]"#
        .into();
    s.jobs_sites_json = r#"[{"site":"home","runtimes":["flowmesh","lumilake"]},
                            {"site":"office","runtimes":["flowmesh","lumilake"]}]"#
        .into();
    let settings = Arc::new(s);
    let pool = lumid_platform::db::build_pool(&settings).unwrap();
    Some(AppState {
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
            16 * 1024 * 1024,
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
    })
}

/// The real router with the jobs routes + MCP mounted the way boot does.
fn router(st: AppState) -> Router {
    let mut reg = lumid_platform::mcp::McpRegistry::default();
    reg.tools.extend(lumid_platform::jobs::mcp_tools::tools());
    let ext =
        lumid_platform::jobs::api::routes().merge(lumid_platform::mcp::build_router(Arc::new(reg)));
    lumid_platform::app::build_router(
        st,
        Router::new(),
        ext,
        Router::new(),
        Router::new(),
        Router::new(),
    )
}

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    tok: &str,
    body: Option<Value>,
    key: Option<&str>,
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {tok}"));
    if let Some(k) = key {
        b = b.header("idempotency-key", k);
    }
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(b.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into())),
    )
}

async fn fixture(st: &AppState) {
    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "DROP SCHEMA IF EXISTS jobs CASCADE; DROP SCHEMA IF EXISTS ukb_demo CASCADE;
             CREATE SCHEMA ukb_demo;
             CREATE TABLE ukb_demo.phenotype (id int, bmi float8);
             INSERT INTO ukb_demo.phenotype SELECT g, 20 + g % 10 FROM generate_series(1, 30) g;",
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
}

async fn call_h(
    app: &Router,
    path: &str,
    tok: &str,
    body: Value,
    hdrs: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", format!("Bearer {tok}"))
        .header("content-type", "application/json");
    for (k, v) in hdrs {
        b = b.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(b.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore]
async fn a_runs_reads_are_attributed_to_its_run_app_and_table() {
    let Some(st) = state().await else { return };
    fixture(&st).await;
    lumid_platform::activity::migrate(&st.pool, "")
        .await
        .unwrap();
    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute("TRUNCATE provenance.table_activity")
        .await
        .unwrap();
    let app = router(st.clone());

    let (s, v) = call_h(
        &app,
        "/retrieve",
        ALICE,
        json!({"sql": "select * from ukb_demo.phenotype where bmi > 25"}),
        &[
            ("x-lumid-app", "venue-link-matcher"),
            ("x-lumid-loop", "match"),
            ("x-lumid-run", "run-42"),
        ],
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let n = v["rowcount"].as_i64().unwrap();
    assert!(n > 0);

    // Recorded asynchronously: wait for it.
    let mut run = Value::Null;
    for _ in 0..50 {
        let (_, r) = call(&app, "GET", "/activity/runs/run-42", ALICE, None, None).await;
        if r["read"].as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            run = r;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(run["read"][0]["schema"], "ukb_demo", "{run}");
    assert_eq!(run["read"][0]["table"], "phenotype", "{run}");
    assert_eq!(run["read"][0]["rows"], n, "{run}");
    assert!(run["wrote"].as_array().unwrap().is_empty(), "{run}");

    // The app's (and the workflow's) view.
    let (s, a) = call(
        &app,
        "GET",
        "/activity/apps/venue-link-matcher?loop=match",
        ALICE,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{a}");
    assert_eq!(a["read"][0]["table"], "phenotype", "{a}");

    // The table's view: Alice sees her app by name.
    let (_, t) = call(
        &app,
        "GET",
        "/catalog/tables/ukb_demo/phenotype/activity",
        ALICE,
        None,
        None,
    )
    .await;
    assert_eq!(t["read"][0]["app"], "venue-link-matcher", "{t}");
    assert_eq!(t["read"][0]["loop"], "match", "{t}");
    assert_eq!(t["others"]["readers"], 0, "{t}");

    // Bob sees neither her run nor her app's names — only that someone reads it.
    let (_, r) = call(&app, "GET", "/activity/runs/run-42", BOB, None, None).await;
    assert!(r["read"].as_array().unwrap().is_empty(), "{r}");
    let (_, t) = call(
        &app,
        "GET",
        "/catalog/tables/ukb_demo/phenotype/activity",
        BOB,
        None,
        None,
    )
    .await;
    assert!(t["read"].as_array().unwrap().is_empty(), "{t}");
    assert_eq!(t["others"]["readers"], 1, "{t}");
}
