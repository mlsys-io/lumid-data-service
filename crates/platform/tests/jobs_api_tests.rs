//! End-to-end `/jobs/v1/*` + MCP through the REAL router and the REAL auth
//! gate: a stub lum.id introspection server maps tokens to two ordinary users
//! (role `user`), plus local keys for a runtime and a prober.
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://… \
//!     cargo test -p lumid-platform --test jobs_api_tests -- --ignored --test-threads=1
//!
//! THROWAWAY database only.

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

fn code(v: &Value) -> &str {
    v["error"]["code"].as_str().unwrap_or("")
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn sources_sharing_plan_submit_tokens_events_cancel() {
    let Some(st) = state().await else { return };
    fixture(&st).await;
    let app = router(st.clone());

    // Alice registers a source on this hub's data; can't register another hub's.
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/sources",
        ALICE,
        Some(json!({"name": "panel", "ref": "lumid-data://ukb_demo.phenotype"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let src = v["source_id"].as_str().unwrap().to_string();
    assert_eq!(v["visibility"], "private");
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/sources",
        ALICE,
        Some(json!({"name": "far", "ref": "nus://ukb"})),
        None,
    )
    .await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::BAD_REQUEST, "source_not_on_this_hub")
    );
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/sources",
        ALICE,
        Some(json!({"name": "panel", "ref": "lumid-data://ukb_demo.phenotype"})),
        None,
    )
    .await;
    assert_eq!((s, code(&v)), (StatusCode::CONFLICT, "source_exists"));

    let spec =
        json!({"app": "e2e", "runtime": "lumilake", "sources": [{"ref": format!("src://{src}")}]});

    // Bob can't see or use it yet.
    let (_, v) = call(
        &app,
        "GET",
        "/jobs/v1/sources?scope=shared",
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!(v["sources"].as_array().unwrap().len(), 0);
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/plan",
        BOB,
        Some(json!({"spec": spec})),
        None,
    )
    .await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::FORBIDDEN, "source_not_entitled"),
        "{v}"
    );
    // Only the owner shares.
    let (s, _) = call(
        &app,
        "POST",
        &format!("/jobs/v1/sources/{src}/grants"),
        BOB,
        Some(json!({"grantee": "bob-sub"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Alice shares; Bob sees it and plans.
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/sources/{src}/grants"),
        ALICE,
        Some(json!({"grantee": "bob-sub"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let (_, v) = call(
        &app,
        "GET",
        "/jobs/v1/sources/shared-with-me",
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!(v["sources"][0]["source_id"], src.as_str());
    assert_eq!(v["sources"][0]["visibility"], "shared");
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/plan",
        BOB,
        Some(json!({"spec": spec})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["plan"]["outcome"], "placed");
    assert_eq!(v["plan"]["site"], "home");
    assert_eq!(v["plan"]["sources"][0]["via"], "local");
    assert_eq!(v["plan"]["sources"][0]["url"], "http://192.168.7.155:5101");
    // The alias resolves to the underlying table.
    assert_eq!(v["sources"][0]["ref"], "lumid-data://ukb_demo.phenotype");

    // Submit is idempotent.
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        BOB,
        Some(json!({"spec": spec})),
        Some("k1"),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let job = v["job_id"].as_str().unwrap().to_string();
    assert_eq!(v["status"], "planned");
    assert_eq!(v["sources"][0]["ref"], "lumid-data://ukb_demo.phenotype");
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        BOB,
        Some(json!({"spec": spec})),
        Some("k1"),
    )
    .await;
    assert_eq!(
        (s, v["job_id"].as_str().unwrap()),
        (StatusCode::OK, job.as_str())
    );

    // Visibility: Bob yes, Alice no (even though it reads her source).
    let (s, _) = call(
        &app,
        "GET",
        &format!("/jobs/v1/jobs/{job}"),
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = call(
        &app,
        "GET",
        &format!("/jobs/v1/jobs/{job}"),
        ALICE,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Tokens go to the job's runtime, never to a person.
    let (s, _) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/token"),
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    // The runtime must name the user it authenticated, and that user must
    // own the job: a submitter quoting someone else's job id gets nothing.
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/token"),
        RUNTIME,
        None,
        None,
    )
    .await;
    assert_eq!(
        (s, v["error"]["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("for_sub_required")),
        "{v}"
    );
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/token"),
        RUNTIME,
        Some(json!({"for_sub": "alice-sub"})),
        None,
    )
    .await;
    assert_eq!(
        (s, v["error"]["code"].as_str()),
        (StatusCode::FORBIDDEN, Some("not_the_owner")),
        "{v}"
    );
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/token"),
        RUNTIME,
        Some(json!({"for_sub": "bob-sub"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let jt = v["token"].as_str().unwrap().to_string();

    // The job token reads its source, reports progress, renews itself …
    let (s, v) = call(
        &app,
        "POST",
        "/retrieve",
        &jt,
        Some(json!({"sql": "select count(*) from ukb_demo.phenotype"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/events"),
        &jt,
        Some(json!({"status": "running", "message": "started"})),
        None,
    )
    .await;
    assert_eq!(
        (s, v["status"].as_str()),
        (StatusCode::OK, Some("running")),
        "{v}"
    );
    let (s, _) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/token"),
        &jt,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // … but can't submit, plan, share, or touch another job.
    let (s, _) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        &jt,
        Some(json!({"spec": spec})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = call(
        &app,
        "POST",
        "/jobs/v1/plan",
        &jt,
        Some(json!({"spec": spec})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v2) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        BOB,
        Some(json!({"spec": spec})),
        Some("k2"),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    let other = v2["job_id"].as_str().unwrap();
    let (s, _) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{other}/events"),
        &jt,
        Some(json!({"status": "running"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // Illegal lifecycle moves are refused.
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/events"),
        RUNTIME,
        Some(json!({"status": "planned"})),
        None,
    )
    .await;
    assert_eq!((s, code(&v)), (StatusCode::CONFLICT, "invalid_transition"));

    // Events and per-source bytes are visible to the owner.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, v) = call(
        &app,
        "GET",
        &format!("/jobs/v1/jobs/{job}/events"),
        BOB,
        None,
        None,
    )
    .await;
    let kinds: Vec<&str> = v["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["submitted", "status"]);
    let (_, v) = call(
        &app,
        "GET",
        &format!("/jobs/v1/jobs/{job}"),
        BOB,
        None,
        None,
    )
    .await;
    assert!(v["sources"][0]["actual_bytes"].as_i64().unwrap() > 0, "{v}");

    // Cancel kills the token at once.
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/cancel"),
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!(
        (s, v["status"].as_str()),
        (StatusCode::OK, Some("cancelled"))
    );
    let (s, _) = call(
        &app,
        "POST",
        "/retrieve",
        &jt,
        Some(json!({"sql": "select 1 from ukb_demo.phenotype"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, v) = call(
        &app,
        "POST",
        &format!("/jobs/v1/jobs/{job}/cancel"),
        BOB,
        None,
        None,
    )
    .await;
    assert_eq!((s, code(&v)), (StatusCode::CONFLICT, "job_already_ended"));

    // Alice withdraws the share; Bob can no longer plan with it.
    let (s, _) = call(
        &app,
        "DELETE",
        &format!("/jobs/v1/sources/{src}/grants/bob-sub/read"),
        ALICE,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/plan",
        BOB,
        Some(json!({"spec": spec})),
        None,
    )
    .await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::FORBIDDEN, "source_not_entitled")
    );
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn refusals_quotas_mcp_openapi_and_probers() {
    let Some(st) = state().await else { return };
    fixture(&st).await;
    let app = router(st.clone());
    let estate = json!({"app": "e2e", "runtime": "flowmesh", "sources": [{"ref": "lumid-data://ukb_demo.phenotype"}]});

    // No site runs sandbox → refused, recorded, 422 with reasons.
    let mut sandbox = estate.clone();
    sandbox["runtime"] = json!("sandbox");
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        BOB,
        Some(json!({"spec": sandbox})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert_eq!(v["status"], "refused");
    assert_eq!(
        v["plan"]["alternatives"][0]["blocking"][0]["code"],
        "no_worker"
    );

    // Bad refs and specs are coded errors.
    let (s, v) = call(&app, "POST", "/jobs/v1/plan", BOB, Some(json!({"spec": {"app": "e2e", "runtime": "flowmesh", "sources": [{"ref": "ext://127.0.0.1"}]}})), None).await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::BAD_REQUEST, "invalid_ref"),
        "{v}"
    );
    let (s, v) = call(&app, "POST", "/jobs/v1/plan", BOB, Some(json!({"spec": {"app": "e2e", "runtime": "flowmesh", "sinks": [{"ref": "lumid-data://ukb_demo.phenotype"}]}})), None).await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::FORBIDDEN, "source_not_entitled"),
        "{v}"
    );

    // Quota: at most N active jobs.
    std::env::set_var("LUMID_JOBS_MAX_CONCURRENT", "2");
    for i in 0..2 {
        let (s, v) = call(
            &app,
            "POST",
            "/jobs/v1/jobs",
            BOB,
            Some(json!({"spec": estate})),
            Some(&format!("q{i}")),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
    }
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        BOB,
        Some(json!({"spec": estate})),
        Some("q2"),
    )
    .await;
    assert_eq!(
        (s, code(&v)),
        (StatusCode::TOO_MANY_REQUESTS, "quota_exceeded")
    );
    // … per owner: Alice is unaffected.
    let (s, _) = call(
        &app,
        "POST",
        "/jobs/v1/jobs",
        ALICE,
        Some(json!({"spec": estate})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    std::env::remove_var("LUMID_JOBS_MAX_CONCURRENT");
    // Bob's list shows only Bob's jobs.
    let (_, v) = call(&app, "GET", "/jobs/v1/jobs", BOB, None, None).await;
    assert!(v["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .all(|j| j["owner"] == "bob-sub"));

    // MCP: the same verbs, as the calling user.
    let rpc = |method: &str, params: Value| json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let (_, v) = call(
        &app,
        "POST",
        "/mcp",
        BOB,
        Some(rpc("tools/list", json!({}))),
        None,
    )
    .await;
    let names: Vec<&str> = v["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for n in [
        "jobs_sources",
        "jobs_plan",
        "jobs_submit",
        "jobs_status",
        "jobs_cancel",
        "jobs_source_share",
    ] {
        assert!(names.contains(&n), "{n} missing from {names:?}");
    }
    let (_, v) = call(
        &app,
        "POST",
        "/mcp",
        BOB,
        Some(rpc(
            "tools/call",
            json!({"name": "jobs_plan", "arguments": {"spec": estate}}),
        )),
        None,
    )
    .await;
    assert_eq!(v["result"]["isError"], false, "{v}");
    let inner: Value =
        serde_json::from_str(v["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(inner["plan"]["site"], "home");
    let (_, v) = call(
        &app,
        "POST",
        "/mcp",
        BOB,
        Some(rpc(
            "tools/call",
            json!({"name": "jobs_plan", "arguments": {"spec": sandbox}}),
        )),
        None,
    )
    .await;
    let inner: Value =
        serde_json::from_str(v["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(inner["plan"]["outcome"], "refused");
    // A coded error reaches the agent as JSON it can branch on.
    let (_, v) = call(&app, "POST", "/mcp", BOB, Some(rpc("tools/call", json!({"name": "jobs_status", "arguments": {"job_id": uuid::Uuid::new_v4().to_string()}}))), None).await;
    assert_eq!(v["result"]["isError"], true);
    assert!(v["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("job_not_found"));

    // Route health: probers only, this hub's routes only.
    let (s, _) = call(
        &app,
        "POST",
        "/jobs/v1/admin/route-health",
        BOB,
        Some(json!({"route_id": "home-lan-ld", "health": "up"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v) = call(
        &app,
        "POST",
        "/jobs/v1/admin/route-health",
        PROBER,
        Some(json!({"route_id": "home-lan-ld", "health": "up", "latency_ms": 2})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let (s, _) = call(
        &app,
        "POST",
        "/jobs/v1/admin/route-health",
        PROBER,
        Some(json!({"route_id": "nope", "health": "up"})),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Route list: a prober sees its own site's routes, with the health it
    // just reported; another site is refused, not widened; users are refused.
    let (s, v) = call(&app, "GET", "/jobs/v1/admin/routes", PROBER, None, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let routes = v["routes"].as_array().expect("routes array");
    assert!(!routes.is_empty(), "{v}");
    assert!(routes.iter().all(|r| r["from_site"] == "home"), "{v}");
    let ld = routes
        .iter()
        .find(|r| r["route_id"] == "home-lan-ld")
        .expect("home-lan-ld listed");
    assert_eq!(ld["health"], "up");
    assert_eq!(ld["latency_ms"], 2);
    let (s, _) = call(
        &app,
        "GET",
        "/jobs/v1/admin/routes?from_site=office",
        PROBER,
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = call(&app, "GET", "/jobs/v1/admin/routes", BOB, None, None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Unknown callers never get in.
    let (s, _) = call(
        &app,
        "GET",
        "/jobs/v1/jobs",
        "lm_pat_live_nobody",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[test]
fn openapi_documents_the_jobs_surface() {
    let p = lumid_platform::jobs::api::openapi_paths();
    for k in [
        "/jobs/v1/plan",
        "/jobs/v1/jobs",
        "/jobs/v1/sources/{id}/grants",
        "/jobs/v1/jobs/{id}/token",
        "/jobs/v1/admin/routes",
    ] {
        assert!(p.get(k).is_some(), "{k}");
    }
}
