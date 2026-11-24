//! End-to-end job-token enforcement through the REAL router (`app::build_router`):
//! Alice shares a table and a blob prefix with Bob; Bob's job holds a token;
//! the token reads exactly what the job declared, nothing else, and loses
//! access when the share is revoked or the job ends.
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://… \
//!     cargo test -p lumid-platform --test jobs_enforce_tests -- --ignored --test-threads=1
//!
//! THROWAWAY database only: drops and recreates schemas.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use lumid_platform::config::Settings;
use lumid_platform::state::AppState;
use object_store::{path::Path as ObjPath, ObjectStore, PutPayload};
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

const ALICE: &str = "alice-0000";
const BOB: &str = "bob-0000";

fn settings(jobs_enabled: bool) -> Option<Settings> {
    if std::env::var("LUMID_JOBS_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_JOBS_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let url = reqwest::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL")).unwrap();
    let mut s = Settings::from_env();
    s.db_host = url.host_str().unwrap().to_string();
    s.db_port = url.port().unwrap_or(5432);
    s.db_user = url.username().to_string();
    s.db_password = url.password().unwrap_or("").to_string();
    s.db_name = url.path().trim_start_matches('/').to_string();
    s.lumid_enabled = false;
    s.api_keys_raw = "test-key:tester".into();
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
    s.jobs_enabled = jobs_enabled;
    s.hub_id = "home".into();
    s.hub_site = "home".into();
    s.jobs_estate = "lumid-data".into();
    s.jobs_instance = "lumid-data".into();
    s.jobs_routes_json = String::new();
    Some(s)
}

fn state(settings: Settings) -> AppState {
    let settings = Arc::new(settings);
    let pool = lumid_platform::db::build_pool(&settings).unwrap();
    AppState {
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
    }
}

fn router(st: AppState) -> Router {
    lumid_platform::app::build_router(
        st,
        Router::new(),
        Router::new(),
        Router::new(),
        Router::new(),
        Router::new(),
    )
}

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(b.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4 * 1024 * 1024).await.unwrap();
    let v = serde_json::from_slice(&bytes)
        .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, v)
}

/// Boot the platform tables + jobs schema, and lay out Alice's data.
async fn fixture(st: &AppState) {
    let c = st.pool.get().await.unwrap();
    c.batch_execute(
        "DROP SCHEMA IF EXISTS jobs CASCADE;
         DROP SCHEMA IF EXISTS ukb_demo CASCADE;
         DROP SCHEMA IF EXISTS secret CASCADE;
         CREATE SCHEMA ukb_demo; CREATE SCHEMA secret;
         CREATE TABLE ukb_demo.phenotype (id int, bmi float8);
         INSERT INTO ukb_demo.phenotype SELECT g, 20 + g % 10 FROM generate_series(1, 50) g;
         CREATE TABLE ukb_demo.genotype (id int);
         CREATE TABLE secret.t (v text); INSERT INTO secret.t VALUES ('do not read');",
    )
    .await
    .unwrap();
    drop(c);
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

    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, definition, site, visibility) VALUES
               ('src_panel',  'home', '{ALICE}', 'panel',  'table',    '{{\"ref\":\"lumid-data://ukb_demo.phenotype\"}}', 'home', 'shared'),
               ('src_frames', 'home', '{ALICE}', 'frames', 'blob_set', '{{\"ref\":\"data://home/frames/\"}}',          'home', 'shared'),
               ('src_secret', 'home', '{ALICE}', 'secret', 'table',    '{{\"ref\":\"lumid-data://secret.t\"}}',          'home', 'private');
             INSERT INTO jobs.source_grants (source_id, grantee_sub, access, granted_by, home_hub) VALUES
               ('src_panel',  '{BOB}', 'read', '{ALICE}', 'home'),
               ('src_frames', '{BOB}', 'read', '{ALICE}', 'home');"
        ))
        .await
        .unwrap();
    for (k, v) in [
        ("frames/ep1/k0.png", &b"PNG-FRAME-0"[..]),
        ("other/x.png", &b"NOT-YOURS"[..]),
    ] {
        st.blob_store
            .put(&ObjPath::from(k), PutPayload::from(v.to_vec()))
            .await
            .unwrap();
    }
}

/// Bob's running job: declares the panel, the frames prefix — and ALSO tries
/// to declare Alice's private table (which Bob was never granted).
async fn bobs_job(st: &AppState) -> (Uuid, String) {
    let job: Uuid = st
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, status, spec)
             VALUES ('home', $1, 'e2e', 'lumilake', 'running', '{}') RETURNING job_id",
            &[&BOB],
        )
        .await
        .unwrap()
        .get(0);
    st.pool
        .get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO jobs.job_sources (job_id, ref, source_id, access, home_hub) VALUES
               ($1, 'lumid-data://ukb_demo.phenotype', 'src_panel',  'read', 'home'),
               ($1, 'data://home/frames/',             'src_frames', 'read', 'home'),
               ($1, 'lumid-data://secret.t',           'src_secret', 'read', 'home')",
            &[&job],
        )
        .await
        .unwrap();
    let (tok, _) = lumid_platform::jobs::token::mint(&st.pool, job, chrono::Duration::hours(1))
        .await
        .unwrap();
    (job, tok)
}

async fn actual_bytes(st: &AppState, job: Uuid, r: &str) -> i64 {
    st.pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT actual_bytes FROM jobs.job_sources WHERE job_id = $1 AND ref = $2",
            &[&job, &r],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn job_token_reads_exactly_its_declared_shared_sources() {
    let Some(s) = settings(true) else { return };
    let st = state(s);
    fixture(&st).await;
    lumid_platform::jobs::token::invalidate_all();
    let (job, tok) = bobs_job(&st).await;
    let app = router(st.clone());

    // Declared + shared table: allowed, and the bytes are accounted.
    let (code, v) = call(
        &app,
        "POST",
        "/retrieve",
        &tok,
        Some(json!({"sql": "select id, bmi from ukb_demo.phenotype"})),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{v}");
    // Its own retrieval output is readable (owner-gated downstream).
    let uri = v["materialized_uri"].as_str().unwrap().to_string();
    let (code, _) = call(&app, "GET", &uri, &tok, None).await;
    assert_eq!(code, StatusCode::OK);

    // Declared but NEVER granted (Alice's private table): dropped from scope.
    let (code, v) = call(
        &app,
        "POST",
        "/retrieve",
        &tok,
        Some(json!({"sql": "select * from secret.t"})),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN, "{v}");
    // Undeclared table, even though it sits next to a declared one.
    let (code, _) = call(
        &app,
        "POST",
        "/retrieve",
        &tok,
        Some(json!({"sql": "select * from ukb_demo.genotype"})),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    // Relation-hiding functions and per-user bookkeeping.
    for q in [
        "select query_to_xml('select * from secret.t', true, false, '')",
        "select * from ukb_demo.phenotype union all table provenance.runs",
        "select * from jobs.job_tokens",
    ] {
        let (code, v) = call(&app, "POST", "/retrieve", &tok, Some(json!({"sql": q}))).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "{q}: {v}");
    }

    // Blobs: inside the shared prefix yes, outside no.
    let (code, v) = call(&app, "GET", "/blobs/frames/ep1/k0.png", &tok, None).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let (code, _) = call(&app, "GET", "/blobs/other/x.png", &tok, None).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    let (code, _) = call(
        &app,
        "POST",
        "/retrieve",
        &tok,
        Some(
            json!({"plan": {"plan": [{"op": "storage_get", "bucket": "", "key": "other/x.png"}]}}),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);

    // Routes a job never needs.
    for (m, p) in [
        ("GET", "/catalog/lineage/runs"),
        ("GET", "/blobs"),
        ("GET", "/me/blobs"),
        ("POST", "/ingest/blob"),
    ] {
        let (code, _) = call(&app, m, p, &tok, None).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "{m} {p}");
    }
    // Metadata is fine.
    let (code, _) = call(&app, "GET", "/catalog/schemas", &tok, None).await;
    assert_eq!(code, StatusCode::OK);

    // Humans/local keys are untouched by job enforcement.
    let (code, _) = call(
        &app,
        "POST",
        "/retrieve",
        "test-key",
        Some(json!({"sql": "select * from secret.t"})),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    // A made-up job token is refused outright.
    let fake = lumid_platform::jobs::token::generate();
    let (code, _) = call(&app, "GET", "/catalog/schemas", &fake, None).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);

    // Byte accounting reached the right declared refs.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(actual_bytes(&st, job, "lumid-data://ukb_demo.phenotype").await > 0);
    assert_eq!(
        actual_bytes(&st, job, "data://home/frames/").await,
        "PNG-FRAME-0".len() as i64
    );
    assert_eq!(actual_bytes(&st, job, "lumid-data://secret.t").await, 0);
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn revoking_a_share_cuts_off_a_running_job_and_ending_it_kills_the_token() {
    let Some(s) = settings(true) else { return };
    let st = state(s);
    fixture(&st).await;
    lumid_platform::jobs::token::invalidate_all();
    let (job, tok) = bobs_job(&st).await;
    let app = router(st.clone());
    let q = json!({"sql": "select count(*) from ukb_demo.phenotype"});

    let (code, _) = call(&app, "POST", "/retrieve", &tok, Some(q.clone())).await;
    assert_eq!(code, StatusCode::OK);

    // Alice revokes the share. No cache poke: the running job must lose
    // access on its own within the scope TTL.
    st.pool
        .get()
        .await
        .unwrap()
        .execute(
            "UPDATE jobs.source_grants SET revoked_at = now() WHERE source_id = 'src_panel' AND grantee_sub = $1",
            &[&BOB],
        )
        .await
        .unwrap();
    tokio::time::sleep(lumid_platform::jobs::token::SCOPE_TTL + Duration::from_secs(1)).await;
    let (code, v) = call(&app, "POST", "/retrieve", &tok, Some(q.clone())).await;
    assert_eq!(
        code,
        StatusCode::FORBIDDEN,
        "still readable after revoke: {v}"
    );
    // The frames share was not revoked, so that still works.
    let (code, _) = call(&app, "GET", "/blobs/frames/ep1/k0.png", &tok, None).await;
    assert_eq!(code, StatusCode::OK);

    // The job finishes: within the grace window the token still works …
    st.pool
        .get()
        .await
        .unwrap()
        .execute(
            "UPDATE jobs.jobs SET status = 'succeeded', ended_at = now() WHERE job_id = $1",
            &[&job],
        )
        .await
        .unwrap();
    lumid_platform::jobs::token::invalidate_all();
    let (code, _) = call(&app, "GET", "/blobs/frames/ep1/k0.png", &tok, None).await;
    assert_eq!(code, StatusCode::OK);
    // … and after it, the token is dead.
    st.pool
        .get()
        .await
        .unwrap()
        .execute(
            "UPDATE jobs.jobs SET ended_at = now() - interval '11 minutes' WHERE job_id = $1",
            &[&job],
        )
        .await
        .unwrap();
    lumid_platform::jobs::token::invalidate_all();
    let (code, _) = call(&app, "GET", "/blobs/frames/ep1/k0.png", &tok, None).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);

    // Explicit revocation (cancel) also kills it immediately.
    let (job2, tok2) = bobs_job(&st).await;
    lumid_platform::jobs::token::revoke_job(&st.pool, job2)
        .await
        .unwrap();
    let (code, _) = call(&app, "GET", "/catalog/schemas", &tok2, None).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn job_tokens_are_refused_when_jobs_are_disabled() {
    let Some(s) = settings(false) else { return };
    let st = state(s);
    let app = router(st);
    let (code, v) = call(
        &app,
        "GET",
        "/catalog/schemas",
        &lumid_platform::jobs::token::generate(),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::UNAUTHORIZED, "{v}");
}
