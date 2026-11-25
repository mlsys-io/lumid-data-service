//! Hub-to-hub sync end to end: two hubs (`home`, `nus`) in-process, each with
//! its own database, the real router + auth gate on `nus` served over TCP,
//! and `home` driving both directions through `sync::hub::sched`.
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://… \
//!     cargo test -p lumid-platform --test hub_sync_tests -- --ignored --test-threads=1
//!
//! THROWAWAY server only: creates/resets databases `lds_hub_home` and
//! `lds_hub_nus` (schemas jobs, sync, raw are dropped on every test).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use lumid_platform::config::Settings;
use lumid_platform::state::AppState;
use lumid_platform::sync::hub::registry::{self, RegistryBatch};
use lumid_platform::sync::hub::{self, sched, HubPeer};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use tower::ServiceExt;

const NUS_KEY_FOR_HOME: &str = "nus-sync-key-for-home";
const NUS_ADMIN: &str = "nus-admin-key";
const HOME_ADMIN: &str = "home-admin-key";
const CAP: u64 = 1024;

fn gate() -> bool {
    if std::env::var("LUMID_JOBS_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_JOBS_E2E=1 + DATABASE_URL to run live");
        return false;
    }
    true
}

async fn ensure_db(name: &str) {
    let url = std::env::var("DATABASE_URL").unwrap();
    let (c, conn) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect admin db");
    tokio::spawn(conn);
    if c.query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&name])
        .await
        .unwrap()
        .is_none()
    {
        c.batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
    }
}

fn settings(db: &str, hub_id: &str) -> Settings {
    let url = reqwest::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    let mut s = Settings::from_env();
    s.db_host = url.host_str().unwrap().into();
    s.db_port = url.port().unwrap_or(5432);
    s.db_user = url.username().into();
    s.db_password = url.password().unwrap_or("").into();
    s.db_name = db.into();
    // Local keys only; nothing may reach a real identity service.
    s.lumid_enabled = true;
    s.lumid_url = "http://127.0.0.1:9".into();
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
    s.hub_id = hub_id.into();
    s.hub_site = hub_id.into();
    s.jobs_estate = String::new();
    s.jobs_instance = format!("{hub_id}-data");
    s.hub_small_max = CAP;
    s.hub_sync_interval_s = 2;
    s.hub_sync_overlap_s = 120;
    s.hub_sync_confine = true;
    // Small pages so every leg exercises the `more` loop.
    s.sync_batch_rows = 2;
    s.sync_peer_labels = Vec::new();
    s
}

fn app_state(s: Settings) -> AppState {
    let settings = Arc::new(s);
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

/// Reset one hub's database to a freshly-booted state (boot order as `serve`).
async fn reset(st: &AppState) {
    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "DROP SCHEMA IF EXISTS jobs CASCADE; DROP SCHEMA IF EXISTS sync CASCADE;
             DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS nusdata CASCADE;",
        )
        .await
        .unwrap();
    lumid_platform::platform_tables::migrate(&st.pool)
        .await
        .unwrap();
    lumid_platform::handlers::me_blobs::migrate(&st.pool)
        .await
        .unwrap();
    lumid_platform::jobs::schema::migrate(&st.pool, "")
        .await
        .unwrap();
    lumid_platform::sync::migrate(&st.pool).await.unwrap();
    hub::migrate(&st.pool, "").await.unwrap();
    // Idempotent: a second boot runs cleanly over the first.
    hub::migrate(&st.pool, "").await.unwrap();
}

fn router(st: AppState) -> Router {
    let ext = lumid_platform::sync::routes().merge(hub::routes(st.settings.hub_small_max));
    lumid_platform::app::build_router(
        st,
        Router::new(),
        ext,
        Router::new(),
        Router::new(),
        Router::new(),
    )
}

struct Hubs {
    home: AppState,
    nus: AppState,
    nus_app: Router,
    home_app: Router,
    peer: HubPeer,
}

async fn hubs() -> Option<Hubs> {
    if !gate() {
        return None;
    }
    ensure_db("lds_hub_home").await;
    ensure_db("lds_hub_nus").await;

    let mut ns = settings("lds_hub_nus", "nus");
    ns.api_keys_raw = format!("{NUS_KEY_FOR_HOME}:sync:home,{NUS_ADMIN}:admin");
    ns.hub_peer_ids = vec!["home".into()];
    let nus = app_state(ns);
    reset(&nus).await;
    let nus_app = router(nus.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let serve = nus_app.clone();
    tokio::spawn(async move { axum::serve(l, serve).await.unwrap() });

    let mut hs = settings("lds_hub_home", "home");
    hs.api_keys_raw = format!("{HOME_ADMIN}:admin");
    hs.hub_sync_peers_json =
        format!(r#"[{{"hub_id":"nus","base_url":"http://{addr}","key_env":"UNUSED_IN_TEST"}}]"#);
    let home = app_state(hs);
    reset(&home).await;
    let home_app = router(home.clone());
    let peer = HubPeer {
        hub_id: "nus".into(),
        base_url: format!("http://{addr}"),
        token: NUS_KEY_FOR_HOME.into(),
    };
    Some(Hubs {
        home,
        nus,
        nus_app,
        home_app,
        peer,
    })
}

async fn exec(st: &AppState, sql: &str) {
    st.pool
        .get()
        .await
        .unwrap()
        .batch_execute(sql)
        .await
        .unwrap();
}

async fn one(st: &AppState, sql: &str) -> Option<tokio_postgres::Row> {
    st.pool
        .get()
        .await
        .unwrap()
        .query_opt(sql, &[])
        .await
        .unwrap()
}

async fn n(st: &AppState, sql: &str) -> i64 {
    st.pool
        .get()
        .await
        .unwrap()
        .query_one(sql, &[])
        .await
        .unwrap()
        .get(0)
}

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    tok: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {tok}"));
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

fn source_sql(id: &str, hub: &str, name: &str) -> String {
    format!(
        "INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, site, visibility)
         VALUES ('{id}', '{hub}', 'alice-sub', '{name}', 'table', '{hub}', 'private');"
    )
}

async fn clean_cycle(h: &Hubs) -> sched::CycleReport {
    let r = sched::run_cycle(&h.home, &h.peer).await;
    assert!(r.errors.is_empty(), "cycle errors: {:?}", r.errors);
    r
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn sources_cross_both_ways_within_two_intervals() {
    let Some(h) = hubs().await else { return };
    let interval = Duration::from_secs(h.home.settings.hub_sync_interval_s);
    sched::spawn(h.home.clone(), vec![h.peer.clone()]);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Five at nus (pages of 2 → the pull pages), one at home.
    let t0 = Instant::now();
    for i in 0..5 {
        exec(
            &h.nus,
            &source_sql(&format!("src_nus_{i}"), "nus", &format!("nus-{i}")),
        )
        .await;
    }
    exec(&h.home, &source_sql("src_home_1", "home", "home-1")).await;

    let deadline = t0 + interval * 2 + Duration::from_millis(500);
    loop {
        let at_home = n(
            &h.home,
            "SELECT count(*) FROM jobs.sources WHERE home_hub = 'nus'",
        )
        .await;
        let at_nus = n(
            &h.nus,
            "SELECT count(*) FROM jobs.sources WHERE source_id = 'src_home_1'",
        )
        .await;
        if at_home == 5 && at_nus == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "not synced within 2 intervals: home has {at_home}/5 nus rows, nus has {at_nus}/1"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!(
        "synced both ways in {:?} (interval {:?})",
        t0.elapsed(),
        interval
    );

    // Copies keep their owner and version exactly.
    let a = one(
        &h.nus,
        "SELECT home_hub, updated_at::text FROM jobs.sources WHERE source_id='src_nus_3'",
    )
    .await
    .unwrap();
    let b = one(
        &h.home,
        "SELECT home_hub, updated_at::text FROM jobs.sources WHERE source_id='src_nus_3'",
    )
    .await
    .unwrap();
    assert_eq!(a.get::<_, String>(0), b.get::<_, String>(0));
    assert_eq!(a.get::<_, String>(1), b.get::<_, String>(1));
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn deletes_and_updates_propagate_as_tombstones() {
    let Some(h) = hubs().await else { return };
    exec(&h.nus, &source_sql("src_n", "nus", "n")).await;
    exec(&h.home, &source_sql("src_h", "home", "h")).await;
    clean_cycle(&h).await;
    assert!(one(
        &h.home,
        "SELECT 1 FROM jobs.sources WHERE source_id='src_n' AND deleted_at IS NULL"
    )
    .await
    .is_some());

    // Owner-side updates and tombstones, then one cycle.
    exec(
        &h.nus,
        "UPDATE jobs.sources SET deleted_at = now(), updated_at = now() WHERE source_id = 'src_n'",
    )
    .await;
    exec(&h.home, "UPDATE jobs.sources SET visibility = 'shared', updated_at = now() WHERE source_id = 'src_h'").await;
    clean_cycle(&h).await;
    assert!(
        one(
            &h.home,
            "SELECT 1 FROM jobs.sources WHERE source_id='src_n' AND deleted_at IS NOT NULL"
        )
        .await
        .is_some(),
        "nus tombstone reached home"
    );
    assert!(one(
        &h.nus,
        "SELECT 1 FROM jobs.sources WHERE source_id='src_h' AND visibility='shared'"
    )
    .await
    .is_some());
    exec(
        &h.home,
        "UPDATE jobs.sources SET deleted_at = now(), updated_at = now() WHERE source_id = 'src_h'",
    )
    .await;
    clean_cycle(&h).await;
    assert!(one(
        &h.nus,
        "SELECT 1 FROM jobs.sources WHERE source_id='src_h' AND deleted_at IS NOT NULL"
    )
    .await
    .is_some());

    // A second cycle over the overlap window is a no-op, not a re-apply.
    let r = clean_cycle(&h).await;
    assert_eq!(
        (r.pushed, r.pulled),
        (0, 0),
        "re-read window must apply as stale"
    );
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn a_hub_never_overwrites_its_own_rows() {
    let Some(h) = hubs().await else { return };
    exec(&h.home, &source_sql("src_h", "home", "original")).await;
    exec(&h.nus, &source_sql("src_n", "nus", "nus-original")).await;
    clean_cycle(&h).await;

    // 1. The peer tampers with its copy of a home row (newer updated_at) —
    //    its outbox never emits a row it does not own.
    exec(&h.nus, "UPDATE jobs.sources SET name = 'tampered', updated_at = now() + interval '1 hour' WHERE source_id = 'src_h'").await;
    clean_cycle(&h).await;
    let name: String = one(
        &h.home,
        "SELECT name FROM jobs.sources WHERE source_id='src_h'",
    )
    .await
    .unwrap()
    .get(0);
    assert_eq!(name, "original");

    // 2. A forged pulled batch: a home row, newer, still labelled home.
    let future = "2099-01-01T00:00:00+00:00";
    let forged = |rec: Value, hub: &str| {
        let records = vec![rec];
        RegistryBatch {
            batch_id: registry::batch_id(hub, "jobs.sources", &records),
            scope: "registry".into(),
            hub: hub.into(),
            table: "jobs.sources".into(),
            records,
        }
    };
    let rec = json!({"source_id": "src_h", "home_hub": "home", "owner_sub": "mallory", "name": "hijacked",
                     "kind": "table", "visibility": "public", "updated_at": future});
    let ack = registry::apply_batch(&h.home, "nus", &forged(rec, "nus"))
        .await
        .unwrap();
    assert_eq!(
        (ack.skipped_own, ack.inserted + ack.updated),
        (1, 0),
        "{ack:?}"
    );

    // 3. Same row relabelled as the peer's (an ownership takeover) — the DB
    //    guard refuses to touch a row this hub owns.
    let rec = json!({"source_id": "src_h", "home_hub": "nus", "owner_sub": "mallory", "name": "hijacked",
                     "kind": "table", "visibility": "public", "updated_at": future});
    let ack = registry::apply_batch(&h.home, "nus", &forged(rec, "nus"))
        .await
        .unwrap();
    assert_eq!(
        (ack.skipped_own, ack.inserted + ack.updated),
        (1, 0),
        "{ack:?}"
    );
    let r = one(
        &h.home,
        "SELECT name, owner_sub, home_hub FROM jobs.sources WHERE source_id='src_h'",
    )
    .await
    .unwrap();
    assert_eq!(
        (
            r.get::<_, String>(0),
            r.get::<_, String>(1),
            r.get::<_, String>(2)
        ),
        ("original".into(), "alice-sub".into(), "home".into())
    );

    // 4. Over HTTP at nus: home pushes rows claiming to be NUS-owned → never applied.
    let rec = json!({"source_id": "src_n", "home_hub": "nus", "owner_sub": "x", "name": "pwned",
                     "kind": "table", "visibility": "public", "updated_at": future});
    let b = forged(rec, "home");
    let (st, ack) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(serde_json::to_value(&b).unwrap()),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{ack}");
    assert_eq!(ack["skipped_own"], 1, "{ack}");
    let name: String = one(
        &h.nus,
        "SELECT name FROM jobs.sources WHERE source_id='src_n'",
    )
    .await
    .unwrap()
    .get(0);
    assert_eq!(name, "nus-original");

    // 5. A batch that names another hub than its credential → 403. A third
    //    hub's rows inside a correctly-labelled batch → rejected, not applied.
    let rec = json!({"source_id": "src_x", "home_hub": "office", "owner_sub": "x", "name": "x",
                     "kind": "table", "updated_at": future});
    let b = forged(rec.clone(), "office");
    let (st, _) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(serde_json::to_value(&b).unwrap()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    let b = forged(rec, "home");
    let (st, ack) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(serde_json::to_value(&b).unwrap()),
    )
    .await;
    assert_eq!(
        (st, ack["rejected"].as_i64()),
        (StatusCode::OK, Some(1)),
        "{ack}"
    );
    assert!(
        one(&h.nus, "SELECT 1 FROM jobs.sources WHERE source_id='src_x'")
            .await
            .is_none()
    );

    // 6. Older versions from the rightful owner are stale.
    let rec = json!({"source_id": "src_h", "home_hub": "home", "owner_sub": "alice-sub", "name": "older",
                     "kind": "table", "updated_at": "2000-01-01T00:00:00+00:00"});
    let (_, ack) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(serde_json::to_value(&forged(rec, "home")).unwrap()),
    )
    .await;
    assert_eq!(ack["stale"], 1, "{ack}");

    // 7. Redelivery of the same batch dedups on the ledger.
    let rec = json!({"source_id": "src_h2", "home_hub": "home", "owner_sub": "alice-sub", "name": "h2",
                     "kind": "table", "updated_at": "2026-01-01T00:00:00+00:00"});
    let b = serde_json::to_value(forged(rec, "home")).unwrap();
    let (_, a1) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(b.clone()),
    )
    .await;
    let (_, a2) = call(&h.nus_app, "POST", "/sync/inbox", NUS_KEY_FOR_HOME, Some(b)).await;
    assert_eq!(
        (a1["inserted"].as_i64(), a2["duplicate"].as_bool()),
        (Some(1), Some(true)),
        "{a1} {a2}"
    );
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn job_tokens_never_leave_and_jobs_and_events_do() {
    let Some(h) = hubs().await else { return };
    exec(
        &h.home,
        "INSERT INTO jobs.jobs (job_id, home_hub, owner_sub, app, runtime, site, spec)
           VALUES ('11111111-1111-1111-1111-111111111111', 'home', 'alice-sub', 'demo', 'lumilake', 'nus', '{}');
         INSERT INTO jobs.job_tokens (token_sha256, job_id, expires_at)
           VALUES ('\\xdeadbeef', '11111111-1111-1111-1111-111111111111', now() + interval '1 hour');
         INSERT INTO jobs.job_events (job_id, kind, status, payload, home_hub)
           SELECT '11111111-1111-1111-1111-111111111111', 'log', NULL, jsonb_build_object('i', g), 'home'
             FROM generate_series(1, 3) g;",
    )
    .await;
    // nus's own events with colliding local ids must not clash with home's.
    exec(
        &h.nus,
        "INSERT INTO jobs.jobs (job_id, home_hub, owner_sub, app, runtime, site, spec)
           VALUES ('22222222-2222-2222-2222-222222222222', 'nus', 'bob-sub', 'demo', 'lumilake', 'nus', '{}');
         INSERT INTO jobs.job_events (job_id, kind, payload, home_hub)
           VALUES ('22222222-2222-2222-2222-222222222222', 'log', '{}', 'nus');",
    )
    .await;
    clean_cycle(&h).await;
    clean_cycle(&h).await;

    assert_eq!(
        n(
            &h.nus,
            "SELECT count(*) FROM jobs.jobs WHERE home_hub='home'"
        )
        .await,
        1
    );
    assert_eq!(
        n(
            &h.nus,
            "SELECT count(*) FROM jobs.job_events WHERE home_hub='home'"
        )
        .await,
        3,
        "events once, no dups"
    );
    assert_eq!(
        n(
            &h.home,
            "SELECT count(*) FROM jobs.job_events WHERE home_hub='nus' AND origin_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        n(&h.nus, "SELECT count(*) FROM jobs.job_tokens").await,
        0,
        "a token never leaves its hub"
    );
    assert_eq!(n(&h.home, "SELECT count(*) FROM jobs.job_tokens").await, 1);

    // The outbox never offers the token table, and the inbox refuses it.
    let since = "";
    let (st, out) = call(
        &h.nus_app,
        "GET",
        &format!("/sync/outbox?scope=registry&since={since}&limit=5000"),
        NUS_KEY_FOR_HOME,
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{out}");
    assert!(out["batches"]
        .as_array()
        .unwrap()
        .iter()
        .all(|b| b["table"] != "jobs.job_tokens"));
    let recs = vec![
        json!({"token_sha256": "\\xdeadbeef", "job_id": "11111111-1111-1111-1111-111111111111"}),
    ];
    let b = RegistryBatch {
        batch_id: registry::batch_id("home", "jobs.job_tokens", &recs),
        scope: "registry".into(),
        hub: "home".into(),
        table: "jobs.job_tokens".into(),
        records: recs,
    };
    let (st, _) = call(
        &h.nus_app,
        "POST",
        "/sync/inbox",
        NUS_KEY_FOR_HOME,
        Some(serde_json::to_value(&b).unwrap()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(n(&h.nus, "SELECT count(*) FROM jobs.job_tokens").await, 0);
}

async fn put_blob(st: &AppState, owner: &str, bytes: &[u8]) -> String {
    let sha = hex::encode(Sha256::digest(bytes));
    let key = lumid_platform::handlers::me_blobs::tenant_key(owner, &sha);
    st.blob_store
        .put(
            &ObjPath::from(key.as_str()),
            Bytes::copy_from_slice(bytes).into(),
        )
        .await
        .unwrap();
    st.pool
        .get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO raw.tenant_blobs (owner_sub, blob_sha256, storage_key, content_type, size_bytes, suggested_name)
             VALUES ($1, $2, $3, 'application/octet-stream', $4, 'données.bin')",
            &[&owner, &sha, &key, &(bytes.len() as i64)],
        )
        .await
        .unwrap();
    sha
}

async fn read_blob(st: &AppState, owner: &str, sha: &str) -> Option<Vec<u8>> {
    let key = lumid_platform::handlers::me_blobs::tenant_key(owner, sha);
    match st.blob_store.get(&ObjPath::from(key.as_str())).await {
        Ok(g) => Some(g.bytes().await.unwrap().to_vec()),
        Err(_) => None,
    }
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn small_blobs_copy_both_ways_and_large_ones_never() {
    let Some(h) = hubs().await else { return };
    let small_home = put_blob(&h.home, "alice-sub", b"small home bytes").await;
    let big_home = put_blob(&h.home, "alice-sub", &vec![7u8; CAP as usize + 1]).await;
    let small_nus = put_blob(&h.nus, "bob-sub", b"small nus bytes").await;
    let r = clean_cycle(&h).await;
    assert_eq!((r.blobs_pushed, r.blobs_pulled), (1, 1), "{r:?}");

    assert_eq!(
        read_blob(&h.nus, "alice-sub", &small_home).await.as_deref(),
        Some(&b"small home bytes"[..])
    );
    assert!(
        read_blob(&h.nus, "alice-sub", &big_home).await.is_none(),
        "over the cap: never copies"
    );
    assert_eq!(
        read_blob(&h.home, "bob-sub", &small_nus).await.as_deref(),
        Some(&b"small nus bytes"[..])
    );
    // The index row moved with the bytes and names the upload's hub.
    let r = one(&h.nus, &format!("SELECT home_hub, suggested_name, size_bytes FROM raw.tenant_blobs WHERE blob_sha256='{small_home}'")).await.unwrap();
    assert_eq!(
        (
            r.get::<_, String>(0),
            r.get::<_, String>(1),
            r.get::<_, i64>(2)
        ),
        ("home".into(), "données.bin".into(), 16)
    );
    assert!(one(
        &h.nus,
        &format!("SELECT 1 FROM raw.tenant_blobs WHERE blob_sha256='{big_home}'")
    )
    .await
    .is_none());
    let hh: String = one(
        &h.home,
        &format!("SELECT home_hub FROM raw.tenant_blobs WHERE blob_sha256='{small_nus}'"),
    )
    .await
    .unwrap()
    .get(0);
    assert_eq!(hh, "nus");

    // Copy-if-missing: a second cycle moves nothing; nus never re-offers home's blob back.
    let r = clean_cycle(&h).await;
    assert_eq!((r.blobs_pushed, r.blobs_pulled), (0, 0));

    // The receiver re-hashes: wrong bytes for a sha are refused.
    let fake = hub::blobs::BlobIndexRow {
        owner_sub: "eve".into(),
        blob_sha256: "ab".repeat(32),
        content_type: "application/octet-stream".into(),
        size_bytes: 3,
        suggested_name: None,
        created_at: chrono::Utc::now(),
        home_hub: "home".into(),
    };
    let resp = h
        .home
        .http
        .put(format!(
            "{}/sync/blobs/eve/{}",
            h.peer.base_url,
            "ab".repeat(32)
        ))
        .bearer_auth(NUS_KEY_FOR_HOME)
        .header(
            hub::blobs::INDEX_HEADER,
            hex::encode(serde_json::to_vec(&fake).unwrap()),
        )
        .body("xyz")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(one(
        &h.nus,
        "SELECT 1 FROM raw.tenant_blobs WHERE owner_sub='eve'"
    )
    .await
    .is_none());
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn catalog_mirror_carries_the_peers_tables() {
    let Some(h) = hubs().await else { return };
    exec(&h.nus, "CREATE SCHEMA nusdata; CREATE TABLE nusdata.cohort (id int); INSERT INTO nusdata.cohort SELECT generate_series(1,100); ANALYZE nusdata.cohort;").await;
    let r = hub::catalog::refresh(&h.nus.pool, "nus", "nus-data", &[])
        .await
        .unwrap();
    assert!(r.changed >= 1, "{r:?}");
    let again = hub::catalog::refresh(&h.nus.pool, "nus", "nus-data", &[])
        .await
        .unwrap();
    assert_eq!(again.changed, 0, "an unchanged table keeps its version");
    clean_cycle(&h).await;
    let row = one(&h.home, "SELECT home_hub, kind, row_est FROM jobs.catalog_mirror WHERE hub_id='nus-data' AND schema_name='nusdata' AND table_name='cohort'").await.expect("mirrored");
    assert_eq!(row.get::<_, String>(0), "nus");
    assert_eq!(row.get::<_, String>(1), "table");
    assert_eq!(row.get::<_, Option<i64>>(2), Some(100));
    // Home's planner sees it; a dropped table tombstones and that syncs too.
    exec(&h.nus, "DROP TABLE nusdata.cohort").await;
    assert_eq!(
        hub::catalog::refresh(&h.nus.pool, "nus", "nus-data", &[])
            .await
            .unwrap()
            .tombstoned,
        1
    );
    clean_cycle(&h).await;
    assert!(one(
        &h.home,
        "SELECT 1 FROM jobs.catalog_mirror WHERE table_name='cohort' AND deleted_at IS NOT NULL"
    )
    .await
    .is_some());
}

#[tokio::test]
#[ignore = "requires a throwaway Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn status_reports_lag_and_errors_and_peer_keys_are_confined() {
    let Some(h) = hubs().await else { return };
    let (_, s0) = call(&h.home_app, "GET", "/sync/status", HOME_ADMIN, None).await;
    assert_eq!(
        s0["peers"].as_array().unwrap().len(),
        0,
        "never synced: no rows yet"
    );

    exec(&h.nus, &source_sql("src_n", "nus", "n")).await;
    clean_cycle(&h).await;
    let (st, s) = call(&h.home_app, "GET", "/sync/status", HOME_ADMIN, None).await;
    assert_eq!(st, StatusCode::OK, "{s}");
    let p = &s["peers"][0];
    assert_eq!(p["peer"], "nus");
    assert!(p["synced_age_s"].as_i64().unwrap() <= 2, "{s}");
    assert_eq!(p["failing"], false);
    for (scope, dir) in [
        ("registry", "push"),
        ("registry", "pull"),
        ("blobs", "push"),
        ("blobs", "pull"),
    ] {
        assert!(
            p["scopes"][scope][dir]["last_success_at"].is_string(),
            "{scope}/{dir}: {s}"
        );
    }
    assert!(
        p["scopes"]["registry"]["pull"]["cursor_age_s"]
            .as_i64()
            .is_some(),
        "{s}"
    );

    // The peer side records what home did to it.
    let (_, ns) = call(&h.nus_app, "GET", "/sync/status", NUS_ADMIN, None).await;
    assert_eq!(ns["peers"][0]["peer"], "home", "{ns}");
    assert!(
        ns["peers"][0]["scopes"]["registry"]["outbound"]["last_success_at"].is_string(),
        "{ns}"
    );

    // A dead peer: the cycle fails, the cursor holds, status says so.
    let dead = HubPeer {
        base_url: "http://127.0.0.1:9".into(),
        ..h.peer.clone()
    };
    let before = hub::load_cursor(&h.home.pool, "nus", "registry", "pull")
        .await
        .unwrap();
    let r = sched::run_cycle(&h.home, &dead).await;
    assert!(!r.errors.is_empty());
    assert_eq!(
        hub::load_cursor(&h.home.pool, "nus", "registry", "pull")
            .await
            .unwrap(),
        before
    );
    let (_, s) = call(&h.home_app, "GET", "/sync/status", HOME_ADMIN, None).await;
    assert_eq!(s["peers"][0]["failing"], true, "{s}");
    assert!(
        s["peers"][0]["scopes"]["registry"]["pull"]["last_error"].is_string(),
        "{s}"
    );

    // A hub peer key reaches /sync/* only — no data plane, no admin.
    for (m, path) in [
        ("GET", "/catalog/schemas"),
        ("GET", "/admin/sync/status"),
        ("GET", "/blobs"),
    ] {
        let (st, _) = call(&h.nus_app, m, path, NUS_KEY_FOR_HOME, None).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{path}");
    }
    let (st, _) = call(
        &h.nus_app,
        "POST",
        "/sync/apply/nusdata/cohort",
        NUS_KEY_FOR_HOME,
        Some(
            json!({"batch_id": uuid::Uuid::new_v4(), "source": "x", "source_endpoint": "y",
                    "source_run_id": uuid::Uuid::new_v4(), "records": [{"id": 1}]}),
        ),
    )
    .await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "hub peer key on the data-push plane"
    );
    // …and a non-sync key cannot use the hub endpoints.
    let (st, _) = call(
        &h.nus_app,
        "GET",
        "/sync/outbox?scope=registry",
        NUS_ADMIN,
        None,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}
