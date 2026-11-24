//! Live-DB checks for the `jobs` registry bootstrap (schema, reader lock-out,
//! seeding, single-writer ownership). Gated so default `cargo test` needs no DB:
//!
//!   LUMID_JOBS_E2E=1 DATABASE_URL=postgres://… \
//!     cargo test -p lumid-platform --test jobs_registry_tests -- --ignored --test-threads=1
//!
//! Point it at a THROWAWAY database: it drops and recreates the `jobs` schema.

use deadpool_postgres::{Config, Pool, Runtime};
use lumid_platform::jobs::{schema, seed};
use tokio_postgres::NoTls;

async fn pool() -> Option<Pool> {
    if std::env::var("LUMID_JOBS_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_JOBS_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let mut cfg = Config::new();
    cfg.url = Some(std::env::var("DATABASE_URL").expect("DATABASE_URL"));
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    let c = pool.get().await.expect("connect");
    // A fresh `jobs` schema, a `provenance.runs` to extend, and a reader role
    // set up exactly like deploy/standalone/reader_role.sql (default
    // privileges that WOULD grant every new table).
    c.batch_execute(
        "DROP SCHEMA IF EXISTS jobs CASCADE;
         CREATE SCHEMA IF NOT EXISTS provenance;
         CREATE TABLE IF NOT EXISTS provenance.runs (run_id uuid PRIMARY KEY DEFAULT gen_random_uuid());
         DO $r$ BEGIN
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lumid_reader') THEN
             CREATE ROLE lumid_reader NOLOGIN;
           END IF;
         END $r$;
         ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO lumid_reader;
         ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO lumid_reader;",
    )
    .await
    .expect("fixture");
    Some(pool)
}

async fn scalar_bool(pool: &Pool, sql: &str) -> bool {
    pool.get()
        .await
        .unwrap()
        .query_one(sql, &[])
        .await
        .unwrap()
        .get(0)
}
async fn scalar_i64(pool: &Pool, sql: &str) -> i64 {
    pool.get()
        .await
        .unwrap()
        .query_one(sql, &[])
        .await
        .unwrap()
        .get(0)
}

fn routes(json: &str) -> Vec<seed::RouteSpec> {
    seed::parse_routes(json).unwrap()
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn schema_is_idempotent_concurrent_and_hidden_from_the_reader() {
    let Some(pool) = pool().await else { return };

    // Eight concurrent first boots (replicas) must all succeed. The DDL AND
    // the reader REVOKE share the bootstrap lock: concurrent GRANT/REVOKE on
    // the same objects otherwise fails with "tuple concurrently updated".
    let results =
        futures_util::future::join_all((0..8).map(|_| schema::migrate(&pool, "lumid_reader")))
            .await;
    for (i, r) in results.into_iter().enumerate() {
        r.unwrap_or_else(|e| panic!("concurrent migrate #{i}: {e}"));
    }
    // And a third, on an existing schema.
    schema::migrate(&pool, "lumid_reader").await.unwrap();

    for rel in schema::JOBS_RELATIONS {
        assert!(
            scalar_bool(&pool, &format!("SELECT to_regclass('{rel}') IS NOT NULL")).await,
            "{rel} missing"
        );
        assert!(
            !scalar_bool(
                &pool,
                &format!("SELECT has_table_privilege('lumid_reader','{rel}','SELECT')")
            )
            .await,
            "{rel} readable by the reader role"
        );
    }
    assert!(
        !scalar_bool(
            &pool,
            "SELECT has_schema_privilege('lumid_reader','jobs','USAGE')"
        )
        .await
    );
    assert!(
        scalar_bool(
            &pool,
            "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='provenance.runs'::regclass \
             AND attname='job_id' AND NOT attisdropped)"
        )
        .await
    );
    // An unset / nonexistent reader role is a no-op, not a boot failure.
    schema::migrate(&pool, "").await.unwrap();
    schema::migrate(&pool, "no_such_role_xyz").await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn seeding_is_idempotent_tombstones_and_never_touches_a_peers_rows() {
    let Some(pool) = pool().await else { return };
    schema::migrate(&pool, "lumid_reader").await.unwrap();

    // A row the NUS hub owns, with an id the home hub will also seed.
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, site, visibility)
             VALUES ('nus', 'nus', 'system', 'nus', 'estate', 'nus', 'public');
             INSERT INTO jobs.site_routes (route_id, from_site, source_id, kind, url, home_hub)
             VALUES ('nus-local', 'nus', 'nus', 'lan', 'http://nus-hub:5101', 'nus');",
        )
        .await
        .unwrap();

    let estate = vec![
        "findata".to_string(),
        "lumid-data".to_string(),
        "nus".to_string(),
    ];
    let r1 = routes(
        r#"[{"route_id":"home-lan-lumid-data","from_site":"home","source_id":"lumid-data","kind":"lan","url":"http://192.168.7.155:5101","pref":10},
            {"route_id":"office-tailnet-lumid-data","from_site":"office","source_id":"lumid-data","kind":"tailnet","url":"http://100.115.66.10:5101","pref":20},
            {"route_id":"nus-local","from_site":"home","source_id":"nus","kind":"public","url":"https://evil.example"}]"#,
    );

    let rep = seed::apply(&pool, "home", "home", &estate, &r1)
        .await
        .unwrap();
    // findata + lumid-data created; 'nus' belongs to the nus hub → untouched.
    assert_eq!(rep.estate_upserted, 2, "{rep:?}");
    // two home routes created; 'nus-local' belongs to nus → untouched.
    assert_eq!(rep.routes_upserted, 2, "{rep:?}");
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.sources WHERE source_id='nus' AND home_hub='nus' AND site='nus'").await,
        1
    );
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.site_routes WHERE route_id='nus-local' AND url='http://nus-hub:5101' AND home_hub='nus'").await,
        1,
        "a peer-owned route was overwritten"
    );
    assert_eq!(
        scalar_i64(
            &pool,
            "SELECT count(*) FROM jobs.source_locations WHERE home_hub='home' AND site='home'"
        )
        .await,
        2
    );

    // Same config again: nothing changes (no version churn to sync).
    let rep2 = seed::apply(&pool, "home", "home", &estate, &r1)
        .await
        .unwrap();
    assert_eq!(
        rep2,
        seed::SeedReport::default(),
        "re-seed changed rows: {rep2:?}"
    );

    // Health written by a prober survives a re-seed.
    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE jobs.site_routes SET health='up', latency_ms=3, health_at=now() WHERE route_id='home-lan-lumid-data'",
            &[],
        )
        .await
        .unwrap();

    // Drop the office route from config → tombstoned (not deleted), so a peer sees it go.
    let r2 = routes(
        r#"[{"route_id":"home-lan-lumid-data","from_site":"home","source_id":"lumid-data","kind":"lan","url":"http://192.168.7.155:5101","pref":10}]"#,
    );
    let rep3 = seed::apply(&pool, "home", "home", &estate, &r2)
        .await
        .unwrap();
    assert_eq!(rep3.routes_tombstoned, 1, "{rep3:?}");
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.site_routes WHERE route_id='office-tailnet-lumid-data' AND deleted_at IS NOT NULL").await,
        1
    );
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.site_routes WHERE route_id='home-lan-lumid-data' AND health='up' AND latency_ms=3").await,
        1,
        "re-seed reset prober health"
    );
    // The peer's route is still alive (tombstoning is scoped to this hub).
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.site_routes WHERE route_id='nus-local' AND deleted_at IS NULL").await,
        1
    );

    // Bringing a tombstoned route back revives it.
    let rep4 = seed::apply(&pool, "home", "home", &estate, &r1)
        .await
        .unwrap();
    assert_eq!(rep4.routes_upserted, 1, "{rep4:?}");
    assert_eq!(
        scalar_i64(&pool, "SELECT count(*) FROM jobs.site_routes WHERE route_id='office-tailnet-lumid-data' AND deleted_at IS NULL").await,
        1
    );

    // The jobs table enforces its invariants.
    let c = pool.get().await.unwrap();
    let no_app = c
        .execute(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, spec) VALUES ('home','u','', 'flowmesh','{}')",
            &[],
        )
        .await;
    assert!(no_app.is_err(), "empty app accepted");
    let bad_runtime = c
        .execute(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, spec) VALUES ('home','u','lqt','k8s','{}')",
            &[],
        )
        .await;
    assert!(bad_runtime.is_err(), "unknown runtime accepted");
    c.execute(
        "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, spec, idempotency_key) VALUES ('home','u','lqt','lumilake','{}','k1')",
        &[],
    )
    .await
    .unwrap();
    let dup = c
        .execute(
            "INSERT INTO jobs.jobs (home_hub, owner_sub, app, runtime, spec, idempotency_key) VALUES ('home','u','lqt','lumilake','{}','k1')",
            &[],
        )
        .await;
    assert!(dup.is_err(), "duplicate idempotency key accepted");
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn retrieval_runs_migration_survives_concurrent_boots_and_hides_from_reader() {
    let Some(pool) = pool().await else { return };
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP TABLE IF EXISTS provenance.retrieval_runs")
        .await
        .unwrap();
    let results = futures_util::future::join_all(
        (0..8).map(|_| lumid_platform::handlers::retrieve::migrate(&pool, "lumid_reader")),
    )
    .await;
    for (i, r) in results.into_iter().enumerate() {
        r.unwrap_or_else(|e| panic!("concurrent retrieval migrate #{i}: {e}"));
    }
    assert!(
        !scalar_bool(
            &pool,
            "SELECT has_table_privilege('lumid_reader','provenance.retrieval_runs','SELECT')"
        )
        .await,
        "retrieval_runs readable by the reader role"
    );
}

#[tokio::test]
#[ignore = "requires a live Postgres; LUMID_JOBS_E2E=1 + DATABASE_URL"]
async fn planner_inputs_come_from_the_catalog_and_health_is_owner_written() {
    use lumid_platform::jobs::{inputs, planner::Size, refs::parse};
    let Some(pool) = pool().await else { return };
    schema::migrate(&pool, "lumid_reader").await.unwrap();
    let c = pool.get().await.unwrap();
    c.batch_execute(
        "DROP SCHEMA IF EXISTS est CASCADE; CREATE SCHEMA est;
         CREATE TABLE est.big AS SELECT g AS id, repeat('x', 200) AS pad FROM generate_series(1, 20000) g;
         CREATE TABLE est.small (id int);
         INSERT INTO jobs.catalog_mirror (hub_id, schema_name, table_name, size_bytes_est)
           VALUES ('findata', 'market', 'ohlc_1d', 7000000000), ('findata', 'market', 'trades', 1000);
         INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, site, visibility)
           VALUES ('ld', 'home', 'system', 'ld', 'estate', 'home', 'public');
         INSERT INTO jobs.source_locations (source_id, site, role, home_hub) VALUES ('ld', 'nas', 'primary', 'home');
         INSERT INTO jobs.site_routes (route_id, from_site, source_id, kind, url, home_hub) VALUES
           ('mine', 'office', 'ld', 'tailnet', 'http://t', 'home'),
           ('theirs', 'nus', 'ld', 'lan', 'http://n', 'nus');",
    )
    .await
    .unwrap();
    drop(c);

    // Table size: an upper bound from the catalog, no scan.
    match inputs::estimate(
        &pool,
        "lumid-data",
        &parse("lumid-data://est.big").unwrap(),
        None,
    )
    .await
    .unwrap()
    {
        Size::UpperBound(n) => assert!(n > 20000 * 200, "{n}"),
        other => panic!("{other:?}"),
    }
    let schema_size = inputs::estimate(
        &pool,
        "lumid-data",
        &parse("lumid-data://est").unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(schema_size, Size::UpperBound(n) if n > 20000 * 200));
    assert_eq!(
        inputs::estimate(
            &pool,
            "lumid-data",
            &parse("lumid-data://est.missing").unwrap(),
            None
        )
        .await
        .unwrap(),
        Size::Unknown
    );
    // Another instance → the synced mirror.
    assert_eq!(
        inputs::estimate(
            &pool,
            "lumid-data",
            &parse("findata://market.ohlc_1d").unwrap(),
            None
        )
        .await
        .unwrap(),
        Size::UpperBound(7_000_000_000)
    );
    assert_eq!(
        inputs::estimate(
            &pool,
            "lumid-data",
            &parse("findata://market").unwrap(),
            None
        )
        .await
        .unwrap(),
        Size::UpperBound(7_000_001_000)
    );
    // A declared bound always wins.
    assert_eq!(
        inputs::estimate(
            &pool,
            "lumid-data",
            &parse("findata://market.ohlc_1d").unwrap(),
            Some(5_000_000)
        )
        .await
        .unwrap(),
        Size::Declared(5_000_000)
    );

    // Location: the primary location beats the source row's site.
    assert_eq!(
        inputs::source_site(&pool, "ld").await.unwrap().as_deref(),
        Some("nas")
    );
    assert_eq!(inputs::source_site(&pool, "nope").await.unwrap(), None);

    // Health: only the owning hub writes it.
    assert!(
        inputs::record_route_health(&pool, "home", "mine", "up", Some(4))
            .await
            .unwrap()
    );
    assert!(
        !inputs::record_route_health(&pool, "home", "theirs", "down", None)
            .await
            .unwrap()
    );
    assert!(
        inputs::record_route_health(&pool, "home", "mine", "sideways", None)
            .await
            .is_err()
    );
    let routes = inputs::load_routes(&pool).await.unwrap();
    let mine = routes.iter().find(|r| r.route_id == "mine").unwrap();
    assert_eq!((mine.health.as_str(), mine.latency_ms), ("up", Some(4)));
    assert!(mine.health_at.is_some());
    let theirs = routes.iter().find(|r| r.route_id == "theirs").unwrap();
    assert_eq!(theirs.health, "unknown");
}
