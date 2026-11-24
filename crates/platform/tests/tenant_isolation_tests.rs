//! Live-DB check of the private-object rules in `handlers::retrieve::may_read_key`:
//! two NON-admin users (A, B) plus the privileged roles, against tenant blobs
//! and materialized retrieval outputs.
//!
//! Gated like the sync e2e test, so a default `cargo test` needs no database:
//!
//!   LUMID_ISOLATION_E2E=1 DATABASE_URL=postgres://user:pw@host:port/db \
//!     cargo test -p lumid-platform --test tenant_isolation_tests -- --ignored
//!
//! Point it at a THROWAWAY database: it creates `provenance.retrieval_runs`.

use deadpool_postgres::{Config, Pool, Runtime};
use lumid_platform::auth::Identity;
use lumid_platform::handlers::retrieve::{may_read_key, migrate, record_owner};
use tokio_postgres::NoTls;
use uuid::Uuid;

const PREFIX: &str = "retrievals";

fn ident(sub: &str, role: &str) -> Identity {
    Identity {
        sub: sub.into(),
        role: role.into(),
        email: None,
        active: true,
        scopes: vec![],
    }
}

async fn pool() -> Option<Pool> {
    if std::env::var("LUMID_ISOLATION_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_ISOLATION_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut cfg = Config::new();
    cfg.url = Some(url);
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("create pool");
    pool.get()
        .await
        .expect("connect")
        // Same lock as the platform bootstrap: both tests set up concurrently.
        .batch_execute(
            "SELECT pg_advisory_xact_lock(7815273027795448930); \
             CREATE SCHEMA IF NOT EXISTS provenance",
        )
        .await
        .expect("provenance schema");
    migrate(&pool).await.expect("migrate retrieval_runs");
    Some(pool)
}

#[tokio::test]
#[ignore = "requires a live Postgres; run with LUMID_ISOLATION_E2E=1 + DATABASE_URL"]
async fn retrieval_outputs_are_owner_only() {
    let Some(pool) = pool().await else { return };
    let a = ident("user-a", "user");
    let b = ident("user-b", "user");

    let run_a = Uuid::new_v4().to_string();
    record_owner(&pool, &run_a, &a.sub).await.unwrap();
    let key = format!("{PREFIX}/{run_a}/result.jsonl");

    assert!(
        may_read_key(&pool, Some(&a), &key, PREFIX).await.unwrap(),
        "owner reads"
    );
    assert!(
        !may_read_key(&pool, Some(&b), &key, PREFIX).await.unwrap(),
        "B denied"
    );
    assert!(
        !may_read_key(&pool, None, &key, PREFIX).await.unwrap(),
        "anonymous denied"
    );
    for role in ["admin", "super_admin", "local"] {
        assert!(
            may_read_key(&pool, Some(&ident("ops", role)), &key, PREFIX)
                .await
                .unwrap(),
            "{role} reads"
        );
    }

    // A run with no ownership row (pre-dates the check, or anonymous):
    // privileged roles only.
    let orphan = format!("{PREFIX}/{}/result.jsonl", Uuid::new_v4());
    assert!(!may_read_key(&pool, Some(&a), &orphan, PREFIX)
        .await
        .unwrap());
    assert!(
        may_read_key(&pool, Some(&ident("ops", "local")), &orphan, PREFIX)
            .await
            .unwrap()
    );

    // A non-UUID run segment never matches an owner.
    let junk = format!("{PREFIX}/not-a-uuid/result.jsonl");
    assert!(!may_read_key(&pool, Some(&a), &junk, PREFIX).await.unwrap());

    // Recording the same run twice keeps the FIRST owner (B can't claim A's run).
    record_owner(&pool, &run_a, &b.sub).await.unwrap();
    assert!(!may_read_key(&pool, Some(&b), &key, PREFIX).await.unwrap());
}

#[tokio::test]
#[ignore = "requires a live Postgres; run with LUMID_ISOLATION_E2E=1 + DATABASE_URL"]
async fn tenant_blobs_are_owner_only_and_public_keys_stay_open() {
    let Some(pool) = pool().await else { return };
    let a = ident("user-a", "user");
    let b = ident("user-b", "user");
    let key = "tenants/user-a/sha256=00ab";

    assert!(may_read_key(&pool, Some(&a), key, PREFIX).await.unwrap());
    assert!(!may_read_key(&pool, Some(&b), key, PREFIX).await.unwrap());
    assert!(!may_read_key(&pool, None, key, PREFIX).await.unwrap());
    assert!(
        may_read_key(&pool, Some(&ident("ops", "admin")), key, PREFIX)
            .await
            .unwrap()
    );

    // Ordinary content-addressed blobs are not private: unchanged behaviour.
    for k in ["blob/sha256=00ab", "retrievals-archive/x/y.jsonl"] {
        assert!(
            may_read_key(&pool, Some(&b), k, PREFIX).await.unwrap(),
            "{k}"
        );
        assert!(
            may_read_key(&pool, None, k, PREFIX).await.unwrap(),
            "{k} anon"
        );
    }
}
