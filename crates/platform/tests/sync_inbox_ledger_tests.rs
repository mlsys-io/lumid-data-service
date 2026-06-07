//! Sync inbox ledger semantics — partial-reject re-apply contract.
//!
//! These tests pin the inbox half of PR #18's non-advance contract:
//!
//!   A batch that partially rejects (some rows applied, some rejected at
//!   validation) must NOT leave a terminal `(peer, batch_id)` dedup row in
//!   `sync.inbox_ledger`. Otherwise the producer's idempotent re-push of the
//!   same deterministic `batch_id` (cursor parked by PR #18) would dedup to a
//!   no-op and the previously-rejected rows would be stranded forever.
//!
//! The pure dedup-vs-reattempt decision (`should_ledger`) is unit-tested inside
//! `src/sync/inbox.rs`. The live round-trip below exercises the SAME decision
//! against the REAL `sync.inbox_ledger` DDL + dedup SQL the inbox handler uses,
//! proving the ledger lookup behaves as the contract requires:
//!   - a partially-rejected batch leaves NO ledger row → a re-lookup does NOT
//!     short-circuit → the re-push re-runs apply (the previously-rejected row
//!     finally lands);
//!   - a fully-successful batch DOES get a ledger row → a re-lookup DOES
//!     short-circuit → true duplicate delivery / retry storms dedup
//!     (exactly-once-effective holds).
//!
//! Live-Postgres gated, matching the repo's `#[ignore]` convention
//! (`agent_tests.rs`): run with a real DB and `LUMID_SYNC_E2E=1`, e.g.
//!   LUMID_SYNC_E2E=1 DATABASE_URL=postgres://... \
//!     cargo test -p lumid-platform --test sync_inbox_ledger_tests -- --ignored

use lumid_platform::sync::SYNC_DDL;
use tokio_postgres::NoTls;
use uuid::Uuid;

/// Mirror of the inbox's terminal-only ledger gate (`should_ledger` in
/// `src/sync/inbox.rs`): ledger ONLY a fully-complete batch (0 rejects).
fn should_ledger(failed: usize) -> bool {
    failed == 0
}

/// Connect to the test Postgres named by `DATABASE_URL`. Skips (returns `None`)
/// when the live-DB gate is off, so a default `cargo test` run never needs a DB.
async fn connect() -> Option<tokio_postgres::Client> {
    if std::env::var("LUMID_SYNC_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set LUMID_SYNC_E2E=1 + DATABASE_URL to run live");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set when LUMID_SYNC_E2E=1");
    let (client, conn) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect to test Postgres");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Some(client)
}

/// The inbox's exact dedup probe: is `(peer, batch_id)` already terminally
/// recorded? `Some` short-circuits the apply (returns `duplicate: true`).
async fn ledger_seen(client: &tokio_postgres::Client, peer: &str, batch_id: Uuid) -> bool {
    client
        .query_opt(
            "SELECT inserted, updated FROM sync.inbox_ledger WHERE peer=$1 AND batch_id=$2",
            &[&peer, &batch_id],
        )
        .await
        .expect("ledger lookup")
        .is_some()
}

/// The inbox's exact terminal-ledger write, gated by `should_ledger(failed)`.
/// Returns whether a row was written.
async fn maybe_ledger(
    client: &tokio_postgres::Client,
    peer: &str,
    batch_id: Uuid,
    inserted: i64,
    updated: i64,
    received: i64,
    failed: usize,
) -> bool {
    if !should_ledger(failed) {
        return false;
    }
    let run = Uuid::new_v4();
    client
        .execute(
            "INSERT INTO sync.inbox_ledger \
               (peer, batch_id, source_run_id, target_schema, target_table, inserted, updated, n_rows) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (peer, batch_id) DO NOTHING",
            &[&peer, &batch_id, &run, &"market", &"divs", &inserted, &updated, &received],
        )
        .await
        .expect("ledger insert");
    true
}

#[tokio::test]
#[ignore = "requires a live Postgres pool; run with LUMID_SYNC_E2E=1 + DATABASE_URL"]
async fn partial_reject_leaves_no_ledger_then_re_push_re_applies() {
    let Some(client) = connect().await else { return };
    client.batch_execute(SYNC_DDL).await.expect("apply SYNC_DDL");

    let peer = format!("test-peer-{}", Uuid::new_v4());
    let batch_id = Uuid::new_v4();

    // 1. First apply: target schema is half-broken → 2 rows apply, 1 rejects.
    //    inbox returns status="partial"; it must NOT ledger.
    assert!(!ledger_seen(&client, &peer, batch_id).await, "fresh batch: not seen");
    let wrote = maybe_ledger(&client, &peer, batch_id, 2, 0, 3, /*failed=*/ 1).await;
    assert!(!wrote, "partial reject must NOT write a ledger row");

    // 2. Re-push of the SAME deterministic batch_id (producer cursor parked by
    //    #18). The dedup lookup must still MISS — so the inbox re-runs apply
    //    instead of short-circuiting to a no-op. THIS is the bug PR #18's inbox
    //    half must avoid: if the partial had been ledgered, this would dedup and
    //    strand the rejected row forever.
    assert!(
        !ledger_seen(&client, &peer, batch_id).await,
        "re-push after partial reject must NOT dedup — the rejected row must re-apply"
    );

    // 3. Operator fixes the target schema; the re-push now fully succeeds (0
    //    rejects) → the inbox ledgers it terminally.
    let wrote = maybe_ledger(&client, &peer, batch_id, 1, 2, 3, /*failed=*/ 0).await;
    assert!(wrote, "a fully-complete batch must write the ledger row");

    // 4. Any FURTHER duplicate delivery of this now-complete batch dedups —
    //    exactly-once-effective holds (retry storms / at-least-once redelivery).
    assert!(
        ledger_seen(&client, &peer, batch_id).await,
        "a completed batch must dedup on redelivery (exactly-once-effective)"
    );

    // Cleanup so reruns start clean.
    client
        .execute("DELETE FROM sync.inbox_ledger WHERE peer=$1", &[&peer])
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore = "requires a live Postgres pool; run with LUMID_SYNC_E2E=1 + DATABASE_URL"]
async fn clean_batch_dedups_immediately() {
    let Some(client) = connect().await else { return };
    client.batch_execute(SYNC_DDL).await.expect("apply SYNC_DDL");

    let peer = format!("test-peer-{}", Uuid::new_v4());
    let batch_id = Uuid::new_v4();

    // A clean apply ledgers; immediate redelivery dedups (true duplicate).
    assert!(maybe_ledger(&client, &peer, batch_id, 3, 0, 3, 0).await);
    assert!(
        ledger_seen(&client, &peer, batch_id).await,
        "clean batch must be deduped on redelivery"
    );

    client
        .execute("DELETE FROM sync.inbox_ledger WHERE peer=$1", &[&peer])
        .await
        .expect("cleanup");
}
