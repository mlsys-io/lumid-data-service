//! Platform-owned bookkeeping tables.
//!
//! The ingest, catalog, sync, and blob planes read these tables on a normal
//! request, so a fresh deployment must create them before serving traffic.
//!
//! Shape is kept compatible with the two pre-existing bootstraps
//! (`findata/ddl/01_schema.sql` + `20_ingress.sql` and
//! `lqt-data/ddl/00_provenance.sql`): counters and registry columns stay
//! nullable, because the sync inbox upserts peer rows through
//! `jsonb_populate_record`, which turns an absent/null key into NULL rather
//! than the column default.
//!
//! Safety properties of [`migrate`]:
//! * **Serialized across replicas** — the whole batch runs as one implicit
//!   transaction that first takes a transaction-scoped advisory lock, so pods
//!   booting together on a fresh DB cannot race the `to_regclass` guards.
//! * **No-op on an existing DB** — schemas and tables are created only when
//!   absent (no `CREATE SCHEMA IF NOT EXISTS`, which still needs database
//!   CREATE privilege when the schema exists), and indexes/seeds only inside
//!   the guard that just created their table.
//! * Column backfills for older registries run separately and best-effort
//!   (see [`PLATFORM_COLUMNS_DDL`]): they need table ownership, and a missing
//!   privilege there must not turn a degraded feature into a boot failure.

use deadpool_postgres::Pool;
use tokio_postgres::Client;

/// `pg_advisory_xact_lock` key that serializes platform bootstrap DDL.
pub const MIGRATION_LOCK_KEY: i64 = 0x6c75_6d69_6470_7462; // "lumidptb"

/// Idempotent DDL for every platform-owned bookkeeping relation.
pub const PLATFORM_TABLES_DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);

DO $platform_tables$
BEGIN
  IF to_regnamespace('provenance') IS NULL THEN
    CREATE SCHEMA provenance;
  END IF;
  IF to_regnamespace('raw') IS NULL THEN
    CREATE SCHEMA raw;
  END IF;
  -- gen_random_uuid() is core only from Postgres 13; older servers need
  -- pgcrypto. Only attempted where a table that uses it is still missing.
  IF current_setting('server_version_num')::int < 130000
     AND (to_regclass('provenance.runs') IS NULL
          OR to_regclass('provenance.ingress_proposals') IS NULL
          OR to_regclass('provenance.webhooks') IS NULL) THEN
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.api_sources') IS NULL THEN
    CREATE TABLE provenance.api_sources (
      source          text        NOT NULL,
      display_name    text,
      base_url        text,
      api_namespace   text,
      auth_mode       text,
      auth_param      text,
      rate_limit      jsonb,
      docs_url        text,
      notes           text,
      created_at      timestamptz DEFAULT now(),
      PRIMARY KEY (source)
    );
    -- Source for the catch-all ingress endpoint (matches findata 20_ingress.sql).
    INSERT INTO provenance.api_sources (source) VALUES ('ingress');
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.endpoints') IS NULL THEN
    CREATE TABLE provenance.endpoints (
      endpoint_id     text        NOT NULL,
      source          text        REFERENCES provenance.api_sources(source),
      path_template   text,
      http_method     text        DEFAULT 'GET',
      pagination      text,
      scope           text,
      symbol_universe text,
      target_schema   text,
      target_table    text,
      field_map       jsonb,
      response_shape  jsonb,
      shape_hash      text,
      cadence         text,
      active          boolean     DEFAULT true,
      notes           text,
      created_at      timestamptz DEFAULT now(),
      updated_at      timestamptz DEFAULT now(),
      PRIMARY KEY (endpoint_id)
    );
    CREATE INDEX endpoints_by_source
      ON provenance.endpoints (source, active);
    CREATE INDEX endpoints_by_target
      ON provenance.endpoints (target_schema, target_table);
    -- Every /ingest/* and blob run is opened with endpoint_id
    -- 'ingress:generic'. Registering it lets the catalog's runs ⋈ endpoints
    -- lineage joins see those runs (the per-run target lives in runs.args).
    -- Same row as findata 20_ingress.sql.
    INSERT INTO provenance.endpoints
      (endpoint_id, source, path_template, http_method,
       scope, target_schema, target_table, active, notes)
    VALUES
      ('ingress:generic', 'ingress', '/ingest/{schema}/{table}', 'POST',
       'ingress', '__ingress__', '__ingress__', true,
       'Catch-all endpoint for HTTP-ingress writes. Per-run target lives in provenance.runs.args.');
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.runs') IS NULL THEN
    -- endpoint_id deliberately has no FK: a peer's runs may reference
    -- endpoints this instance never registered.
    CREATE TABLE provenance.runs (
      run_id               uuid        NOT NULL DEFAULT gen_random_uuid(),
      endpoint_id          text        NOT NULL,
      credential_label     text,
      started_at           timestamptz NOT NULL DEFAULT now(),
      ended_at             timestamptz,
      status               text        NOT NULL DEFAULT 'running',
      args                 jsonb,
      rows_inserted        bigint,
      rows_updated         bigint,
      rows_failed          bigint,
      response_schema_hash text,
      error_text           text,
      notes                text,
      submitted_by         text,
      PRIMARY KEY (run_id)
    );
    CREATE INDEX runs_by_endpoint
      ON provenance.runs (endpoint_id, started_at DESC);
    CREATE INDEX runs_by_status
      ON provenance.runs (status, started_at DESC);
    CREATE INDEX runs_by_submitter
      ON provenance.runs (submitted_by, started_at DESC)
      WHERE submitted_by IS NOT NULL;
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.table_backend') IS NULL THEN
    CREATE TABLE provenance.table_backend (
      target_schema text        NOT NULL,
      target_table  text        NOT NULL,
      backend       text        NOT NULL DEFAULT 'postgres',
      created_at    timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (target_schema, target_table)
    );
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.ingress_acl') IS NULL THEN
    CREATE TABLE provenance.ingress_acl (
      role          text        NOT NULL,
      target_schema text        NOT NULL,
      target_table  text        NOT NULL,
      can_write     boolean     NOT NULL DEFAULT true,
      notes         text,
      created_at    timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (role, target_schema, target_table)
    );
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.ingress_proposals') IS NULL THEN
    CREATE TABLE provenance.ingress_proposals (
      proposal_id       uuid        NOT NULL DEFAULT gen_random_uuid(),
      declared_schema   text        NOT NULL,
      declared_table    text        NOT NULL,
      proposer_sub      text        NOT NULL,
      proposer_role     text        NOT NULL,
      inferred_schema   jsonb       NOT NULL,
      inferred_key      text[]      NOT NULL DEFAULT '{}',
      sample_records    jsonb       NOT NULL DEFAULT '[]',
      drop_count        bigint      NOT NULL DEFAULT 0,
      suggested_backend text        NOT NULL DEFAULT 'postgres',
      status            text        NOT NULL DEFAULT 'pending',
      applied_table     text,
      reviewer_sub      text,
      reviewed_at       timestamptz,
      review_notes      text,
      rounds            jsonb       NOT NULL DEFAULT '[]',
      created_at        timestamptz NOT NULL DEFAULT now(),
      updated_at        timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (proposal_id)
    );
    CREATE INDEX ingress_proposals_status_created
      ON provenance.ingress_proposals (status, created_at DESC);
    CREATE INDEX ingress_proposals_proposer
      ON provenance.ingress_proposals (proposer_sub, created_at DESC);
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.webhooks') IS NULL THEN
    CREATE TABLE provenance.webhooks (
      webhook_id      uuid        NOT NULL DEFAULT gen_random_uuid(),
      owner_sub       text        NOT NULL,
      secret_plain    text        NOT NULL,
      target_schema   text,
      target_table    text,
      adapter_id      text,
      source_endpoint text,
      active          boolean     NOT NULL DEFAULT true,
      use_count       bigint      NOT NULL DEFAULT 0,
      last_used_at    timestamptz,
      created_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (webhook_id)
    );
    CREATE INDEX webhooks_by_owner
      ON provenance.webhooks (owner_sub, created_at DESC);
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.endpoint_freshness') IS NULL THEN
    -- A VIEW, as in findata 20_endpoint_freshness.sql (minus scrape_cursor,
    -- which the platform does not own): SLA derived from each active
    -- endpoint's newest successful run, so /freshness and /status track the
    -- registry instead of reading a table nothing writes.
    CREATE VIEW provenance.endpoint_freshness AS
    SELECT e.endpoint_id,
           e.target_table,
           e.cadence,
           r.last_ok                             AS last_data_date,
           (CURRENT_DATE - r.last_ok::date)      AS days_stale,
           r.last_ok                             AS last_run_at,
           CASE
             WHEN r.last_ok IS NULL THEN 'gray'
             WHEN e.cadence = 'daily'     AND (CURRENT_DATE - r.last_ok::date) > 3   THEN 'red'
             WHEN e.cadence = 'weekly'    AND (CURRENT_DATE - r.last_ok::date) > 14  THEN 'red'
             WHEN e.cadence = 'quarterly' AND (CURRENT_DATE - r.last_ok::date) > 120 THEN 'red'
             WHEN e.cadence = 'daily'     AND (CURRENT_DATE - r.last_ok::date) > 1   THEN 'amber'
             WHEN e.cadence = 'weekly'    AND (CURRENT_DATE - r.last_ok::date) > 7   THEN 'amber'
             WHEN e.cadence = 'quarterly' AND (CURRENT_DATE - r.last_ok::date) > 95  THEN 'amber'
             ELSE 'green'
           END                                   AS sla_status
      FROM provenance.endpoints e
      LEFT JOIN LATERAL (
          SELECT max(ru.started_at) AS last_ok
            FROM provenance.runs ru
           WHERE ru.endpoint_id = e.endpoint_id
             AND ru.status IN ('ok', 'completed', 'partial')
      ) r ON true
     WHERE e.active AND e.target_table IS DISTINCT FROM '__unmapped__';
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('raw.blobs') IS NULL THEN
    CREATE TABLE raw.blobs (
      blob_sha256     text        NOT NULL,
      storage_url     text        NOT NULL,
      content_type    text        NOT NULL,
      size_bytes      bigint      NOT NULL,
      suggested_name  text,
      metadata        jsonb       NOT NULL DEFAULT '{}',
      source          text        NOT NULL,
      source_endpoint text        NOT NULL,
      source_run_id   uuid        NOT NULL REFERENCES provenance.runs(run_id),
      submitted_by    text        NOT NULL,
      created_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (blob_sha256)
    );
    CREATE INDEX blobs_by_source_run
      ON raw.blobs (source_run_id);
  END IF;
END
$platform_tables$;";

/// Columns the runtime writes that older registries (findata's original
/// `01_schema.sql`, lqt-data before its idempotent ADDs) lack. Each ADD runs
/// only when the column is actually missing, so a current DB executes no
/// ALTER at all and needs no ownership.
pub const PLATFORM_COLUMNS_DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);

DO $platform_columns$
BEGIN
  IF to_regclass('provenance.runs') IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM pg_attribute
        WHERE attrelid = 'provenance.runs'::regclass
          AND attname = 'submitted_by' AND NOT attisdropped) THEN
    ALTER TABLE provenance.runs ADD COLUMN submitted_by text;
  END IF;
  IF to_regclass('provenance.ingress_proposals') IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM pg_attribute
        WHERE attrelid = 'provenance.ingress_proposals'::regclass
          AND attname = 'suggested_backend' AND NOT attisdropped) THEN
    ALTER TABLE provenance.ingress_proposals
      ADD COLUMN suggested_backend text NOT NULL DEFAULT 'postgres';
  END IF;
  IF to_regclass('provenance.ingress_proposals') IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM pg_attribute
        WHERE attrelid = 'provenance.ingress_proposals'::regclass
          AND attname = 'rounds' AND NOT attisdropped) THEN
    ALTER TABLE provenance.ingress_proposals
      ADD COLUMN rounds jsonb NOT NULL DEFAULT '[]'::jsonb;
  END IF;
END
$platform_columns$;";

/// Create the platform schemas and bookkeeping tables (idempotent).
pub async fn migrate(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;
    migrate_client(&client).await
}

/// [`migrate`] against a bare client (also used by the DB-backed test).
///
/// Each `batch_execute` is one simple-query message, which Postgres runs as a
/// single implicit transaction — the leading `pg_advisory_xact_lock` is held
/// until that batch commits or rolls back.
pub async fn migrate_client(client: &Client) -> anyhow::Result<()> {
    if let Err(e) = client.batch_execute(PLATFORM_TABLES_DDL).await {
        // A DB that already carries a provenance registry was serving before
        // this migration existed; a missing privilege for some optional
        // relation must not take it down on upgrade. Only a DB with no
        // registry at all (nothing could work) fails boot.
        let has_registry = client
            .query_one("SELECT to_regclass('provenance.runs') IS NOT NULL", &[])
            .await
            .map(|r| r.get::<_, bool>(0))
            .unwrap_or(false);
        if !has_registry {
            return Err(anyhow::anyhow!("platform bookkeeping tables: {e}"));
        }
        tracing::warn!(
            "platform bookkeeping: could not create missing relations in the existing \
             provenance registry ({e}); routes that need them will fail"
        );
    }
    if let Err(e) = client.batch_execute(PLATFORM_COLUMNS_DDL).await {
        tracing::warn!(
            "platform bookkeeping: could not add missing columns to an existing \
             provenance registry ({e}); ingest/proposal writes needing them will fail"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{MIGRATION_LOCK_KEY, PLATFORM_COLUMNS_DDL, PLATFORM_TABLES_DDL};

    fn blocks() -> Vec<&'static str> {
        PLATFORM_TABLES_DDL
            .split("DO $platform_tables$")
            .skip(1)
            .collect()
    }

    #[test]
    fn lock_key_matches_ddl() {
        let lock = format!("pg_advisory_xact_lock({MIGRATION_LOCK_KEY})");
        assert!(PLATFORM_TABLES_DDL.starts_with(&format!("SELECT {lock};")));
        assert!(PLATFORM_COLUMNS_DDL.starts_with(&format!("SELECT {lock};")));
    }

    #[test]
    fn ddl_is_guarded_and_covers_platform_relations() {
        let expected: BTreeSet<&str> = [
            "provenance.runs",
            "provenance.table_backend",
            "provenance.ingress_proposals",
            "provenance.ingress_acl",
            "provenance.endpoints",
            "provenance.api_sources",
            "provenance.webhooks",
            "provenance.endpoint_freshness",
            "raw.blobs",
        ]
        .into_iter()
        .collect();
        let blocks = blocks();
        // One preamble block (schemas/extension) + one per relation.
        assert_eq!(blocks.len(), expected.len() + 1);

        for relation in expected {
            let block = blocks
                .iter()
                .find(|block| block.contains(&format!("to_regclass('{relation}') IS NULL THEN\n")))
                .unwrap_or_else(|| panic!("missing guarded block for {relation}"));
            assert!(
                block.contains(&format!("CREATE TABLE {relation} ("))
                    || block.contains(&format!("CREATE VIEW {relation} AS")),
                "missing {relation}"
            );
        }

        // Nothing unguarded: no IF NOT EXISTS DDL that would touch an
        // existing DB (or need privileges it may not grant).
        assert!(!PLATFORM_TABLES_DDL.contains("CREATE SCHEMA IF NOT EXISTS"));
        assert!(!PLATFORM_TABLES_DDL.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(!PLATFORM_TABLES_DDL.contains("CREATE INDEX IF NOT EXISTS"));
        let outside_blocks = PLATFORM_TABLES_DDL
            .split("DO $platform_tables$")
            .next()
            .expect("prefix");
        assert!(!outside_blocks.contains("CREATE"));

        // endpoint_freshness must be derived, never a table nothing writes.
        assert!(!PLATFORM_TABLES_DDL.contains("CREATE TABLE provenance.endpoint_freshness"));
    }

    #[test]
    fn ddl_has_every_runtime_conflict_target() {
        for target in [
            "PRIMARY KEY (run_id)",
            "PRIMARY KEY (target_schema, target_table)",
            "PRIMARY KEY (role, target_schema, target_table)",
            "PRIMARY KEY (proposal_id)",
            "PRIMARY KEY (endpoint_id)",
            "PRIMARY KEY (source)",
            "PRIMARY KEY (webhook_id)",
            "PRIMARY KEY (blob_sha256)",
        ] {
            assert!(PLATFORM_TABLES_DDL.contains(target), "missing {target}");
        }
    }

    /// Executes the DDL for real. Runs only when `PLATFORM_TABLES_TEST_PG`
    /// points at a disposable database (it creates and drops schemas):
    ///   PLATFORM_TABLES_TEST_PG='host=localhost user=postgres password=x dbname=t' \
    ///     cargo test -p lumid-platform platform_tables
    #[tokio::test]
    async fn migrate_is_idempotent_and_runtime_sql_fits() {
        let Ok(dsn) = std::env::var("PLATFORM_TABLES_TEST_PG") else {
            eprintln!("PLATFORM_TABLES_TEST_PG unset; skipping DB-backed test");
            return;
        };
        let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(conn);
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS provenance CASCADE;",
            )
            .await
            .unwrap();

        super::migrate_client(&client).await.expect("first migrate");
        super::migrate_client(&client)
            .await
            .expect("second migrate (no-op)");

        // Runtime shapes: open_run / close_run, blob insert, a peer run
        // pushed through the sync inbox's jsonb_populate_record with null
        // counters and an endpoint with no source.
        client
            .batch_execute(
                "INSERT INTO provenance.runs (endpoint_id, credential_label, status, args)
                   VALUES ('ingress:generic', NULL, 'running', '{\"target_schema\":\"s\",\"target_table\":\"t\"}');
                 UPDATE provenance.runs SET ended_at = now(), status = 'ok',
                        rows_inserted = 2, rows_updated = 0, rows_failed = 0, submitted_by = 'u';
                 INSERT INTO raw.blobs (blob_sha256, storage_url, content_type, size_bytes,
                        source, source_endpoint, source_run_id, submitted_by)
                   SELECT 'abc', 'file:///x', 'text/plain', 1, 'ingress', 'ingress:generic', run_id, 'u'
                     FROM provenance.runs LIMIT 1;
                 INSERT INTO provenance.endpoints
                   SELECT (jsonb_populate_record(NULL::provenance.endpoints,
                          '{\"endpoint_id\":\"peer:x\",\"source\":null}')).*
                   ON CONFLICT (endpoint_id) DO NOTHING;
                 INSERT INTO provenance.runs
                   SELECT (jsonb_populate_record(NULL::provenance.runs,
                          '{\"run_id\":\"00000000-0000-0000-0000-000000000001\",
                            \"endpoint_id\":\"peer:x\",\"status\":\"running\",
                            \"started_at\":\"2026-01-01T00:00:00Z\",\"rows_inserted\":null}')).*
                   ON CONFLICT (run_id) DO NOTHING;",
            )
            .await
            .expect("runtime writes");

        // Catalog lineage join sees the ingress run; freshness view reports it.
        let lineage = client
            .query_one(
                "SELECT count(*) FROM provenance.runs r
                   JOIN provenance.endpoints e ON e.endpoint_id = r.endpoint_id
                  WHERE r.args->>'target_schema' = 's'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(lineage.get::<_, i64>(0), 1);
        let green = client
            .query_one(
                "SELECT count(*) FILTER (WHERE sla_status = 'green') FROM provenance.endpoint_freshness",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(green.get::<_, i64>(0), 1);

        client
            .batch_execute("DROP SCHEMA raw CASCADE; DROP SCHEMA provenance CASCADE;")
            .await
            .unwrap();
    }
}
