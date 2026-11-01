//! Platform-owned bookkeeping tables.
//!
//! The ingest, catalog, sync, and blob planes read these tables on a normal
//! request, so a fresh deployment must create them before serving traffic.

use deadpool_postgres::Pool;

/// Idempotent DDL for every platform-owned bookkeeping relation.
pub const PLATFORM_TABLES_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS provenance;
CREATE SCHEMA IF NOT EXISTS raw;

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
      created_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (source)
    );
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.endpoints') IS NULL THEN
    CREATE TABLE provenance.endpoints (
      endpoint_id     text        NOT NULL,
      source          text        NOT NULL REFERENCES provenance.api_sources(source),
      path_template   text,
      http_method     text        NOT NULL DEFAULT 'GET',
      pagination      text,
      scope           text,
      symbol_universe text,
      target_schema   text,
      target_table    text,
      field_map       jsonb,
      response_shape  jsonb,
      shape_hash      text,
      cadence         text,
      active          boolean     NOT NULL DEFAULT true,
      notes           text,
      created_at      timestamptz NOT NULL DEFAULT now(),
      updated_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (endpoint_id)
    );
    CREATE INDEX endpoints_by_source
      ON provenance.endpoints (source, active);
    CREATE INDEX endpoints_by_target
      ON provenance.endpoints (target_schema, target_table);
  END IF;
END
$platform_tables$;

DO $platform_tables$
BEGIN
  IF to_regclass('provenance.runs') IS NULL THEN
    CREATE TABLE provenance.runs (
      run_id               uuid        NOT NULL DEFAULT gen_random_uuid(),
      endpoint_id          text        NOT NULL,
      credential_label     text,
      started_at           timestamptz NOT NULL DEFAULT now(),
      ended_at             timestamptz,
      status               text        NOT NULL DEFAULT 'running',
      args                 jsonb,
      rows_inserted        bigint      NOT NULL DEFAULT 0,
      rows_updated         bigint      NOT NULL DEFAULT 0,
      rows_failed          bigint      NOT NULL DEFAULT 0,
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
    CREATE TABLE provenance.endpoint_freshness (
      endpoint_id    text        NOT NULL,
      target_table   text,
      last_data_date timestamptz,
      days_stale     integer,
      last_run_at    timestamptz,
      sla_status     text        NOT NULL DEFAULT 'gray',
      PRIMARY KEY (endpoint_id)
    );
    CREATE INDEX endpoint_freshness_by_sla
      ON provenance.endpoint_freshness (sla_status);
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

/// Create the platform schemas and bookkeeping tables (idempotent).
pub async fn migrate(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client.batch_execute(PLATFORM_TABLES_DDL).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::PLATFORM_TABLES_DDL;

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
        let blocks: Vec<&str> = PLATFORM_TABLES_DDL
            .split("DO $platform_tables$")
            .skip(1)
            .collect();
        assert_eq!(blocks.len(), expected.len());

        for relation in expected {
            let block = blocks
                .iter()
                .find(|block| block.contains(&format!("to_regclass('{relation}') IS NULL")))
                .unwrap_or_else(|| panic!("missing guarded block for {relation}"));
            assert!(
                block.contains(&format!("CREATE TABLE {relation} (")),
                "missing {relation}"
            );
        }

        let outside_blocks = PLATFORM_TABLES_DDL
            .split("DO $platform_tables$")
            .next()
            .expect("prefix");
        let suffix = PLATFORM_TABLES_DDL
            .split("$platform_tables$;")
            .last()
            .expect("suffix");
        assert!(!outside_blocks.contains("CREATE TABLE"));
        assert!(!outside_blocks.contains("CREATE INDEX"));
        assert!(!suffix.contains("CREATE TABLE"));
        assert!(!suffix.contains("CREATE INDEX"));

        for index in [
            "endpoints_by_source",
            "endpoints_by_target",
            "runs_by_endpoint",
            "runs_by_status",
            "runs_by_submitter",
            "ingress_proposals_status_created",
            "ingress_proposals_proposer",
            "webhooks_by_owner",
            "endpoint_freshness_by_sla",
            "blobs_by_source_run",
        ] {
            let defining_blocks: Vec<&str> = blocks
                .iter()
                .filter(|block| block.contains(&format!("CREATE INDEX {index}")))
                .copied()
                .collect();
            assert_eq!(defining_blocks.len(), 1, "unexpected count for {index}");
        }
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
}
