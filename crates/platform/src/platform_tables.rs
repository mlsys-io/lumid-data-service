//! Platform-owned bookkeeping tables.
//!
//! The ingest, catalog, sync, and blob planes read these tables on a normal
//! request, so a fresh deployment must create them before serving traffic.

use deadpool_postgres::Pool;

/// Idempotent DDL for every platform-owned bookkeeping relation.
pub const PLATFORM_TABLES_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS provenance;
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS provenance.api_sources (
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

CREATE TABLE IF NOT EXISTS provenance.endpoints (
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
CREATE INDEX IF NOT EXISTS endpoints_by_source
  ON provenance.endpoints (source, active);
CREATE INDEX IF NOT EXISTS endpoints_by_target
  ON provenance.endpoints (target_schema, target_table);

CREATE TABLE IF NOT EXISTS provenance.runs (
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
CREATE INDEX IF NOT EXISTS runs_by_endpoint
  ON provenance.runs (endpoint_id, started_at DESC);
CREATE INDEX IF NOT EXISTS runs_by_status
  ON provenance.runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS runs_by_submitter
  ON provenance.runs (submitted_by, started_at DESC)
  WHERE submitted_by IS NOT NULL;

CREATE TABLE IF NOT EXISTS provenance.table_backend (
  target_schema text        NOT NULL,
  target_table  text        NOT NULL,
  backend       text        NOT NULL DEFAULT 'postgres',
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target_schema, target_table)
);

CREATE TABLE IF NOT EXISTS provenance.ingress_acl (
  role          text        NOT NULL,
  target_schema text        NOT NULL,
  target_table  text        NOT NULL,
  can_write     boolean     NOT NULL DEFAULT true,
  notes         text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (role, target_schema, target_table)
);

CREATE TABLE IF NOT EXISTS provenance.ingress_proposals (
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
CREATE INDEX IF NOT EXISTS ingress_proposals_status_created
  ON provenance.ingress_proposals (status, created_at DESC);
CREATE INDEX IF NOT EXISTS ingress_proposals_proposer
  ON provenance.ingress_proposals (proposer_sub, created_at DESC);

CREATE TABLE IF NOT EXISTS provenance.webhooks (
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
CREATE INDEX IF NOT EXISTS webhooks_by_owner
  ON provenance.webhooks (owner_sub, created_at DESC);

CREATE TABLE IF NOT EXISTS provenance.endpoint_freshness (
  endpoint_id    text        NOT NULL,
  target_table   text,
  last_data_date timestamptz,
  days_stale     integer,
  last_run_at    timestamptz,
  sla_status     text        NOT NULL DEFAULT 'gray',
  PRIMARY KEY (endpoint_id)
);
CREATE INDEX IF NOT EXISTS endpoint_freshness_by_sla
  ON provenance.endpoint_freshness (sla_status);

CREATE TABLE IF NOT EXISTS raw.blobs (
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
CREATE INDEX IF NOT EXISTS blobs_by_source_run
  ON raw.blobs (source_run_id);";

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
    fn ddl_is_idempotent_and_covers_platform_relations() {
        for statement_start in ["CREATE SCHEMA", "CREATE TABLE", "CREATE INDEX"] {
            let lower = PLATFORM_TABLES_DDL.to_ascii_lowercase();
            let needle = format!("{} if not exists", statement_start.to_ascii_lowercase());
            let count = lower.matches(&needle).count();
            let total = PLATFORM_TABLES_DDL
                .lines()
                .filter(|line| line.starts_with(statement_start))
                .count();
            assert_eq!(count, total, "{statement_start} must be idempotent");
        }

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
        for relation in expected {
            assert!(
                PLATFORM_TABLES_DDL.contains(&format!("TABLE IF NOT EXISTS {relation} (")),
                "missing {relation}"
            );
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
