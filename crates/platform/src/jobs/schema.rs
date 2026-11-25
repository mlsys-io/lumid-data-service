//! The `jobs` schema: the multi-user job registry, user-owned shareable
//! sources, where data lives, and how to reach it from each site.
//!
//! Ownership follows location. Every row a hub can sync to another carries
//! `home_hub` (the ONLY hub that writes it) and `updated_at` (the version a
//! peer compares). Deletes are tombstones (`deleted_at`) so they can sync too.
//! `job_tokens` is deliberately local-only: a credential never leaves the hub
//! that issued it.
//!
//! Same bootstrap rules as `platform_tables` (one simple-query batch = one
//! implicit transaction, serialized on the platform bootstrap advisory lock;
//! relations created only when absent, so an existing DB runs no DDL).
//!
//! The retrieval reader role is stripped from the whole schema on every boot:
//! nothing here may be readable through caller-shaped `/retrieve` SQL, and
//! `ALTER DEFAULT PRIVILEGES … GRANT SELECT ON TABLES` (deploy/standalone/
//! reader_role.sql) would otherwise grant every new table to it.

use deadpool_postgres::Pool;

/// Idempotent DDL for the `jobs` schema.
pub const JOBS_DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);

DO $jobs$
BEGIN
  IF to_regnamespace('jobs') IS NULL THEN
    CREATE SCHEMA jobs;
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.hubs') IS NULL THEN
    CREATE TABLE jobs.hubs (
      hub_id          text        PRIMARY KEY,
      base_url        text        NOT NULL,
      site            text        NOT NULL,
      reachable_from  jsonb       NOT NULL DEFAULT '[]',
      home_hub        text        NOT NULL,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.sources') IS NULL THEN
    CREATE TABLE jobs.sources (
      source_id       text        PRIMARY KEY,
      home_hub        text        NOT NULL,
      owner_sub       text        NOT NULL,
      org_id          text,
      name            text        NOT NULL,
      kind            text        NOT NULL CHECK (kind IN
                        ('estate','table','view','query','blob_set','model','external')),
      definition      jsonb       NOT NULL DEFAULT '{}',
      site            text,
      visibility      text        NOT NULL DEFAULT 'private'
                        CHECK (visibility IN ('private','shared','public')),
      size_bytes_est  bigint,
      created_at      timestamptz NOT NULL DEFAULT now(),
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz,
      UNIQUE (home_hub, owner_sub, name)
    );
    CREATE INDEX sources_by_owner ON jobs.sources (owner_sub) WHERE deleted_at IS NULL;
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.source_grants') IS NULL THEN
    CREATE TABLE jobs.source_grants (
      source_id       text        NOT NULL REFERENCES jobs.sources(source_id),
      grantee_sub     text        NOT NULL,
      grantee_org     text,
      access          text        NOT NULL CHECK (access IN ('read','write')),
      granted_by      text        NOT NULL,
      home_hub        text        NOT NULL,
      created_at      timestamptz NOT NULL DEFAULT now(),
      expires_at      timestamptz,
      revoked_at      timestamptz,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (source_id, grantee_sub, access)
    );
    CREATE INDEX source_grants_by_grantee ON jobs.source_grants (grantee_sub)
      WHERE revoked_at IS NULL;
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.source_locations') IS NULL THEN
    CREATE TABLE jobs.source_locations (
      source_id       text        NOT NULL REFERENCES jobs.sources(source_id),
      site            text        NOT NULL,
      role            text        NOT NULL DEFAULT 'primary' CHECK (role IN ('primary','replica')),
      endpoint        text,
      size_bytes_est  bigint,
      home_hub        text        NOT NULL,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz,
      PRIMARY KEY (source_id, site)
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.site_routes') IS NULL THEN
    CREATE TABLE jobs.site_routes (
      route_id        text        PRIMARY KEY,
      from_site       text        NOT NULL,
      source_id       text        NOT NULL,
      kind            text        NOT NULL CHECK (kind IN ('lan','tailnet','proxy','egress','public')),
      url             text        NOT NULL,
      cred_mode       text        NOT NULL DEFAULT 'job_token'
                        CHECK (cred_mode IN ('job_token','bridge_jwt','service_pat')),
      max_bytes       bigint,
      pref            integer     NOT NULL DEFAULT 100,
      health          text        NOT NULL DEFAULT 'unknown'
                        CHECK (health IN ('unknown','up','degraded','down')),
      latency_ms      integer,
      health_at       timestamptz,
      home_hub        text        NOT NULL,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz
    );
    CREATE INDEX site_routes_by_site ON jobs.site_routes (from_site, source_id)
      WHERE deleted_at IS NULL;
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.jobs') IS NULL THEN
    CREATE TABLE jobs.jobs (
      job_id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
      home_hub        text        NOT NULL,
      owner_sub       text        NOT NULL,
      org_id          text,
      app             text        NOT NULL CHECK (app <> ''),
      runtime         text        NOT NULL CHECK (runtime IN ('flowmesh','lumilake','sandbox')),
      runtime_ref     text,
      site            text,
      parent_job_id   uuid,
      status          text        NOT NULL DEFAULT 'planned' CHECK (status IN
                        ('planned','submitted','running','succeeded','failed','cancelled','refused')),
      spec            jsonb       NOT NULL,
      plan            jsonb,
      plan_id         uuid,
      idempotency_key text,
      est_bytes       bigint,
      error           jsonb,
      created_at      timestamptz NOT NULL DEFAULT now(),
      started_at      timestamptz,
      ended_at        timestamptz,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz,
      UNIQUE (owner_sub, idempotency_key)
    );
    CREATE INDEX jobs_by_owner ON jobs.jobs (owner_sub, created_at DESC);
    CREATE INDEX jobs_active ON jobs.jobs (status)
      WHERE status IN ('planned','submitted','running');
    CREATE INDEX jobs_by_parent ON jobs.jobs (parent_job_id) WHERE parent_job_id IS NOT NULL;
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.job_sources') IS NULL THEN
    CREATE TABLE jobs.job_sources (
      job_id          uuid        NOT NULL REFERENCES jobs.jobs(job_id),
      ref             text        NOT NULL,
      source_id       text,
      access          text        NOT NULL CHECK (access IN ('read','write')),
      site            text,
      route_id        text,
      route_kind      text,
      url             text,
      est_bytes       bigint,
      actual_bytes    bigint      NOT NULL DEFAULT 0,
      home_hub        text        NOT NULL,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (job_id, ref, access)
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.job_tokens') IS NULL THEN
    CREATE TABLE jobs.job_tokens (
      token_sha256    bytea       PRIMARY KEY,
      job_id          uuid        NOT NULL REFERENCES jobs.jobs(job_id),
      created_at      timestamptz NOT NULL DEFAULT now(),
      expires_at      timestamptz NOT NULL,
      revoked_at      timestamptz
    );
    CREATE INDEX job_tokens_by_job ON jobs.job_tokens (job_id);
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.job_events') IS NULL THEN
    CREATE TABLE jobs.job_events (
      id              bigserial   PRIMARY KEY,
      job_id          uuid        NOT NULL REFERENCES jobs.jobs(job_id),
      at              timestamptz NOT NULL DEFAULT now(),
      kind            text        NOT NULL,
      status          text,
      payload         jsonb       NOT NULL DEFAULT '{}',
      home_hub        text        NOT NULL
    );
    CREATE INDEX job_events_by_job ON jobs.job_events (job_id, id);
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.plans') IS NULL THEN
    CREATE TABLE jobs.plans (
      plan_id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
      owner_sub       text        NOT NULL,
      spec            jsonb       NOT NULL,
      plan            jsonb       NOT NULL,
      created_at      timestamptz NOT NULL DEFAULT now(),
      expires_at      timestamptz NOT NULL
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.quotas') IS NULL THEN
    CREATE TABLE jobs.quotas (
      owner_sub         text        PRIMARY KEY,
      max_concurrent    integer,
      max_planned_bytes bigint,
      updated_at        timestamptz NOT NULL DEFAULT now()
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  IF to_regclass('jobs.catalog_mirror') IS NULL THEN
    CREATE TABLE jobs.catalog_mirror (
      hub_id          text        NOT NULL,
      schema_name     text        NOT NULL,
      table_name      text        NOT NULL,
      kind            text,
      row_est         bigint,
      size_bytes_est  bigint,
      freshness_at    timestamptz,
      updated_at      timestamptz NOT NULL DEFAULT now(),
      deleted_at      timestamptz,
      PRIMARY KEY (hub_id, schema_name, table_name)
    );
  END IF;
END
$jobs$;

DO $jobs$
BEGIN
  -- to_regclass, not ::regclass: the cast raises when the table is missing,
  -- and SQL does not promise to short-circuit the guard before it.
  IF to_regclass('provenance.runs') IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM pg_attribute
        WHERE attrelid = to_regclass('provenance.runs')
          AND attname = 'job_id' AND NOT attisdropped) THEN
    ALTER TABLE provenance.runs ADD COLUMN job_id uuid;
  END IF;
END
$jobs$;";

/// Every relation [`JOBS_DDL`] creates, in creation order.
pub const JOBS_RELATIONS: &[&str] = &[
    "jobs.hubs",
    "jobs.sources",
    "jobs.source_grants",
    "jobs.source_locations",
    "jobs.site_routes",
    "jobs.jobs",
    "jobs.job_sources",
    "jobs.job_tokens",
    "jobs.job_events",
    "jobs.plans",
    "jobs.quotas",
    "jobs.catalog_mirror",
];

/// `REVOKE` the whole `jobs` schema from `reader_role` (no-op when the role is
/// unset or does not exist). Revoking schema USAGE alone would already block
/// every table; the table-level revoke keeps `has_table_privilege` honest too.
pub fn revoke_from_reader_sql(reader_role: &str) -> Option<String> {
    let role = reader_role.trim();
    if role.is_empty() {
        return None;
    }
    let ident = format!("\"{}\"", role.replace('"', "\"\""));
    let literal = role.replace('\'', "''");
    Some(format!(
        "DO $revoke$ BEGIN \
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{literal}') \
              AND to_regnamespace('jobs') IS NOT NULL THEN \
             REVOKE ALL ON ALL TABLES IN SCHEMA jobs FROM {ident}; \
             REVOKE ALL ON ALL SEQUENCES IN SCHEMA jobs FROM {ident}; \
             REVOKE ALL ON SCHEMA jobs FROM {ident}; \
           END IF; \
         END $revoke$;"
    ))
}

/// The full boot batch: DDL, then the reader revoke, as ONE simple-query
/// message so the leading advisory lock covers both. Concurrent GRANT/REVOKE
/// on the same objects otherwise fails with "tuple concurrently updated" (or
/// deadlocks) — measured: 10/10 runs of 8 concurrent boots failed when the
/// revoke ran as its own batch.
pub fn boot_sql(reader_role: &str) -> String {
    match revoke_from_reader_sql(reader_role) {
        Some(revoke) => format!("{JOBS_DDL}\n\n{revoke}"),
        None => JOBS_DDL.to_string(),
    }
}

/// Create the `jobs` schema (idempotent) and keep it away from the reader role.
pub async fn migrate(pool: &Pool, reader_role: &str) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client
        .batch_execute(&boot_sql(reader_role))
        .await
        .map_err(|e| anyhow::anyhow!("jobs schema: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocks() -> Vec<&'static str> {
        JOBS_DDL.split("DO $jobs$").skip(1).collect()
    }

    #[test]
    fn ddl_takes_the_platform_bootstrap_lock() {
        assert!(JOBS_DDL.starts_with("SELECT pg_advisory_xact_lock(7815273027795448930);"));
    }

    #[test]
    fn every_relation_has_one_guarded_create() {
        let blocks = blocks();
        // schema block + one per relation + the provenance.runs column block
        assert_eq!(blocks.len(), JOBS_RELATIONS.len() + 2);
        for rel in JOBS_RELATIONS {
            let n = blocks
                .iter()
                .filter(|b| b.contains(&format!("to_regclass('{rel}') IS NULL THEN\n")))
                .count();
            assert_eq!(n, 1, "{rel}");
            assert!(
                blocks
                    .iter()
                    .any(|b| b.contains(&format!("CREATE TABLE {rel} ("))),
                "{rel}"
            );
        }
        assert!(!JOBS_DDL.contains("IF NOT EXISTS"));
    }

    #[test]
    fn synced_relations_carry_home_hub_and_version() {
        // Everything a peer may receive names its writer and its version.
        for rel in [
            "jobs.hubs",
            "jobs.sources",
            "jobs.source_grants",
            "jobs.source_locations",
            "jobs.site_routes",
            "jobs.jobs",
            "jobs.job_sources",
        ] {
            let b = blocks()
                .into_iter()
                .find(|b| b.contains(&format!("CREATE TABLE {rel} (")))
                .unwrap();
            assert!(b.contains("home_hub"), "{rel} lacks home_hub");
            assert!(b.contains("updated_at"), "{rel} lacks updated_at");
        }
        // The credential table is local-only: no home_hub, never synced.
        let tokens = blocks()
            .into_iter()
            .find(|b| b.contains("CREATE TABLE jobs.job_tokens ("))
            .unwrap();
        assert!(!tokens.contains("home_hub"));
    }

    #[test]
    fn revoke_runs_inside_the_locked_batch() {
        let sql = boot_sql("lumid_reader");
        assert!(sql.starts_with("SELECT pg_advisory_xact_lock("));
        assert!(sql.contains("REVOKE ALL ON SCHEMA jobs"));
        assert_eq!(sql.matches("pg_advisory_xact_lock").count(), 1);
        assert_eq!(boot_sql(""), JOBS_DDL);
    }

    #[test]
    fn reader_revoke_is_guarded_and_quoted() {
        assert_eq!(revoke_from_reader_sql(" "), None);
        let sql = revoke_from_reader_sql("lumid_reader").unwrap();
        assert!(sql.contains("REVOKE ALL ON SCHEMA jobs FROM \"lumid_reader\""));
        assert!(sql.contains("rolname = 'lumid_reader'"));
        let sql = revoke_from_reader_sql("a\"b'c").unwrap();
        assert!(sql.contains("FROM \"a\"\"b'c\""));
        assert!(sql.contains("rolname = 'a\"b''c'"));
    }
}
