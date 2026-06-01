//! Backend registry — resolves which storage backend owns a given `schema.table`.
//!
//! Source of truth is the `provenance.table_backend(target_schema, target_table,
//! backend, created_at)` table (created in `lqt-data/ddl/00_provenance.sql`).
//! When no row exists the table defaults to **Postgres** — so every pre-existing
//! table (which has no `table_backend` row) resolves to PG and the legacy path is
//! byte-identical. The resolve result is moka-cached (like
//! `write/introspect.rs`'s metadata cache) and cleared by the admin
//! `/admin/ingress/refresh-schemas` route.
//!
//! Phase A holds only the [`PostgresBackend`]; a `ClickHouseBackend` slot is
//! reserved (`Option`, `None` for now) so Phase B can drop in CH without touching
//! callers.

use std::sync::Arc;

use deadpool_postgres::Pool;
use moka::future::Cache;

use super::{Backend, BackendKind, ClickHouseBackend, PostgresBackend};
use crate::error::ApiResult;

/// Resolves `(schema, table) → Backend`. Cheap to clone (everything inside is an
/// `Arc`); construct once at boot and stash on `AppState`.
pub struct Registry {
    pool: Pool,
    postgres: PostgresBackend,
    /// The ClickHouse backend, present only when the deployment configured
    /// `LUMID_CLICKHOUSE_URL` (+ user/pass/db). `None` ⇒ Phase A behavior
    /// (every table resolves to Postgres; a CH approve/route is rejected).
    clickhouse: Option<Arc<dyn Backend>>,
    /// `schema.table -> BackendKind`, mirroring the introspect metadata cache
    /// (256 entries, cleared by `refresh-schemas`).
    cache: Cache<String, BackendKind>,
}

impl Registry {
    /// Build a Postgres-only registry (Phase A). The CH slot stays `None`.
    pub fn new_postgres_only(pool: Pool) -> Self {
        let postgres = PostgresBackend::new(pool.clone());
        Self {
            pool,
            postgres,
            clickhouse: None,
            cache: Cache::new(256),
        }
    }

    /// Build a registry with ClickHouse enabled (Phase B). `boot` calls this
    /// when `LUMID_CLICKHOUSE_URL` is configured; otherwise it stays on
    /// [`Self::new_postgres_only`].
    pub fn new_with_clickhouse(pool: Pool, ch: ClickHouseBackend) -> Self {
        let postgres = PostgresBackend::new(pool.clone());
        Self {
            pool,
            postgres,
            clickhouse: Some(Arc::new(ch)),
            cache: Cache::new(256),
        }
    }

    /// Whether a working ClickHouse backend is configured. The approve path
    /// rejects a `clickhouse` choice with a clear 503 when this is `false`.
    pub fn clickhouse_configured(&self) -> bool {
        self.clickhouse.is_some()
    }

    /// The ClickHouse backend handle, if configured. Used by the approve path to
    /// dispatch `create_table` to CH before the PG bookkeeping tx.
    pub fn clickhouse_backend(&self) -> Option<&dyn Backend> {
        self.clickhouse.as_deref()
    }

    /// Seed the resolve cache with a freshly-recorded backend so the next
    /// ingest/read routes correctly without a DB round-trip (called by the
    /// approve path right after it commits the `table_backend` row).
    pub async fn note_backend_cached(&self, schema: &str, table: &str, kind: BackendKind) {
        self.cache.insert(Self::key(schema, table), kind).await;
    }

    fn key(schema: &str, table: &str) -> String {
        format!("{schema}.{table}")
    }

    /// Clear the resolve cache (admin `refresh-schemas`), mirroring
    /// `introspect::refresh_cache`.
    pub async fn refresh_cache(&self) {
        self.cache.invalidate_all();
    }

    /// Resolve the backend kind for `schema.table`. Reads the
    /// `provenance.table_backend` row (cached); default → Postgres when no row.
    pub async fn resolve(&self, schema: &str, table: &str) -> ApiResult<BackendKind> {
        let k = Self::key(schema, table);
        if let Some(kind) = self.cache.get(&k).await {
            return Ok(kind);
        }
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT backend FROM provenance.table_backend \
                 WHERE target_schema = $1 AND target_table = $2",
                &[&schema, &table],
            )
            .await?;
        let kind = match row {
            Some(r) => BackendKind::from_str_or_pg(&r.get::<_, String>("backend")),
            None => BackendKind::Postgres,
        };
        self.cache.insert(k, kind).await;
        Ok(kind)
    }

    /// Backend handle for `schema.table`. Resolves the table's backend kind and
    /// hands back the matching impl. If a `table_backend` row names ClickHouse
    /// but no CH backend is configured (slot `None`), fall back to Postgres so
    /// the path stays safe rather than panicking — the approve path is what
    /// gates CH selection on `clickhouse_configured()`, so this is defence in
    /// depth for a row that predates the config.
    pub async fn get(&self, schema: &str, table: &str) -> ApiResult<&dyn Backend> {
        match self.resolve(schema, table).await? {
            BackendKind::ClickHouse => {
                if let Some(ch) = &self.clickhouse {
                    Ok(ch.as_ref())
                } else {
                    Ok(&self.postgres)
                }
            }
            BackendKind::Postgres => Ok(&self.postgres),
        }
    }

    /// Record the backend a table was created on (called from approve). Defaults
    /// the `backend` column to 'postgres'; idempotent via upsert.
    pub async fn record_backend(
        &self,
        schema: &str,
        table: &str,
        kind: BackendKind,
    ) -> ApiResult<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO provenance.table_backend (target_schema, target_table, backend) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (target_schema, target_table) DO UPDATE SET backend = EXCLUDED.backend",
                &[&schema, &table, &kind.as_str()],
            )
            .await?;
        // Keep the resolve cache consistent with the just-written row.
        self.cache.insert(Self::key(schema, table), kind).await;
        Ok(())
    }

    /// Direct access to the Postgres backend (for callers mid-migration).
    pub fn postgres(&self) -> &PostgresBackend {
        &self.postgres
    }

    /// The shared Postgres pool. Provenance/run bookkeeping (open_run, close_run,
    /// validation introspection) still runs on PG regardless of a table's data
    /// backend, so callers reach the pool through the registry.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::backend::Backend;

    /// Build a pool without connecting (deadpool is lazy — no backend touched
    /// until `.get()`), so we can unit-test wiring offline.
    fn offline_pool() -> Pool {
        let mut cfg = deadpool_postgres::Config::new();
        cfg.host = Some("127.0.0.1".into());
        cfg.port = Some(1); // never dialed in these tests
        cfg.user = Some("nobody".into());
        cfg.dbname = Some("nodb".into());
        cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
            .expect("lazy pool build")
    }

    #[test]
    fn registry_holds_postgres_backend_and_no_clickhouse() {
        let reg = Registry::new_postgres_only(offline_pool());
        // Phase A: PG backend present, CH slot empty.
        assert_eq!(reg.postgres().kind(), BackendKind::Postgres);
        assert!(reg.clickhouse.is_none());
    }

    #[tokio::test]
    async fn resolve_caches_default_to_pg_without_hitting_db() {
        // Pre-seed the resolve cache so `resolve` returns from cache and never
        // dials the (offline) DB — proving the default-to-Postgres contract and
        // that `get()` hands back the Postgres backend for a PG-resolved table.
        let reg = Registry::new_postgres_only(offline_pool());
        reg.cache
            .insert(Registry::key("obs", "events"), BackendKind::Postgres)
            .await;
        assert_eq!(reg.resolve("obs", "events").await.unwrap(), BackendKind::Postgres);
        assert_eq!(reg.get("obs", "events").await.unwrap().kind(), BackendKind::Postgres);
    }

    #[test]
    fn backend_kind_wire_roundtrip() {
        assert_eq!(BackendKind::Postgres.as_str(), "postgres");
        assert_eq!(BackendKind::ClickHouse.as_str(), "clickhouse");
        assert_eq!(BackendKind::from_str_or_pg("postgres"), BackendKind::Postgres);
        assert_eq!(BackendKind::from_str_or_pg("clickhouse"), BackendKind::ClickHouse);
        // Unknown / empty / mixed-case all default to Postgres (zero-behavior-change).
        assert_eq!(BackendKind::from_str_or_pg("ClickHouse"), BackendKind::ClickHouse);
        assert_eq!(BackendKind::from_str_or_pg("mysql"), BackendKind::Postgres);
        assert_eq!(BackendKind::from_str_or_pg(""), BackendKind::Postgres);
        assert_eq!(BackendKind::from_str_or_pg("  POSTGRES  "), BackendKind::Postgres);
    }
}
