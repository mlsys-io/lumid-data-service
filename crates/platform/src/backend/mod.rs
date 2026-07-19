//! Storage-backend abstraction (multi-backend, Phase A — foundation only).
//!
//! The platform was Postgres-only: ingest wrote via `write::engine::copy_and_merge`
//! and reads ran straight off `st.pool`. To let the operator approve **both** a
//! schema AND a storage backend per table (PG, ClickHouse, …) without touching
//! writers, this module introduces a backend-agnostic seam:
//!
//!   * [`Backend`] — the trait every storage backend implements (table metadata,
//!     DDL, upsert/write, row query).
//!   * [`postgres::PostgresBackend`] — the only concrete backend today; it WRAPS
//!     the existing Postgres code paths (introspect / proposals DDL / write engine
//!     / read exec) so the PG path stays byte-identical.
//!   * [`registry::Registry`] — resolves `(schema, table) → BackendKind` from the
//!     `provenance.table_backend` registry table (default Postgres when no row),
//!     and hands back the matching `&dyn Backend`.
//!
//! **Phase A is zero-behavior-change:** every table resolves to Postgres (no
//! `table_backend` row ⇒ default PG), so existing tables are unaffected and no
//! data migration is needed. ClickHouse write/read land in Phase B / C.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::error::ApiResult;
use crate::write::introspect::TableMeta;

pub mod clickhouse;
pub mod postgres;
pub mod registry;

pub use clickhouse::ClickHouseBackend;
pub use postgres::PostgresBackend;
pub use registry::Registry;

/// Which storage engine a table lives on. As of Phase B the `ClickHouse`
/// variant is a working backend ([`ClickHouseBackend`]) when the deployment has
/// `LUMID_CLICKHOUSE_URL` configured; otherwise the registry leaves the slot
/// `None` and falls back to Postgres.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Postgres,
    ClickHouse,
}

impl BackendKind {
    /// Wire form stored in `provenance.table_backend.backend`.
    pub fn as_str(self) -> &'static str {
        match self {
            BackendKind::Postgres => "postgres",
            BackendKind::ClickHouse => "clickhouse",
        }
    }

    /// Parse the wire form; unknown strings default to Postgres (the safe,
    /// zero-behavior-change fallback).
    pub fn from_str_or_pg(s: &str) -> BackendKind {
        match s.trim().to_ascii_lowercase().as_str() {
            "clickhouse" => BackendKind::ClickHouse,
            _ => BackendKind::Postgres,
        }
    }
}

/// DDL plan for creating a target table. Backend-agnostic so the same approve
/// path can route to PG today and CH later. The columns/key are exactly what the
/// proposals flow inferred; the Postgres backend renders them to PG DDL.
pub struct CreateTablePlan<'a> {
    pub schema: &'a str,
    pub table: &'a str,
    /// Inferred `column_name -> postgres_type` map (the proposal's `inferred_schema`).
    pub inferred: &'a Map<String, Value>,
    /// Normalised natural-key columns present in `inferred` (may be empty → a
    /// generated-identity surrogate PK is used).
    pub key: &'a [String],
}

/// One write (upsert) request: the introspected target metadata, the parsed
/// records, and the provenance triplet to stamp. Backend-agnostic.
pub struct WriteRequest<'a> {
    pub schema: &'a str,
    pub table: &'a str,
    pub meta: &'a TableMeta,
    pub records: &'a [Value],
    pub source: &'a str,
    pub source_endpoint: &'a str,
    pub source_run_id: &'a uuid::Uuid,
}

/// One read query: already-bound SQL + its positional parameters. The SQL
/// carries PG-dialect `$N` placeholders (with `::int8`/`::float8` casts on
/// numeric binds). The Postgres backend runs it on `st.pool` via `params`; the
/// ClickHouse backend translates `$N`(+cast) → `?` and binds from `binds` (the
/// backend-neutral values — it can't recover a value from a `dyn ToSql`).
/// `params` and `binds` are the same values in the same order.
pub struct BoundQuery<'a> {
    pub sql: &'a str,
    pub params: Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
    pub binds: &'a [crate::read::bind::BindValue],
    /// When `true`, `sql` is already lowered to the target backend's dialect
    /// (`T-READ-IR-001` CH path — `?` placeholders, CH casts) and must be run
    /// verbatim. When `false`, `sql` carries PG `$N` placeholders and a non-PG
    /// backend translates them itself (the PR #9 placeholder-only path). Postgres
    /// ignores this flag (it always runs `sql`/`params` directly).
    pub pre_lowered: bool,
}

/// A storage backend. Writers/readers depend only on this trait; the registry
/// picks the concrete impl per table.
#[async_trait]
pub trait Backend: Send + Sync {
    fn kind(&self) -> BackendKind;

    /// Introspect (or fetch cached) the metadata for `schema.table`. `None` ⇒
    /// unknown table.
    async fn table_meta(&self, schema: &str, table: &str) -> ApiResult<Option<Arc<TableMeta>>>;

    /// Create the target table per the plan (idempotent — `IF NOT EXISTS`).
    async fn create_table(&self, plan: &CreateTablePlan<'_>) -> ApiResult<()>;

    /// Upsert the parsed records. Returns `(inserted, updated)`.
    async fn write_records(&self, req: &WriteRequest<'_>) -> ApiResult<(i64, i64)>;

    /// Execute a bound read query and return the row objects (lineage/shape are
    /// applied by the caller).
    async fn query_rows(&self, q: &BoundQuery<'_>) -> ApiResult<Vec<Map<String, Value>>>;

    /// Execute a bound read query with the session role de-escalated / re-scoped
    /// to `role` for THIS query only (Phase D4 admin cross-tenant oversight). The
    /// Postgres backend runs it inside a `READ ONLY` transaction with
    /// `SET LOCAL ROLE <role>` (reverts at txn end, no pool leakage) so an admin
    /// caller reads under a cross-tenant-visible role WITHOUT a `bypassrls` grant.
    ///
    /// Default impl ignores `role` and delegates to [`query_rows`] — backends that
    /// don't model Postgres RLS roles (e.g. ClickHouse) just run the query
    /// normally. Callers must only reach this path after a server-side admin-role
    /// check (see `read/exec.rs`), so the default fallthrough never widens access.
    async fn query_rows_as_role(&self, q: &BoundQuery<'_>, _role: &str) -> ApiResult<Vec<Map<String, Value>>> {
        self.query_rows(q).await
    }

    /// Execute a bound read query with the RLS tenant GUC pinned to `sub` for
    /// THIS query only (Phase 0c self-tenant user-inspection). The Postgres
    /// backend runs it inside a `READ ONLY` transaction with
    /// `select set_config('app.tenant_id', <sub>, true)` (the `true` = local,
    /// so it reverts at txn end — no pool leakage) so RLS-scoped tables
    /// (`core.tenant_strategies`) filter to exactly the caller's tenant. Unlike
    /// [`query_rows_as_role`] this does NOT change the session role — it only
    /// sets the tenant GUC, keeping the pool's normal RLS-forced role. `sub` MUST
    /// be the server-authenticated `Identity.sub`, never a client-supplied value
    /// (the read handler enforces this — see `read/exec.rs`).
    ///
    /// Default impl ignores `sub` and delegates to [`query_rows`] — backends that
    /// don't model Postgres RLS GUCs (e.g. ClickHouse) run the query normally
    /// (the self-tenant endpoints are Postgres-backed by construction, and the
    /// server-injected `WHERE tenant_id = :sub` filter is the primary scoping).
    async fn query_rows_as_tenant(&self, q: &BoundQuery<'_>, _sub: &str) -> ApiResult<Vec<Map<String, Value>>> {
        self.query_rows(q).await
    }
}

/// Validate + double-quote a Postgres role identifier for a `SET LOCAL ROLE`.
/// Rejects nothing but escapes embedded quotes so a hostile `LUMID_ADMIN_READ_ROLE`
/// can't break out of the quoted identifier — defence in depth (the value is
/// operator-configured, not user input, but the role name reaches a `batch_execute`).
/// Mirrors the `quote_pg_ident` guard used by the retrieval replayer.
pub(crate) fn quote_role_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::quote_role_ident;

    #[test]
    fn plain_role_is_double_quoted() {
        assert_eq!(quote_role_ident("lqt_admin_read"), "\"lqt_admin_read\"");
    }

    #[test]
    fn embedded_quotes_are_escaped_no_breakout() {
        // A hostile role name can't terminate the quoted identifier and inject SQL.
        assert_eq!(
            quote_role_ident("a\"; DROP ROLE x; --"),
            "\"a\"\"; DROP ROLE x; --\""
        );
    }
}
