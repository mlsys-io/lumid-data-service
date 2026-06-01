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
/// `FINDATA_CLICKHOUSE_URL` configured; otherwise the registry leaves the slot
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

/// One read query: already-bound SQL + its positional parameters. The PG backend
/// runs it on `st.pool`; a CH backend would translate the dialect (Phase C).
pub struct BoundQuery<'a> {
    pub sql: &'a str,
    pub params: Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
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
}
