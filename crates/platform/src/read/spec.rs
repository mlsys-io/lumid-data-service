//! Read-endpoint spec types + loader (parses the app's read-config TOML).
//!
//! Each `[[read.endpoint]]` is a declarative read: a named-bind SQL template
//! (`:name` values, always bound — never interpolated) with allow-listed
//! `{{fragment}}` switches driven by params. Params support: required/optional,
//! defaults, range clamps, a `transform` (upper/lower), `enum` fragment maps
//! (`select`), and `present`/`absent` fragment maps for optional filters. The
//! per-request resolver (see `bind.rs`) picks fragments, substitutes them, then
//! lowers the remaining `:name` binds to positional `$N`.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use crate::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Path,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    #[default]
    None,
    Upper,
    Lower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    #[default]
    Rows,
    One,
    Envelope,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParamSpec {
    pub name: String,
    pub kind: Kind,
    /// symbol | str | int | float | date | timestamp | bool | enum
    #[serde(rename = "type")]
    pub ty: String,
    /// Path params are implicitly required; query params default optional.
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<toml::Value>,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub max_len: Option<usize>,
    #[serde(default)]
    pub transform: Transform,
    /// enum: chosen value → {fragment_name → sql}. Allow-list (never raw input).
    #[serde(default)]
    pub select: HashMap<String, HashMap<String, String>>,
    /// optional filter: fragments emitted when the param IS present (the SQL
    /// here may carry this param's `:name` bind).
    #[serde(default)]
    pub present: HashMap<String, String>,
    /// fragments emitted when the param is ABSENT (usually empty strings).
    #[serde(default)]
    pub absent: HashMap<String, String>,
}

impl ParamSpec {
    pub fn is_enum(&self) -> bool {
        self.ty == "enum" || !self.select.is_empty()
    }
    /// A presence-gated optional filter (has `present`/`absent` fragment maps).
    pub fn is_presence(&self) -> bool {
        !self.present.is_empty() || !self.absent.is_empty()
    }
    pub fn effectively_required(&self) -> bool {
        self.required || self.kind == Kind::Path
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointSpec {
    pub id: String,
    /// Human-readable description surfaced in OpenAPI + MCP tool docs.
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_method")]
    pub method: String,
    pub path: String,
    /// Source tables/views read (for cache invalidation). MV-backed endpoints
    /// list the MV name, not the base table.
    #[serde(default)]
    pub tables: Vec<String>,
    /// humantime duration string, e.g. "6h", "60s".
    pub ttl: String,
    #[serde(default)]
    pub shape: Shape,
    #[serde(default = "default_true")]
    pub strip_lineage: bool,
    #[serde(default)]
    pub row_cap: Option<i64>,
    /// For envelope shape: the key under which the array is nested (default "data").
    #[serde(default)]
    pub envelope_key: Option<String>,
    pub sql: String,
    #[serde(default, rename = "param")]
    pub params: Vec<ParamSpec>,
    /// Whether to cache responses (false for live/realtime-backed reads).
    #[serde(default = "default_true")]
    pub cache: bool,
    /// Opt-in `FINAL` for a ClickHouse-backed read (`T-READ-IR-001`): forces
    /// dedup-on-read for `ReplacingMergeTree` tables (exact, slower). Ignored on
    /// Postgres. Off by default — most reads tolerate the transient pre-merge
    /// duplicates, and `FINAL` is expensive.
    #[serde(default)]
    pub ch_final: bool,
    /// Optional per-spec query timeout in milliseconds. When set, the backend
    /// `query_rows` call is wrapped in `tokio::time::timeout`; on expiry the
    /// spec returns an empty result (200 []) rather than propagating as 500.
    /// Intended for full-text search specs over compressed hypertables where
    /// zero-match queries scan all chunks and can take minutes.
    #[serde(default)]
    pub query_timeout_ms: Option<u64>,
    /// Admin cross-tenant oversight (Phase D4). When `true`, an introspected
    /// `admin`/`super_admin`/`local` caller reads this endpoint under the
    /// configured `admin_read_role` (a `SET LOCAL ROLE` in a `READ ONLY` txn),
    /// transcending per-tenant RLS for VIEW only — so oversight endpoints
    /// (a user's strategies, `obs.runtime_cycles`, `audit.event_chain`) return
    /// all tenants' rows for an admin. NON-admin callers are UNAFFECTED (they run
    /// the normal self-scoped path — byte-identical to today), and elevation is
    /// gated ONLY on the server-introspected role, never a client-supplied field.
    /// No-op unless `LUMID_ADMIN_READ_ROLE` is also set. Default false.
    #[serde(default)]
    pub admin_cross_tenant: bool,
    /// Parsed backend-neutral query IR (`T-READ-IR-001`). Populated at spec-load
    /// when `sql` fits the bounded read-only SELECT grammar; `None` ⇒ the spec
    /// fell back to the raw-SQL Postgres path (the un-parseable construct was
    /// logged at load). Not part of the TOML surface (`#[serde(skip)]`).
    #[serde(skip)]
    pub ir: Option<super::ir::QueryIr>,
}

impl EndpointSpec {
    pub fn ttl_duration(&self) -> Duration {
        humantime::parse_duration(&self.ttl).unwrap_or(Duration::from_secs(60))
    }
    pub fn param(&self, name: &str) -> Option<&ParamSpec> {
        self.params.iter().find(|p| p.name == name)
    }
}

fn default_method() -> String {
    "GET".to_string()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct Root {
    #[serde(default)]
    read: ReadSection,
}

#[derive(Debug, Default, Deserialize)]
struct ReadSection {
    #[serde(default, rename = "endpoint")]
    endpoint: Vec<EndpointSpec>,
}

/// Parse the read-config TOML into endpoint specs + run startup lints.
pub fn load(path: &str) -> Result<Vec<EndpointSpec>, ApiError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("read {path}: {e}")))?;
    parse(&text)
}

pub fn parse(text: &str) -> Result<Vec<EndpointSpec>, ApiError> {
    let root: Root = toml::from_str(text)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("parse read config: {e}")))?;
    let mut specs = root.read.endpoint;
    for ep in &specs {
        lint(ep)?;
    }
    // Compile each spec's SQL to the backend-neutral query IR (`T-READ-IR-001`).
    // A spec whose SQL fits the bounded read-only SELECT grammar gets an IR and
    // can lower to ClickHouse as well as Postgres; one that uses an un-parseable
    // construct falls back to the raw-SQL Postgres path with a `warn!` naming the
    // construct. Either way the Postgres lowering is byte-equivalent to before —
    // the IR never changes the PG output.
    for ep in &mut specs {
        match super::parse::parse_select(&ep.sql) {
            super::parse::ParseOutcome::Ir(ir) => {
                // Per-backend compile-time validation: if the spec targets a
                // ClickHouse table but uses a construct the CH lowerer can't
                // express, surface it now (loud, at load) rather than at request
                // time. (We don't have the backend kind here — the registry
                // resolves it at runtime — so we only warn; a hard CH-incompat
                // error is raised when the request actually lowers for CH.)
                if let Err(construct) = super::dialect::validate_clickhouse(&ir) {
                    tracing::warn!(
                        spec = %ep.id,
                        "read spec '{}' IR uses a ClickHouse-incompatible construct ({}); \
                         it will only serve on Postgres",
                        ep.id,
                        construct
                    );
                }
                ep.ir = Some(*ir);
            }
            super::parse::ParseOutcome::Fallback(reason) => {
                tracing::warn!(
                    spec = %ep.id,
                    "read spec '{}' uses {} — not representable in the query IR; \
                     pinned to the raw-SQL Postgres path",
                    ep.id,
                    reason
                );
                ep.ir = None;
            }
        }
    }
    Ok(specs)
}

/// Startup lints: no `count(distinct` (TS); every `{{fragment}}` referenced by
/// the SQL is produced by some param; humantime TTL parses.
fn lint(ep: &EndpointSpec) -> Result<(), ApiError> {
    let bail = |m: String| ApiError::Internal(anyhow::anyhow!("spec '{}': {m}", ep.id));
    if ep.sql.to_lowercase().contains("count(distinct") {
        return Err(bail("count(distinct ...) is banned on hypertables".into()));
    }
    if humantime::parse_duration(&ep.ttl).is_err() {
        return Err(bail(format!("bad ttl '{}'", ep.ttl)));
    }
    // Collect fragment names produced by params.
    let mut produced: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for p in &ep.params {
        for m in p.select.values() {
            produced.extend(m.keys().map(|s| s.as_str()));
        }
        produced.extend(p.present.keys().map(|s| s.as_str()));
        produced.extend(p.absent.keys().map(|s| s.as_str()));
    }
    // Every {{frag}} in the SQL must be produced.
    for frag in fragment_names(&ep.sql) {
        if !produced.contains(frag.as_str()) {
            return Err(bail(format!("{{{{{frag}}}}}}}}} has no param producing it")));
        }
    }
    Ok(())
}

/// Extract `{{name}}` fragment identifiers from a SQL template.
pub fn fragment_names(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(end) = sql[i + 2..].find("}}") {
                let name = sql[i + 2..i + 2 + end].trim().to_string();
                if !name.is_empty() {
                    out.push(name);
                }
                i = i + 2 + end + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}
