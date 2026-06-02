//! The ClickHouse backend (multi-backend, Phase B).
//!
//! Lets the operator approve a table onto **ClickHouse** instead of Postgres
//! (the per-table backend choice recorded in `provenance.table_backend`). The
//! writer is unaffected — it still POSTs to `/ingest/:schema/:table`; the
//! registry routes the write here when the table's backend row says
//! `clickhouse`.
//!
//! Design notes (and the honest semantic gaps vs Postgres):
//!
//!   * **`table_meta`** reads `system.columns` (+ `system.tables.sorting_key`
//!     for the ORDER BY key) and maps CH types onto the same `TableMeta` /
//!     `ColumnInfo` shape the PG path uses. `Nullable(T)` → `is_nullable=true`;
//!     everything else is required. The ORDER BY key columns become
//!     `conflict_cols` so the validator's "no UNIQUE/PK ⇒ refuse" guard is
//!     satisfied (a `ReplacingMergeTree` ORDER BY key *is* the dedup key).
//!
//!   * **`create_table`** emits
//!     `ENGINE = ReplacingMergeTree(ingest_ts) ORDER BY (<natural key>, source)`.
//!     ReplacingMergeTree keeps the row with the largest `ingest_ts` per ORDER
//!     BY tuple — but **only on background merge**, so duplicates are
//!     transiently visible and `FINAL` (or a GROUP BY) is needed for exact
//!     reads. There is no synchronous upsert.
//!
//!   * **`write_records`** does a **dynamic** `INSERT … FORMAT JSONEachRow`
//!     over the HTTP interface (the `clickhouse` crate's typed `insert::<T>()`
//!     can't take a runtime-shaped schema). It stamps the four provenance
//!     columns (`source`, `source_endpoint`, `source_run_id`, `ingest_ts`) +
//!     `raw`, and returns `(received, 0)` — CH cannot report exact
//!     inserted-vs-updated at write time because dedup is deferred to merge.
//!
//!   * **`query_rows`** runs the (already-bound) SQL with
//!     `FORMAT JSONEachRow` and parses the NDJSON response into JSON objects.
//!     NOTE: the **read dialect** (CH SQL vs PG SQL, `$N` vs `?` binds) is a
//!     Phase C concern — see `query_rows` below. A read endpoint whose first
//!     table resolves to CH will still be sent PG-shaped SQL; authoring CH read
//!     specs is deferred.

use std::sync::Arc;

use async_trait::async_trait;
use clickhouse::Client;
use serde_json::{Map, Value};

use super::{Backend, BackendKind, BoundQuery, CreateTablePlan, WriteRequest};
use crate::error::{ApiError, ApiResult};
use crate::read::bind::BindValue;
use crate::validation::SERVER_STAMPED_COLS;
use crate::write::introspect::{ColumnInfo, TableMeta};

/// Translate the PG-dialect placeholders the bind layer emits into ClickHouse's
/// `?` positional form: each `$N` (optionally followed by an `::int8`/`::float8`
/// cast) becomes a single `?`. The bind layer numbers placeholders `$1,$2,…` in
/// left-to-right order, so the `?`s come out in the same order as `q.binds`.
fn pg_placeholders_to_ch(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            // Strip the inline numeric-widening cast the PG path adds.
            for cast in ["::int8", "::float8"] {
                if sql[j..].starts_with(cast) {
                    j += cast.len();
                    break;
                }
            }
            out.push('?');
            i = j;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Validate + normalise a SQL identifier to `^[a-z_][a-z0-9_]{0,62}$`. Same
/// rule the PG backend uses (`postgres::norm_ident`) so a table can be created
/// on either backend from the identical inferred schema.
fn norm_ident(s: &str) -> Option<String> {
    let l = s.trim().to_lowercase();
    let ok = !l.is_empty()
        && l.len() <= 63
        && l.chars().next().map(|c| c.is_ascii_lowercase() || c == '_').unwrap_or(false)
        && l.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    ok.then_some(l)
}

/// Map a Postgres inferred type (the proposal's `inferred_schema` values — one
/// of `text | bigint | double precision | boolean | jsonb`) onto a ClickHouse
/// column type. Unknown → `String` (the safe text fallback, matching the PG
/// builder's `_ => "text"`).
fn pg_type_to_ch(pg: &str) -> &'static str {
    match pg {
        "bigint" => "Int64",
        "double precision" => "Float64",
        "boolean" => "Bool",
        // jsonb has no native CH analogue in the inferred-shape set; store the
        // JSON text as a String (the `raw` provenance column is also String).
        "jsonb" => "String",
        _ => "String",
    }
}

/// Build the `CREATE TABLE IF NOT EXISTS … ENGINE = ReplacingMergeTree …` DDL
/// for an approved proposal. Returns `(db, table, ddl)` with all identifiers
/// re-validated + backtick-quoted. Public so it can be unit-tested for shape.
pub fn build_create_table_ddl(
    plan: &CreateTablePlan<'_>,
) -> ApiResult<(String, String, String)> {
    let schema_n = norm_ident(plan.schema).ok_or_else(|| ApiError::BadRequest("bad schema".into()))?;
    let table_n = norm_ident(plan.table).ok_or_else(|| ApiError::BadRequest("bad table".into()))?;

    let mut col_ddl = Vec::new();
    for (c, ty) in plan.inferred {
        let c_n = norm_ident(c).ok_or_else(|| ApiError::BadRequest(format!("bad column {c:?}")))?;
        let ch_ty = pg_type_to_ch(ty.as_str().unwrap_or("text"));
        col_ddl.push(format!("`{c_n}` {ch_ty}"));
    }

    // ORDER BY = inferred natural key (present cols only) + source, so a
    // multi-source firehose dedups per (key, source). Empty key ⇒ ORDER BY
    // (source) alone (no surrogate identity column on CH — there's no IDENTITY).
    let key_n: Vec<String> = plan
        .key
        .iter()
        .filter_map(|k| norm_ident(k))
        .filter(|k| plan.inferred.contains_key(k))
        .collect();
    let mut order_cols: Vec<String> = key_n.iter().map(|c| format!("`{c}`")).collect();
    order_cols.push("source".to_string());
    let order_by = order_cols.join(", ");

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS `{schema_n}`.`{table_n}` (\n  {cols},\n\
           `source` String,\n  `source_endpoint` String,\n\
           `source_run_id` String,\n  `ingest_ts` DateTime64(6),\n  `raw` String\n\
         )\nENGINE = ReplacingMergeTree(ingest_ts)\nORDER BY ({order_by})",
        cols = col_ddl.join(",\n  "),
    );
    Ok((schema_n, table_n, ddl))
}

/// ClickHouse storage backend. Cheap to clone (the `clickhouse::Client` is an
/// `Arc`-backed handle), so the registry stashes one `Arc<dyn Backend>`.
#[derive(Clone)]
pub struct ClickHouseBackend {
    client: Client,
}

impl ClickHouseBackend {
    /// Build a backend bound to `database`. Mirrors lqt-store-clickhouse's
    /// `base_client` builder, incl. the CH 25.8 analyzer-bug workaround
    /// (`prefer_column_name_to_alias=1`) so `max(x) AS x … WHERE x >= ?`
    /// resolves the column not the alias.
    pub fn new(url: &str, user: &str, password: &str, database: &str) -> Self {
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(database)
            // T-STORE-005b (lqt-store-clickhouse): CH 25.8's analyzer resolves
            // bare column refs in WHERE against the SELECT alias list before the
            // column list, so `max(ts) AS ts … WHERE ts >= ?` mis-resolves to
            // `WHERE max(ts) >= ?` → ILLEGAL_AGGREGATION. Preferring the column
            // name restores pre-25.8 behaviour. Set at the client layer so every
            // read path inherits it without per-query plumbing.
            .with_option("prefer_column_name_to_alias", "1");
        Self { client }
    }

    /// Map a ClickHouse `system.columns.type` onto the `(data_type, udt_name,
    /// is_nullable)` triple the validator/coercer expect. `Nullable(T)` peels to
    /// the inner type with `is_nullable=true`; everything else is required.
    fn parse_ch_type(ch_type: &str) -> (String, bool) {
        let t = ch_type.trim();
        if let Some(inner) = t.strip_prefix("Nullable(").and_then(|s| s.strip_suffix(')')) {
            (inner.trim().to_string(), true)
        } else {
            (t.to_string(), false)
        }
    }
}

/// One row of `SELECT name, type FROM system.columns`.
#[derive(clickhouse::Row, serde::Deserialize)]
struct SystemColumn {
    name: String,
    #[serde(rename = "type")]
    ch_type: String,
}

#[async_trait]
impl Backend for ClickHouseBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::ClickHouse
    }

    async fn table_meta(&self, schema: &str, table: &str) -> ApiResult<Option<Arc<TableMeta>>> {
        // Column list from system.columns (params bound — no identifier
        // interpolation into the WHERE).
        let cols: Vec<SystemColumn> = self
            .client
            .query(
                "SELECT name, type FROM system.columns \
                 WHERE database = ? AND table = ? ORDER BY position",
            )
            .bind(schema)
            .bind(table)
            .fetch_all::<SystemColumn>()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse system.columns: {e}")))?;

        if cols.is_empty() {
            return Ok(None); // unknown table
        }

        let columns: Vec<ColumnInfo> = cols
            .into_iter()
            .map(|c| {
                let (data_type, is_nullable) = Self::parse_ch_type(&c.ch_type);
                ColumnInfo {
                    name: c.name,
                    udt_name: data_type.clone(),
                    data_type,
                    is_nullable,
                    // CH columns we create carry no DEFAULT; treat as no-default
                    // so NOT-NULL non-stamped cols stay required in validation.
                    has_default: false,
                }
            })
            .collect();

        // The ReplacingMergeTree ORDER BY key is the dedup/conflict key. Read it
        // from system.tables.sorting_key (comma-separated). Strip the trailing
        // `source` we appended at create so conflict_cols is the natural key.
        let sorting_key: Option<String> = self
            .client
            .query("SELECT sorting_key FROM system.tables WHERE database = ? AND name = ?")
            .bind(schema)
            .bind(table)
            .fetch_optional::<String>()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse system.tables: {e}")))?;

        let conflict_cols: Vec<String> = sorting_key
            .map(|sk| {
                sk.split(',')
                    .map(|s| s.trim().trim_matches('`').to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        Ok(Some(Arc::new(TableMeta { columns, conflict_cols })))
    }

    async fn create_table(&self, plan: &CreateTablePlan<'_>) -> ApiResult<()> {
        let (schema_n, _table_n, ddl) = build_create_table_ddl(plan)?;
        // CREATE DATABASE then CREATE TABLE (both idempotent).
        self.client
            .query(&format!("CREATE DATABASE IF NOT EXISTS `{schema_n}`"))
            .execute()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse create database: {e}")))?;
        self.client
            .query(&ddl)
            .execute()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse create table: {e}")))?;
        Ok(())
    }

    async fn write_records(&self, req: &WriteRequest<'_>) -> ApiResult<(i64, i64)> {
        if req.records.is_empty() {
            return Ok((0, 0));
        }
        let meta: &TableMeta = req.meta;
        // Writable columns of the target (excluding server-stamped — we stamp
        // those ourselves below), intersected with what's present per record.
        let writable: Vec<&str> = meta
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .filter(|c| !SERVER_STAMPED_COLS.contains(*c))
            .collect();

        let schema_n = norm_ident(req.schema)
            .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("bad schema {:?}", req.schema)))?;
        let table_n = norm_ident(req.table)
            .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("bad table {:?}", req.table)))?;

        // Build the JSONEachRow body: one provenance-stamped JSON object per
        // line. `ingest_ts` is a DateTime64(6) — CH's JSONEachRow accepts an
        // ISO-8601 / "YYYY-MM-DD HH:MM:SS.ffffff" string; we use RFC3339-ish
        // micros (UTC). source_run_id is the run UUID as text.
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let mut body = String::new();
        for rec in req.records {
            let mut row = Map::new();
            if let Some(obj) = rec.as_object() {
                for &col in &writable {
                    if let Some(v) = obj.get(col) {
                        row.insert(col.to_string(), v.clone());
                    }
                }
            }
            // Provenance stamp (overwrites any caller-supplied values — these
            // are server-owned). `raw` carries the original record verbatim as a
            // JSON string (CH `raw` column is String).
            row.insert("source".into(), Value::String(req.source.to_string()));
            row.insert("source_endpoint".into(), Value::String(req.source_endpoint.to_string()));
            row.insert("source_run_id".into(), Value::String(req.source_run_id.to_string()));
            row.insert("ingest_ts".into(), Value::String(now.clone()));
            row.insert("raw".into(), Value::String(rec.to_string()));

            body.push_str(&Value::Object(row).to_string());
            body.push('\n');
        }

        let sql = format!(
            "INSERT INTO `{schema_n}`.`{table_n}` FORMAT JSONEachRow\n{body}"
        );
        self.client
            .query(&sql)
            .execute()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse insert: {e}")))?;

        // ReplacingMergeTree dedups on background merge, so CH cannot report
        // exact inserted-vs-updated at write time. Report everything as
        // "received" (inserted); updated=0. Documented semantic gap.
        Ok((req.records.len() as i64, 0))
    }

    async fn query_rows(&self, q: &BoundQuery<'_>) -> ApiResult<Vec<Map<String, Value>>> {
        // Phase C: translate the PG-dialect placeholders the bind layer emits
        // (`$N`, with `::int8`/`::float8` casts on numeric binds) into CH's `?`
        // positional form, then bind `q.binds` (the backend-neutral values) in
        // order. Only the PLACEHOLDER dialect is translated — a read spec whose
        // SQL also uses PG-only functions/casts must be authored CH-native; the
        // platform doesn't transpile arbitrary SQL.
        let ch_sql = pg_placeholders_to_ch(q.sql);
        let mut query = self.client.query(&ch_sql);
        for b in q.binds {
            query = match b {
                BindValue::Text(s) => query.bind(s.clone()),
                BindValue::Int(i) => query.bind(*i),
                BindValue::Float(f) => query.bind(*f),
                BindValue::Bool(b) => query.bind(*b),
                // CH bind has no native date/timestamp here (default-features
                // off); send the canonical string and let CH parse/compare.
                BindValue::Date(d) => query.bind(d.to_string()),
                BindValue::Ts(t) => query.bind(t.to_rfc3339()),
            };
        }
        let mut cursor = query
            .fetch_bytes("JSONEachRow")
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse query: {e}")))?;
        let bytes = cursor
            .collect()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("clickhouse fetch: {e}")))?;

        // Parse NDJSON → Vec<Map>. Each non-empty line is one JSON object.
        let text = String::from_utf8_lossy(&bytes);
        let mut out = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(line) {
                Ok(Value::Object(o)) => out.push(o),
                Ok(_) => {} // non-object line (shouldn't happen for JSONEachRow)
                Err(e) => {
                    return Err(ApiError::Internal(anyhow::anyhow!(
                        "clickhouse JSONEachRow parse: {e}"
                    )))
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn inferred(pairs: &[(&str, &str)]) -> Map<String, Value> {
        pairs.iter().map(|(c, t)| ((*c).to_string(), json!(*t))).collect()
    }

    #[test]
    fn create_ddl_is_replacing_mergetree_with_key_plus_source_order() {
        let inf = inferred(&[
            ("tenant_id", "text"),
            ("venue", "text"),
            ("instrument_id", "bigint"),
            ("ts_event_ns", "bigint"),
            ("bid_price_ticks", "double precision"),
        ]);
        let key = vec![
            "tenant_id".to_string(),
            "venue".to_string(),
            "instrument_id".to_string(),
            "ts_event_ns".to_string(),
        ];
        let (db, tbl, ddl) = build_create_table_ddl(&CreateTablePlan {
            schema: "md",
            table: "book_bbo",
            inferred: &inf,
            key: &key,
        })
        .unwrap();
        assert_eq!(db, "md");
        assert_eq!(tbl, "book_bbo");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS `md`.`book_bbo`"), "{ddl}");
        // Engine + ordered key (natural key + source).
        assert!(ddl.contains("ENGINE = ReplacingMergeTree(ingest_ts)"), "{ddl}");
        assert!(
            ddl.contains("ORDER BY (`tenant_id`, `venue`, `instrument_id`, `ts_event_ns`, source)"),
            "{ddl}"
        );
        // Provenance columns present + CH-typed.
        assert!(ddl.contains("`source` String"), "{ddl}");
        assert!(ddl.contains("`source_run_id` String"), "{ddl}");
        assert!(ddl.contains("`ingest_ts` DateTime64(6)"), "{ddl}");
        assert!(ddl.contains("`raw` String"), "{ddl}");
        // Type mapping.
        assert!(ddl.contains("`instrument_id` Int64"), "{ddl}");
        assert!(ddl.contains("`bid_price_ticks` Float64"), "{ddl}");
        assert!(ddl.contains("`tenant_id` String"), "{ddl}");
    }

    #[test]
    fn keyless_table_orders_by_source_alone() {
        let inf = inferred(&[("foo", "text")]);
        let (_, _, ddl) = build_create_table_ddl(&CreateTablePlan {
            schema: "obs",
            table: "events",
            inferred: &inf,
            key: &[],
        })
        .unwrap();
        assert!(ddl.contains("ORDER BY (source)"), "{ddl}");
    }

    #[test]
    fn type_mapping_covers_the_inferred_set() {
        assert_eq!(pg_type_to_ch("bigint"), "Int64");
        assert_eq!(pg_type_to_ch("double precision"), "Float64");
        assert_eq!(pg_type_to_ch("boolean"), "Bool");
        assert_eq!(pg_type_to_ch("jsonb"), "String");
        assert_eq!(pg_type_to_ch("text"), "String");
        assert_eq!(pg_type_to_ch("anything_else"), "String");
    }

    #[test]
    fn parse_ch_type_peels_nullable() {
        assert_eq!(ClickHouseBackend::parse_ch_type("Int64"), ("Int64".into(), false));
        assert_eq!(
            ClickHouseBackend::parse_ch_type("Nullable(Float64)"),
            ("Float64".into(), true)
        );
        assert_eq!(
            ClickHouseBackend::parse_ch_type("Nullable(String)"),
            ("String".into(), true)
        );
    }

    /// Live CH round-trip: create → write → read. `#[ignore]`-gated; needs a
    /// running ClickHouse HTTP interface. Run with:
    ///   `LUMID_CLICKHOUSE_URL=http://127.0.0.1:8123 \
    ///    cargo test -p lumid-platform clickhouse_live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn clickhouse_live_create_write_read() {
        use crate::backend::Backend;
        use crate::write::introspect::{ColumnInfo, TableMeta};

        let url = crate::config::env_var("CLICKHOUSE_URL")
            .unwrap_or_else(|| "http://127.0.0.1:8123".into());
        let user = crate::config::env_var("CLICKHOUSE_USER").unwrap_or_else(|| "default".into());
        let pass = crate::config::env_var("CLICKHOUSE_PASSWORD").unwrap_or_default();
        let db = crate::config::env_var("CLICKHOUSE_DB").unwrap_or_else(|| "default".into());
        let be = ClickHouseBackend::new(&url, &user, &pass, &db);

        let inf = inferred(&[("venue", "text"), ("ts_event_ns", "bigint"), ("px", "double precision")]);
        let key = vec!["venue".to_string(), "ts_event_ns".to_string()];
        let plan = CreateTablePlan { schema: "md", table: "phaseb_smoke", inferred: &inf, key: &key };
        be.create_table(&plan).await.expect("create_table");

        let meta = be.table_meta("md", "phaseb_smoke").await.expect("table_meta").expect("exists");
        assert!(meta.columns.iter().any(|c| c.name == "venue"));
        assert!(!meta.conflict_cols.is_empty());

        let _ = ColumnInfo { name: String::new(), data_type: String::new(), udt_name: String::new(), is_nullable: false, has_default: false };
        let _ = TableMeta { columns: vec![], conflict_cols: vec![] };

        let run_id = uuid::Uuid::new_v4();
        let records = vec![json!({"venue":"polymarket","ts_event_ns":1700000000_i64,"px":0.42})];
        let (recv, upd) = be
            .write_records(&WriteRequest {
                schema: "md",
                table: "phaseb_smoke",
                meta: &meta,
                records: &records,
                source: "test",
                source_endpoint: "test:smoke",
                source_run_id: &run_id,
            })
            .await
            .expect("write_records");
        assert_eq!((recv, upd), (1, 0));

        let rows = be
            .query_rows(&BoundQuery {
                sql: "SELECT venue, px FROM md.phaseb_smoke LIMIT 1",
                params: vec![],
                binds: &[],
            })
            .await
            .expect("query_rows");
        assert!(!rows.is_empty());
    }

    #[test]
    fn bad_identifier_rejected() {
        let inf = inferred(&[("ok_col", "text")]);
        let r = build_create_table_ddl(&CreateTablePlan {
            schema: "md",
            table: "bad-table",
            inferred: &inf,
            key: &[],
        });
        assert!(r.is_err());
    }
}
