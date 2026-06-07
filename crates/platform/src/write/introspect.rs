//! Target-table introspection — port of `writeengine.get_target_columns`,
//! `get_unique_columns`, and the column metadata `validation._introspect`
//! needs. Results are cached in a moka cache (keyed `schema.table`) so the
//! hot ingest path doesn't re-walk `information_schema` on every request; the
//! cache is cleared by the admin `refresh-schemas` route.

use std::sync::Arc;

use moka::future::Cache;
use once_cell::sync::Lazy;
use tokio_postgres::Client;

/// One writable column of a target table.
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    /// `information_schema.columns.data_type` (e.g. "numeric", "timestamp with
    /// time zone", "ARRAY"). Used by the validator + coercer.
    pub data_type: String,
    /// `udt_name` (e.g. "_text" for text[]). Disambiguates ARRAY element type.
    pub udt_name: String,
    pub is_nullable: bool,
    pub has_default: bool,
}

/// Cached per-table metadata: the writable column list (generated/identity
/// columns already dropped) + the natural key columns.
#[derive(Clone, Debug)]
pub struct TableMeta {
    pub columns: Vec<ColumnInfo>,
    /// Natural-key columns (first non-id UNIQUE/PK constraint).
    pub conflict_cols: Vec<String>,
}

impl TableMeta {
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
    pub fn col_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

// 256 tables with a 5-minute TTL so DDL changes made outside the app are
// picked up within minutes (without needing a manual /admin/ingest/schemas/refresh).
static META_CACHE: Lazy<Cache<String, Arc<TableMeta>>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(256)
        .time_to_live(std::time::Duration::from_secs(300))
        .build()
});

fn key(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// Clear the whole metadata cache (admin `refresh-schemas`).
pub fn refresh_cache() {
    META_CACHE.invalidate_all();
}

/// Fetch (or build + cache) the metadata for `schema.table`. Returns None when
/// the table has no columns (== unknown table).
pub async fn table_meta(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<Arc<TableMeta>>, tokio_postgres::Error> {
    let k = key(schema, table);
    if let Some(m) = META_CACHE.get(&k).await {
        return Ok(Some(m));
    }
    let columns = get_target_columns(client, schema, table).await?;
    if columns.is_empty() {
        return Ok(None);
    }
    let conflict_cols = get_unique_columns(client, schema, table).await?;
    let meta = Arc::new(TableMeta { columns, conflict_cols });
    META_CACHE.insert(k, meta.clone()).await;
    Ok(Some(meta))
}

/// Whether the table exists at all (used by the route split-gate probe).
pub async fn table_exists(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema=$1 AND table_name=$2",
            &[&schema, &table],
        )
        .await?;
    Ok(row.is_some())
}

/// Port of `writeengine.get_target_columns` — writable columns only
/// (generated ALWAYS / identity ALWAYS excluded).
pub async fn get_target_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT column_name, data_type, udt_name, is_nullable, column_default, \
                    is_generated, identity_generation \
               FROM information_schema.columns \
              WHERE table_schema = $1 AND table_name = $2 \
              ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let gen: Option<String> = r.get("is_generated");
        let ident: Option<String> = r.get("identity_generation");
        if gen.as_deref() == Some("ALWAYS") {
            continue;
        }
        if ident.as_deref() == Some("ALWAYS") {
            continue;
        }
        let nullable: String = r.get("is_nullable");
        let default: Option<String> = r.get("column_default");
        out.push(ColumnInfo {
            name: r.get("column_name"),
            data_type: r.get("data_type"),
            udt_name: r.get("udt_name"),
            is_nullable: nullable == "YES",
            has_default: default.is_some(),
        });
    }
    Ok(out)
}

/// Generated-ALWAYS columns (need dropping from the temp staging clone).
pub async fn generated_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    // Columns that can't be COPY/INSERT-ed into and must be dropped from the
    // staging temp: GENERATED ALWAYS AS (expr) STORED, AND identity columns.
    // `LIKE ... INCLUDING DEFAULTS` copies a serial's nextval default (fine) but
    // NOT an identity generator, leaving an identity `id` NOT-NULL-without-
    // default → COPY would fail. The merge never inserts `id`, so dropping it
    // from the temp is safe for serial + identity alike.
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns \
              WHERE table_schema=$1 AND table_name=$2 \
                AND (is_generated='ALWAYS' OR identity_generation IS NOT NULL)",
            &[&schema, &table],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get::<_, String>("column_name")).collect())
}

/// Port of `writeengine.get_unique_columns` — first non-`id` UNIQUE constraint,
/// else first PK group, else empty.
pub async fn get_unique_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT kcu.column_name, tc.constraint_type, tc.constraint_name \
               FROM information_schema.table_constraints tc \
               JOIN information_schema.key_column_usage kcu \
                 ON tc.constraint_name = kcu.constraint_name \
                AND tc.table_schema = kcu.table_schema \
              WHERE tc.table_schema = $1 AND tc.table_name = $2 \
                AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY') \
              ORDER BY tc.constraint_type DESC, kcu.ordinal_position",
            &[&schema, &table],
        )
        .await?;
    // Group by (ctype, cname) preserving first-seen order.
    let mut groups: Vec<((String, String), Vec<String>)> = Vec::new();
    for r in &rows {
        let col: String = r.get("column_name");
        let ctype: String = r.get("constraint_type");
        let cname: String = r.get("constraint_name");
        let gkey = (ctype, cname);
        if let Some(g) = groups.iter_mut().find(|(k, _)| *k == gkey) {
            g.1.push(col);
        } else {
            groups.push((gkey, vec![col]));
        }
    }
    for ((ctype, _), cols) in &groups {
        if ctype == "UNIQUE" && !cols.iter().any(|c| c == "id") {
            return Ok(cols.clone());
        }
    }
    Ok(groups.into_iter().next().map(|(_, cols)| cols).unwrap_or_default())
}
