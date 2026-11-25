//! Scope `registry`: the `jobs.*` rows that cross between hubs.
//!
//! Ownership follows location. Every synced row names its only writer in
//! `home_hub`; a hub emits ONLY its own rows, and applies an incoming row only
//! when
//!
//! ```text
//! incoming.updated_at > local.updated_at  AND  incoming.home_hub != LUMID_HUB_ID
//! ```
//!
//! plus one stricter check this module adds: the row's `home_hub` must be the
//! peer the batch came from (a hub forwards nobody else's rows). A hub never
//! overwrites its own rows, whatever the peer claims. Deletes are tombstones
//! (`deleted_at`) and travel as ordinary column values.
//!
//! The same [`collect`] serves `GET /sync/outbox` (the peer pulls) and the
//! entrance hub's push (it collects locally and POSTs to `/sync/inbox`), and the
//! same [`apply_batch`] runs behind `/sync/inbox` and on pulled batches — so
//! both directions share one batch format, one ledger, one apply rule.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// How a table's rows are versioned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Mutable row keyed by `pk`; newer `updated_at` wins (owner-only writes).
    Upsert,
    /// Append-only log with a hub-local serial `id`: shipped as `origin_id`,
    /// deduped on `(home_hub, origin_id)`, never updated.
    AppendOnly,
}

/// One synced relation.
#[derive(Clone, Copy, Debug)]
pub struct RegTable {
    pub name: &'static str,
    pub pk: &'static [&'static str],
    /// Version / watermark column (`updated_at`; `at` for the event log).
    pub version: &'static str,
    pub kind: Kind,
}

/// Every relation in scope `registry`, in FK order: a parent table is drained
/// (and applied) before its children in the same cycle.
pub const REGISTRY_TABLES: &[RegTable] = &[
    RegTable {
        name: "jobs.hubs",
        pk: &["hub_id"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.sources",
        pk: &["source_id"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.source_grants",
        pk: &["source_id", "grantee_sub", "access"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.source_locations",
        pk: &["source_id", "site"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.site_routes",
        pk: &["route_id"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.jobs",
        pk: &["job_id"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.job_sources",
        pk: &["job_id", "ref", "access"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.job_events",
        pk: &["id"],
        version: "at",
        kind: Kind::AppendOnly,
    },
    RegTable {
        name: "jobs.plans",
        pk: &["plan_id"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.quotas",
        pk: &["owner_sub"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
    RegTable {
        name: "jobs.catalog_mirror",
        pk: &["hub_id", "schema_name", "table_name"],
        version: "updated_at",
        kind: Kind::Upsert,
    },
];

/// `jobs.*` relations that NEVER leave the hub: a credential stays where it
/// was issued. The inbox refuses them by name.
pub const LOCAL_ONLY: &[&str] = &["jobs.job_tokens"];

pub fn table(name: &str) -> Option<&'static RegTable> {
    REGISTRY_TABLES.iter().find(|t| t.name == name)
}

/// One registry batch — the wire format of both `/sync/inbox` (body) and
/// `/sync/outbox` (`batches[]`). Single-table; `hub` is the sender and the
/// owner of every record in it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryBatch {
    /// Content hash of `(hub, table, records)`: a redelivered batch dedups in
    /// `sync.inbox_ledger` on `(peer, batch_id)`.
    pub batch_id: Uuid,
    #[serde(default = "registry_scope")]
    pub scope: String,
    pub hub: String,
    pub table: String,
    pub records: Vec<Value>,
}

fn registry_scope() -> String {
    "registry".into()
}

/// Per-batch apply result.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegistryAck {
    pub batch_id: Uuid,
    pub inserted: i64,
    pub updated: i64,
    /// Not newer than the local copy (re-reads of the overlap window land here).
    pub stale: i64,
    /// Rows claiming THIS hub as owner, or keyed onto a row this hub owns —
    /// never applied (the core invariant).
    pub skipped_own: i64,
    /// Rows refused: owned by a third hub, malformed, or failing a constraint.
    pub rejected: i64,
    pub duplicate: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

/// Keyset position per table: `(version, key)` of the last row delivered.
pub type Cursor = BTreeMap<String, (DateTime<Utc>, String)>;

pub fn parse_cursor(raw: Option<&str>) -> ApiResult<Cursor> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(Cursor::new()),
        Some(s) => {
            serde_json::from_str(s).map_err(|e| ApiError::BadRequest(format!("bad cursor: {e}")))
        }
    }
}

pub fn encode_cursor(c: &Cursor) -> String {
    serde_json::to_string(c).unwrap_or_else(|_| "{}".into())
}

/// Step every position back by `overlap_s` and drop the tiebreak key, so a
/// cycle re-reads the recent window (see `LUMID_HUB_SYNC_OVERLAP_SECS`).
pub fn rewind(c: &Cursor, overlap_s: u64) -> Cursor {
    let d = chrono::Duration::seconds(overlap_s.min(i64::MAX as u64) as i64);
    c.iter()
        .map(|(t, (ts, _))| (t.clone(), (*ts - d, String::new())))
        .collect()
}

/// Newest watermark in a cursor (for the status page's cursor age).
pub fn newest(c: &Cursor) -> Option<DateTime<Utc>> {
    c.values().map(|(ts, _)| *ts).max()
}

fn key_expr(t: &RegTable) -> String {
    let parts: Vec<String> = t.pk.iter().map(|c| format!("t.\"{c}\"::text")).collect();
    if parts.len() == 1 {
        parts[0].clone()
    } else {
        format!("concat_ws('|', {})", parts.join(", "))
    }
}

/// The record a table emits: the row, with `home_hub` made explicit (NULL on
/// a few tables means "written here before sync existed" = this hub's).
fn emit_expr(t: &RegTable) -> &'static str {
    match t.kind {
        Kind::Upsert => "jsonb_set(to_jsonb(t), '{home_hub}', to_jsonb($1::text))",
        Kind::AppendOnly => {
            "(to_jsonb(t) - 'id') || jsonb_build_object('origin_id', t.id, 'home_hub', $1::text)"
        }
    }
}

fn collect_sql(t: &RegTable) -> String {
    let k = key_expr(t);
    let v = t.version;
    let own = match t.kind {
        // A received copy of a peer's event carries an origin_id; ours never do.
        Kind::AppendOnly => "COALESCE(t.home_hub, $1) = $1 AND t.origin_id IS NULL",
        Kind::Upsert => "COALESCE(t.home_hub, $1) = $1",
    };
    format!(
        "SELECT {emit} AS rec, t.\"{v}\" AS ver, {k} AS k FROM {name} t \
          WHERE {own} \
            AND ($2::timestamptz IS NULL OR (t.\"{v}\", {k}) > ($2::timestamptz, $3::text)) \
          ORDER BY t.\"{v}\", {k} LIMIT $4",
        emit = emit_expr(t),
        name = t.name,
    )
}

/// Content hash of one batch (reuses the data plane's deterministic id).
pub fn batch_id(hub: &str, table: &str, records: &[Value]) -> Uuid {
    let (schema, tbl) = table.split_once('.').unwrap_or(("jobs", table));
    crate::sync::push::deterministic_batch_id(hub, schema, tbl, "registry", records)
}

/// Rows THIS hub owns, past `cursor`, up to `limit` per table. Returns the
/// batches (one per non-empty table, FK order), the advanced cursor, and
/// whether any table filled its page (call again).
pub async fn collect(
    st: &AppState,
    cursor: &Cursor,
    limit: i64,
) -> ApiResult<(Vec<RegistryBatch>, Cursor, bool)> {
    let hub = st.settings.hub_id.clone();
    let client = st.pool.get().await?;
    let mut out = Vec::new();
    let mut next = cursor.clone();
    let mut more = false;
    for t in REGISTRY_TABLES {
        let (since, key) = match cursor.get(t.name) {
            Some((ts, k)) => (Some(*ts), Some(k.clone())),
            None => (None, None),
        };
        let rows = client
            .query(&collect_sql(t), &[&hub, &since, &key, &limit])
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("outbox {}: {e}", t.name)))?;
        if rows.is_empty() {
            continue;
        }
        if rows.len() as i64 >= limit {
            more = true;
        }
        let last = rows.last().unwrap();
        next.insert(t.name.to_string(), (last.get("ver"), last.get("k")));
        let records: Vec<Value> = rows.iter().map(|r| r.get::<_, Value>("rec")).collect();
        out.push(RegistryBatch {
            batch_id: batch_id(&hub, t.name, &records),
            scope: "registry".into(),
            hub: hub.clone(),
            table: t.name.to_string(),
            records,
        });
    }
    Ok((out, next, more))
}

fn q(c: &str) -> String {
    format!("\"{}\"", c.replace('"', "\"\""))
}

fn upsert_sql(t: &RegTable, cols: &[String]) -> String {
    let collist = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    let sel = cols
        .iter()
        .map(|c| format!("r.{}", q(c)))
        .collect::<Vec<_>>()
        .join(", ");
    match t.kind {
        Kind::AppendOnly => format!(
            "INSERT INTO {n} ({collist}) SELECT {sel} FROM jsonb_populate_record(NULL::{n}, $1) r \
             ON CONFLICT (home_hub, origin_id) WHERE origin_id IS NOT NULL DO NOTHING \
             RETURNING true AS inserted",
            n = t.name
        ),
        Kind::Upsert => {
            let pk = t.pk.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
            let set = cols
                .iter()
                .filter(|c| !t.pk.contains(&c.as_str()))
                .map(|c| format!("{0} = EXCLUDED.{0}", q(c)))
                .collect::<Vec<_>>()
                .join(", ");
            // The WHERE is the core invariant, enforced in the database: only
            // a strictly newer version, and never onto a row this hub owns
            // (`$2` = LUMID_HUB_ID; NULL home_hub = written here pre-sync).
            format!(
                "INSERT INTO {n} AS t ({collist}) SELECT {sel} FROM jsonb_populate_record(NULL::{n}, $1) r \
                 ON CONFLICT ({pk}) DO UPDATE SET {set} \
                 WHERE t.{v} < EXCLUDED.{v} AND COALESCE(t.home_hub, $2) <> $2 \
                 RETURNING (xmax = 0) AS inserted",
                n = t.name,
                v = q(t.version),
            )
        }
    }
}

/// Does the local row this record keys onto belong to this hub? Typed PK
/// comparison so the primary-key index serves it.
fn own_sql(t: &RegTable) -> String {
    let lhs =
        t.pk.iter()
            .map(|c| format!("t.{}", q(c)))
            .collect::<Vec<_>>()
            .join(", ");
    let rhs =
        t.pk.iter()
            .map(|c| format!("r.{}", q(c)))
            .collect::<Vec<_>>()
            .join(", ");
    format!(
        "SELECT COALESCE(t.home_hub, $2) = $2 FROM {n} t \
          WHERE ({lhs}) = (SELECT {rhs} FROM jsonb_populate_record(NULL::{n}, $1) r)",
        n = t.name
    )
}

async fn local_columns(client: &tokio_postgres::Client, rel: &str) -> ApiResult<HashSet<String>> {
    let rows = client
        .query(
            "SELECT attname::text FROM pg_attribute \
              WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
            &[&rel],
        )
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("columns of {rel}: {e}")))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Pure per-record ownership decision.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Apply,
    /// Claims to be ours: never applied.
    Own,
    /// Not the sender's row, or no owner at all.
    Foreign,
}

pub fn verdict(rec: &Value, self_hub: &str, peer: &str) -> Verdict {
    match rec.get("home_hub").and_then(Value::as_str) {
        Some(h) if h == self_hub => Verdict::Own,
        Some(h) if h == peer => Verdict::Apply,
        _ => Verdict::Foreign,
    }
}

/// Apply one registry batch from `peer` to THIS hub. Behind `/sync/inbox`
/// (pushed) and the scheduler's pull (fetched from the peer's outbox).
pub async fn apply_batch(st: &AppState, peer: &str, b: &RegistryBatch) -> ApiResult<RegistryAck> {
    let self_hub = st.settings.hub_id.as_str();
    if LOCAL_ONLY.contains(&b.table.as_str()) {
        return Err(ApiError::Forbidden(format!(
            "{} is local-only and never syncs",
            b.table
        )));
    }
    let t = table(&b.table).ok_or_else(|| {
        ApiError::Forbidden(format!("{} is not in sync scope `registry`", b.table))
    })?;
    if b.scope != "registry" {
        return Err(ApiError::BadRequest(format!("unknown scope `{}`", b.scope)));
    }
    if b.hub != peer {
        return Err(ApiError::Forbidden(format!(
            "batch names hub `{}` but the credential is peer `{peer}`",
            b.hub
        )));
    }
    if peer == self_hub {
        return Err(ApiError::Forbidden("a hub cannot be its own peer".into()));
    }

    let mut client = st.pool.get().await?;
    let mut ack = RegistryAck {
        batch_id: b.batch_id,
        ..Default::default()
    };
    if client
        .query_opt(
            "SELECT 1 FROM sync.inbox_ledger WHERE peer = $1 AND batch_id = $2",
            &[&peer, &b.batch_id],
        )
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("ledger lookup: {e}")))?
        .is_some()
    {
        ack.duplicate = true;
        return Ok(ack);
    }

    let local = local_columns(&client, t.name).await?;
    if local.is_empty() {
        return Err(ApiError::Unavailable(format!(
            "{} does not exist on this hub",
            t.name
        )));
    }
    let mut sql_cache: HashMap<Vec<String>, String> = HashMap::new();
    let mut tx = client
        .transaction()
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("begin: {e}")))?;
    for rec in &b.records {
        match verdict(rec, self_hub, peer) {
            Verdict::Own => {
                ack.skipped_own += 1;
                continue;
            }
            Verdict::Foreign => {
                ack.rejected += 1;
                ack.first_error
                    .get_or_insert_with(|| "row not owned by the sending hub".into());
                continue;
            }
            Verdict::Apply => {}
        }
        let Some(obj) = rec.as_object() else {
            ack.rejected += 1;
            continue;
        };
        let mut cols: Vec<String> = obj
            .keys()
            .filter(|k| local.contains(k.as_str()))
            .filter(|k| !(t.kind == Kind::AppendOnly && k.as_str() == "id"))
            .cloned()
            .collect();
        cols.sort();
        let required: Vec<&str> = match t.kind {
            Kind::Upsert => {
                t.pk.iter()
                    .copied()
                    .chain([t.version, "home_hub"])
                    .collect()
            }
            Kind::AppendOnly => vec!["origin_id", "home_hub"],
        };
        if let Some(missing) = required.iter().find(|c| !cols.iter().any(|x| x == *c)) {
            ack.rejected += 1;
            ack.first_error
                .get_or_insert_with(|| format!("record lacks `{missing}`"));
            continue;
        }
        let sql = sql_cache
            .entry(cols.clone())
            .or_insert_with(|| upsert_sql(t, &cols));
        let sp = tx
            .savepoint("hub_row")
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("savepoint: {e}")))?;
        let res = match t.kind {
            Kind::Upsert => sp.query_opt(sql.as_str(), &[rec, &self_hub]).await,
            Kind::AppendOnly => sp.query_opt(sql.as_str(), &[rec]).await,
        };
        match res {
            Ok(row) => {
                sp.commit()
                    .await
                    .map_err(|e| ApiError::Internal(anyhow::anyhow!("release: {e}")))?;
                match row {
                    Some(r) if r.get::<_, bool>("inserted") => ack.inserted += 1,
                    Some(_) => ack.updated += 1,
                    // 0 rows: not newer, or keyed onto a row THIS hub owns.
                    // Tell them apart (PK lookup) so a takeover attempt shows.
                    None if t.kind == Kind::Upsert => {
                        let own: bool = tx
                            .query_opt(&own_sql(t), &[rec, &self_hub])
                            .await
                            .ok()
                            .flatten()
                            .map(|r| r.get(0))
                            .unwrap_or(false);
                        if own {
                            ack.skipped_own += 1;
                        } else {
                            ack.stale += 1;
                        }
                    }
                    None => ack.stale += 1,
                }
            }
            Err(e) => {
                let _ = sp.rollback().await;
                ack.rejected += 1;
                ack.first_error.get_or_insert_with(|| {
                    e.as_db_error()
                        .map(|d| d.message().to_string())
                        .unwrap_or_else(|| e.to_string())
                });
            }
        }
    }
    if ack.rejected == 0 {
        // Terminal batches only: a partly rejected one re-applies on redelivery.
        tx.execute(
            "INSERT INTO sync.inbox_ledger \
               (peer, batch_id, target_schema, target_table, inserted, updated, n_rows) \
             VALUES ($1, $2, 'jobs', $3, $4, $5, $6) ON CONFLICT (peer, batch_id) DO NOTHING",
            &[
                &peer,
                &b.batch_id,
                &t.name.trim_start_matches("jobs."),
                &ack.inserted,
                &ack.updated,
                &(b.records.len() as i64),
            ],
        )
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("ledger insert: {e}")))?;
    }
    tx.commit()
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("commit: {e}")))?;
    if ack.inserted + ack.updated > 0
        && matches!(
            t.name,
            "jobs.sources" | "jobs.source_grants" | "jobs.job_sources" | "jobs.jobs"
        )
    {
        // Job-token scope decisions are cached; a peer's revoke/tombstone must
        // take effect here as it would on its own hub.
        crate::jobs::token::invalidate_all();
    }
    Ok(ack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_jobs_relation_is_synced_or_local_only() {
        for rel in crate::jobs::schema::JOBS_RELATIONS {
            let synced = table(rel).is_some();
            let local = LOCAL_ONLY.contains(rel);
            assert!(
                synced ^ local,
                "{rel}: must be exactly one of synced / local-only"
            );
        }
        assert!(table("jobs.job_tokens").is_none());
    }

    #[test]
    fn fk_parents_drain_before_children() {
        let pos = |n: &str| REGISTRY_TABLES.iter().position(|t| t.name == n).unwrap();
        for child in ["jobs.source_grants", "jobs.source_locations"] {
            assert!(pos("jobs.sources") < pos(child));
        }
        for child in ["jobs.job_sources", "jobs.job_events"] {
            assert!(pos("jobs.jobs") < pos(child));
        }
    }

    #[test]
    fn verdict_enforces_ownership() {
        let r = |h: &str| json!({"home_hub": h});
        assert_eq!(verdict(&r("nus"), "home", "nus"), Verdict::Apply);
        assert_eq!(verdict(&r("home"), "home", "nus"), Verdict::Own);
        assert_eq!(verdict(&r("office"), "home", "nus"), Verdict::Foreign);
        assert_eq!(verdict(&json!({}), "home", "nus"), Verdict::Foreign);
    }

    #[test]
    fn upsert_guards_version_and_own_rows() {
        let t = table("jobs.sources").unwrap();
        let sql = upsert_sql(
            t,
            &[
                "home_hub".into(),
                "name".into(),
                "source_id".into(),
                "updated_at".into(),
            ],
        );
        assert!(sql.contains("ON CONFLICT (\"source_id\") DO UPDATE"));
        assert!(sql.contains(
            "WHERE t.\"updated_at\" < EXCLUDED.\"updated_at\" AND COALESCE(t.home_hub, $2) <> $2"
        ));
        assert!(!sql.contains("\"source_id\" = EXCLUDED"), "pk is never SET");
        let ev = upsert_sql(
            table("jobs.job_events").unwrap(),
            &["home_hub".into(), "origin_id".into()],
        );
        assert!(
            ev.contains("ON CONFLICT (home_hub, origin_id) WHERE origin_id IS NOT NULL DO NOTHING")
        );
    }

    #[test]
    fn collect_only_reads_own_rows() {
        for t in REGISTRY_TABLES {
            let sql = collect_sql(t);
            assert!(sql.contains("COALESCE(t.home_hub, $1) = $1"), "{}", t.name);
            assert!(sql.contains(&format!("ORDER BY t.\"{}\"", t.version)));
        }
    }

    #[test]
    fn cursor_roundtrip_and_rewind() {
        let ts = DateTime::parse_from_rfc3339("2026-11-25T10:00:00.123456Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut c = Cursor::new();
        c.insert("jobs.sources".into(), (ts, "src_1".into()));
        let back = parse_cursor(Some(&encode_cursor(&c))).unwrap();
        assert_eq!(back, c);
        let r = rewind(&c, 120);
        assert_eq!(r["jobs.sources"].0, ts - chrono::Duration::seconds(120));
        assert_eq!(r["jobs.sources"].1, "");
        assert!(parse_cursor(Some("")).unwrap().is_empty());
        assert!(parse_cursor(Some("nope")).is_err());
    }

    #[test]
    fn batch_id_is_content_addressed() {
        let a = batch_id("nus", "jobs.sources", &[json!({"x": 1})]);
        assert_eq!(a, batch_id("nus", "jobs.sources", &[json!({"x": 1})]));
        assert_ne!(a, batch_id("nus", "jobs.sources", &[json!({"x": 2})]));
        assert_ne!(a, batch_id("home", "jobs.sources", &[json!({"x": 1})]));
    }
}
