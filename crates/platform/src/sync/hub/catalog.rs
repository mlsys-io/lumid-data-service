//! This hub's own entries in `jobs.catalog_mirror`: its schemas, tables, size
//! and row estimates, and freshness — what a peer's planner needs to size a
//! job against data it cannot see. The rows sync to peers in scope
//! `registry`; a peer's rows are read-only here (owned by `home_hub`).
//!
//! Estimates only, never scans: `pg_class` (`approximate_row_count` /
//! `hypertable_size` on TimescaleDB, as `queries::catalog` does), and the
//! newest finished `provenance.runs` row of an endpoint that targets the
//! table (an index probe per endpoint). Rows change `updated_at` only when a
//! value moved, so an idle warehouse syncs nothing.

use deadpool_postgres::Pool;

/// Schemas never published: platform bookkeeping and system namespaces.
pub const HIDDEN_SCHEMAS: &[&str] = &["jobs", "sync", "provenance", "raw", "information_schema"];

fn refresh_sql(ts: bool) -> String {
    let (rows, size) = if ts {
        (
            "CASE WHEN c.relkind IN ('r','p') THEN approximate_row_count(c.oid) ELSE NULL END",
            "CASE WHEN c.relkind IN ('r','p','m') THEN COALESCE(hypertable_size(c.oid), pg_total_relation_size(c.oid)) ELSE NULL END",
        )
    } else {
        (
            "CASE WHEN c.relkind IN ('r','p') THEN GREATEST(c.reltuples, 0) ELSE NULL END",
            "CASE WHEN c.relkind IN ('r','p','m') THEN pg_total_relation_size(c.oid) ELSE NULL END",
        )
    };
    format!(
        "WITH cur AS (
           SELECT n.nspname::text AS schema_name, c.relname::text AS table_name,
                  CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'matview' ELSE 'table' END AS kind,
                  ({rows})::bigint AS row_est,
                  ({size})::bigint AS size_bytes_est
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','p','m','v') AND NOT c.relispartition
              AND n.nspname <> ALL($3::text[])
              AND n.nspname NOT LIKE 'pg\\_%' AND n.nspname NOT LIKE '\\_timescaledb%'
              AND n.nspname NOT LIKE 'timescaledb%'
              AND (cardinality($4::text[]) = 0 OR n.nspname = ANY($4::text[]))
         ), fresh AS (
           SELECT cur.*, (
             SELECT max(lr.ended_at) FROM provenance.endpoints e
               CROSS JOIN LATERAL (
                 SELECT r.ended_at FROM provenance.runs r
                  WHERE r.endpoint_id = e.endpoint_id AND r.ended_at IS NOT NULL
                  ORDER BY r.started_at DESC LIMIT 1) lr
              WHERE e.target_schema = cur.schema_name AND e.target_table = cur.table_name
           ) AS freshness_at
             FROM cur
         )
         INSERT INTO jobs.catalog_mirror AS m
           (hub_id, schema_name, table_name, kind, row_est, size_bytes_est, freshness_at,
            home_hub, updated_at, deleted_at)
         SELECT $1, schema_name, table_name, kind, row_est, size_bytes_est, freshness_at, $2, now(), NULL
           FROM fresh
         ON CONFLICT (hub_id, schema_name, table_name) DO UPDATE
           SET kind = EXCLUDED.kind, row_est = EXCLUDED.row_est,
               size_bytes_est = EXCLUDED.size_bytes_est, freshness_at = EXCLUDED.freshness_at,
               home_hub = EXCLUDED.home_hub, updated_at = now(), deleted_at = NULL
         WHERE COALESCE(m.home_hub, $2) = $2
           AND (m.kind, m.row_est, m.size_bytes_est, m.freshness_at, m.deleted_at IS NULL)
               IS DISTINCT FROM
               (EXCLUDED.kind, EXCLUDED.row_est, EXCLUDED.size_bytes_est, EXCLUDED.freshness_at, true)"
    )
}

/// Result of one refresh.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Refresh {
    pub changed: u64,
    pub tombstoned: u64,
}

fn missing_timescale(e: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    e.as_db_error()
        .map(|db| {
            *db.code() == SqlState::UNDEFINED_TABLE || *db.code() == SqlState::UNDEFINED_FUNCTION
        })
        .unwrap_or(false)
}

/// Upsert this hub's tables (as `hub_id = instance`) and tombstone the ones
/// that disappeared. `only` restricts to these schemas (`LUMID_USER_SCHEMAS`).
pub async fn refresh(
    pool: &Pool,
    self_hub: &str,
    instance: &str,
    only: &[String],
) -> anyhow::Result<Refresh> {
    let mut client = pool.get().await?;
    let hidden: Vec<String> = HIDDEN_SCHEMAS.iter().map(|s| s.to_string()).collect();
    let mut tx = client.transaction().await?;
    let sp = tx.savepoint("ts").await?;
    let changed = match sp
        .execute(&refresh_sql(true), &[&instance, &self_hub, &hidden, &only])
        .await
    {
        Ok(n) => {
            sp.commit().await?;
            n
        }
        Err(e) if missing_timescale(&e) => {
            sp.rollback().await?;
            tx.execute(&refresh_sql(false), &[&instance, &self_hub, &hidden, &only])
                .await?
        }
        Err(e) => return Err(e.into()),
    };
    let tombstoned = tx
        .execute(
            "UPDATE jobs.catalog_mirror m SET deleted_at = now(), updated_at = now()
              WHERE m.hub_id = $1 AND COALESCE(m.home_hub, $2) = $2 AND m.deleted_at IS NULL
                AND to_regclass(format('%I.%I', m.schema_name, m.table_name)) IS NULL",
            &[&instance, &self_hub],
        )
        .await?;
    tx.commit().await?;
    Ok(Refresh {
        changed,
        tombstoned,
    })
}

/// Background refresher (every `LUMID_HUB_CATALOG_REFRESH_SECS`, first run at boot).
pub fn spawn(pool: Pool, self_hub: String, instance: String, only: Vec<String>, every_s: u64) {
    if every_s == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(every_s));
        loop {
            tick.tick().await;
            match refresh(&pool, &self_hub, &instance, &only).await {
                Ok(r) if r.changed + r.tombstoned > 0 => tracing::info!(
                    changed = r.changed,
                    tombstoned = r.tombstoned,
                    "hub sync: catalog mirror refreshed"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!("hub sync: catalog refresh failed: {e:#}"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_never_counts_or_scans() {
        for ts in [true, false] {
            let sql = refresh_sql(ts).to_lowercase();
            // (approximate_row_count is a catalog read, not a count)
            assert!(
                !sql.replace("approximate_row_count(", "").contains("count("),
                "{sql}"
            );
            assert!(
                sql.contains("where coalesce(m.home_hub, $2) = $2"),
                "never rewrites a peer's rows"
            );
            assert!(
                sql.contains("is distinct from"),
                "unchanged rows keep their version"
            );
        }
        assert!(HIDDEN_SCHEMAS.contains(&"jobs") && HIDDEN_SCHEMAS.contains(&"provenance"));
    }
}
