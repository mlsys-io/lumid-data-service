//! Planner inputs from the registry and catalog — cheap reads only.
//!
//! Size estimates come from catalog statistics (`pg_total_relation_size`,
//! recorded blob sizes, the synced `catalog_mirror`), never from scanning
//! data: a `count(*)` or object listing to size a plan would cost more than
//! the job it plans.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::Deserialize;

use super::planner::{Route, SiteCapacity, Size};
use super::refs::SourceRef;

/// Candidate sites, as configured in `LUMID_JOBS_SITES_JSON`:
/// `[{"site":"home","runtimes":["flowmesh","lumilake","sandbox"],"ext_egress":true,
///    "cached_models":["qwen/qwen3-8b"]}]`. (Live worker inventory from
/// mesh-federator replaces this when the runtime adapters land.)
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SiteSpec {
    site: String,
    runtimes: Vec<String>,
    #[serde(default = "yes")]
    ext_egress: bool,
    #[serde(default)]
    cached_models: Vec<String>,
}
fn yes() -> bool {
    true
}

const RUNTIMES: &[&str] = &["flowmesh", "lumilake", "sandbox"];

pub fn parse_sites(raw: &str) -> Result<Vec<SiteCapacity>, String> {
    if raw.trim().is_empty() {
        return Ok(vec![]);
    }
    let specs: Vec<SiteSpec> =
        serde_json::from_str(raw).map_err(|e| format!("LUMID_JOBS_SITES_JSON: {e}"))?;
    let mut out: Vec<SiteCapacity> = Vec::new();
    for s in specs {
        if s.site.is_empty() || out.iter().any(|o| o.site == s.site) {
            return Err(format!(
                "LUMID_JOBS_SITES_JSON: empty or duplicate site '{}'",
                s.site
            ));
        }
        if let Some(bad) = s.runtimes.iter().find(|r| !RUNTIMES.contains(&r.as_str())) {
            return Err(format!(
                "LUMID_JOBS_SITES_JSON: unknown runtime '{bad}' at {}",
                s.site
            ));
        }
        out.push(SiteCapacity {
            site: s.site,
            runtimes: s.runtimes,
            ext_egress: s.ext_egress,
            cached_models: s
                .cached_models
                .into_iter()
                .map(|m| m.to_ascii_lowercase())
                .collect(),
        });
    }
    Ok(out)
}

/// Every live route.
pub async fn load_routes(pool: &Pool) -> anyhow::Result<Vec<Route>> {
    let c = pool.get().await?;
    let rows = c
        .query(
            "SELECT route_id, from_site, source_id, kind, url, max_bytes, pref, health,
                    latency_ms, health_at
               FROM jobs.site_routes WHERE deleted_at IS NULL",
            &[],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| Route {
            route_id: r.get("route_id"),
            from_site: r.get("from_site"),
            source_id: r.get("source_id"),
            kind: r.get("kind"),
            url: r.get("url"),
            max_bytes: r.get("max_bytes"),
            pref: r.get("pref"),
            health: r.get("health"),
            latency_ms: r.get("latency_ms"),
            health_at: r.get::<_, Option<DateTime<Utc>>>("health_at"),
        })
        .collect())
}

/// Where a source's data lives: its primary location, else the source row's
/// own `site`. `None` when the source is unknown or deleted.
pub async fn source_site(pool: &Pool, source_id: &str) -> anyhow::Result<Option<String>> {
    let c = pool.get().await?;
    let row = c
        .query_opt(
            "SELECT COALESCE(
                      (SELECT l.site FROM jobs.source_locations l
                        WHERE l.source_id = s.source_id AND l.role = 'primary'
                          AND l.deleted_at IS NULL
                        ORDER BY l.site LIMIT 1),
                      s.site) AS site
               FROM jobs.sources s
              WHERE s.source_id = $1 AND s.deleted_at IS NULL",
            &[&source_id],
        )
        .await?;
    Ok(row.and_then(|r| r.get::<_, Option<String>>("site")))
}

/// Estimate how much reading `r` could move.
///
/// * `declared` (the submitter's `max_bytes`) always wins.
/// * Tables / schemas on THIS process's instance: on-disk size from the
///   catalog — an upper bound, since reads filter.
/// * Tables on another instance: the synced `catalog_mirror` estimate (that
///   table's `hub_id` column holds the catalog's INSTANCE id, e.g. `findata`).
/// * A blob by sha: its recorded size (exact).
/// * Prefixes, models, external APIs: unknown here.
pub async fn estimate(
    pool: &Pool,
    local_instance: &str,
    r: &SourceRef,
    declared: Option<i64>,
) -> anyhow::Result<Size> {
    if let Some(n) = declared {
        return Ok(Size::Declared(n.max(0)));
    }
    let c = pool.get().await?;
    match r {
        SourceRef::Table {
            instance,
            schema,
            table,
        } if instance == local_instance => {
            let n: Option<i64> = match table {
                Some(t) => c
                    .query_one(
                        "SELECT pg_total_relation_size(to_regclass(format('%I.%I', $1::text, $2::text)))",
                        &[schema, t],
                    )
                    .await?
                    .get(0),
                None => c
                    .query_one(
                        "SELECT sum(pg_total_relation_size(c.oid))::bigint
                           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = $1 AND c.relkind IN ('r','p','m')",
                        &[schema],
                    )
                    .await?
                    .get(0),
            };
            Ok(n.map(Size::UpperBound).unwrap_or(Size::Unknown))
        }
        SourceRef::Table {
            instance,
            schema,
            table,
        } => {
            // Another instance: whatever its hub last synced.
            let n: Option<i64> = c
                .query_one(
                    "SELECT sum(size_bytes_est)::bigint FROM jobs.catalog_mirror
                      WHERE hub_id = $1 AND schema_name = $2
                        AND ($3::text IS NULL OR table_name = $3)
                        AND deleted_at IS NULL",
                    &[instance, schema, table],
                )
                .await?
                .get(0);
            Ok(n.map(Size::UpperBound).unwrap_or(Size::Unknown))
        }
        SourceRef::Blob { sha256, .. } => {
            let n: Option<i64> = c
                .query_one(
                    "SELECT COALESCE(
                       (SELECT max(size_bytes) FROM raw.tenant_blobs WHERE blob_sha256 = $1),
                       (SELECT size_bytes FROM raw.blobs WHERE blob_sha256 = $1))",
                    &[sha256],
                )
                .await
                .map(|r| r.get(0))
                .unwrap_or(None);
            Ok(n.map(Size::Exact).unwrap_or(Size::Unknown))
        }
        _ => Ok(Size::Unknown),
    }
}

/// Record a prober's health reading for a route THIS hub owns. Rows another
/// hub owns are never written (their prober reports to their hub).
pub async fn record_route_health(
    pool: &Pool,
    hub_id: &str,
    route_id: &str,
    health: &str,
    latency_ms: Option<i32>,
) -> anyhow::Result<bool> {
    if !matches!(health, "up" | "degraded" | "down") {
        anyhow::bail!("health must be up, degraded, or down");
    }
    let c = pool.get().await?;
    let n = c
        .execute(
            "UPDATE jobs.site_routes
                SET health = $3, latency_ms = $4, health_at = now()
              WHERE route_id = $2 AND home_hub = $1 AND deleted_at IS NULL",
            &[&hub_id, &route_id, &health, &latency_ms],
        )
        .await?;
    Ok(n == 1)
}

/// Routes this hub owns that start at `from_site` (all sites when `None`) —
/// what a site's prober walks. Live rows only.
pub async fn list_routes(
    pool: &Pool,
    hub_id: &str,
    from_site: Option<&str>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let c = pool.get().await?;
    let rows = c
        .query(
            "SELECT route_id, from_site, source_id, kind, url, cred_mode, max_bytes, pref,
                    health, latency_ms, health_at
               FROM jobs.site_routes
              WHERE home_hub = $1 AND deleted_at IS NULL
                AND ($2::text IS NULL OR from_site = $2)
              ORDER BY from_site, source_id, pref, route_id",
            &[&hub_id, &from_site],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let at: Option<chrono::DateTime<chrono::Utc>> = r.get("health_at");
            serde_json::json!({
                "route_id": r.get::<_, String>("route_id"),
                "from_site": r.get::<_, String>("from_site"),
                "source_id": r.get::<_, String>("source_id"),
                "kind": r.get::<_, String>("kind"),
                "url": r.get::<_, String>("url"),
                "cred_mode": r.get::<_, String>("cred_mode"),
                "max_bytes": r.get::<_, Option<i64>>("max_bytes"),
                "pref": r.get::<_, i32>("pref"),
                "health": r.get::<_, String>("health"),
                "latency_ms": r.get::<_, Option<i32>>("latency_ms"),
                "health_at": at,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sites_parse_and_validate() {
        let s = parse_sites(
            r#"[{"site":"home","runtimes":["flowmesh","lumilake"],"cached_models":["Qwen/Qwen3-8B"]},
                {"site":"chatbox-home","runtimes":["sandbox"],"ext_egress":false}]"#,
        )
        .unwrap();
        assert_eq!(s.len(), 2);
        assert!(s[0].ext_egress);
        assert_eq!(s[0].cached_models, vec!["qwen/qwen3-8b"]);
        assert!(!s[1].ext_egress);
        assert!(parse_sites("").unwrap().is_empty());
        assert!(parse_sites(r#"[{"site":"x","runtimes":["k8s"]}]"#)
            .unwrap_err()
            .contains("k8s"));
        assert!(
            parse_sites(r#"[{"site":"x","runtimes":[]},{"site":"x","runtimes":[]}]"#)
                .unwrap_err()
                .contains("duplicate")
        );
        assert!(parse_sites(r#"[{"site":"x","runtimes":[],"gpu":true}]"#).is_err());
    }
}
