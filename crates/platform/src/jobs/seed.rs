//! Boot-time seeding of what THIS hub owns: the estate sources it is home to
//! and the routes from each site to them.
//!
//! Config is the source of truth for both, and a hub only ever writes rows
//! whose `home_hub` is itself — a row another hub owns is never touched, even
//! if a source id collides. Routes this hub owns that are no longer in config
//! are tombstoned (`deleted_at`), which is how their removal reaches a peer.
//! Route health (`health`, `latency_ms`, `health_at`) is written by probers,
//! never by seeding, so a restart does not reset it.

use deadpool_postgres::Pool;
use serde::Deserialize;

use super::refs::ESTATE_INSTANCES;

const ROUTE_KINDS: &[&str] = &["lan", "tailnet", "proxy", "egress", "public"];
const CRED_MODES: &[&str] = &["job_token", "bridge_jwt", "service_pat"];

/// One route, as configured in `LUMID_JOBS_ROUTES_JSON` (a JSON array).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteSpec {
    pub route_id: String,
    pub from_site: String,
    pub source_id: String,
    pub kind: String,
    pub url: String,
    #[serde(default = "default_cred_mode")]
    pub cred_mode: String,
    #[serde(default)]
    pub max_bytes: Option<i64>,
    #[serde(default = "default_pref")]
    pub pref: i32,
}

fn default_cred_mode() -> String {
    "job_token".into()
}
fn default_pref() -> i32 {
    100
}

fn is_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Parse + validate the route table. Empty input = no routes.
pub fn parse_routes(raw: &str) -> Result<Vec<RouteSpec>, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(vec![]);
    }
    let routes: Vec<RouteSpec> =
        serde_json::from_str(raw).map_err(|e| format!("LUMID_JOBS_ROUTES_JSON: {e}"))?;
    let mut seen = std::collections::BTreeSet::new();
    for r in &routes {
        let ctx = format!("route '{}'", r.route_id);
        if !is_slug(&r.route_id) {
            return Err(format!("{ctx}: route_id must be [A-Za-z0-9_-]"));
        }
        if !seen.insert(r.route_id.clone()) {
            return Err(format!("{ctx}: duplicate route_id"));
        }
        if !is_slug(&r.from_site) || !is_slug(&r.source_id) {
            return Err(format!(
                "{ctx}: from_site and source_id must be [A-Za-z0-9_-]"
            ));
        }
        if !ROUTE_KINDS.contains(&r.kind.as_str()) {
            return Err(format!(
                "{ctx}: kind must be one of {}",
                ROUTE_KINDS.join(", ")
            ));
        }
        if !CRED_MODES.contains(&r.cred_mode.as_str()) {
            return Err(format!(
                "{ctx}: cred_mode must be one of {}",
                CRED_MODES.join(", ")
            ));
        }
        if !(r.url.starts_with("http://") || r.url.starts_with("https://"))
            || r.url.contains(char::is_whitespace)
        {
            return Err(format!("{ctx}: url must be http(s)://…"));
        }
        if r.max_bytes.is_some_and(|m| m <= 0) {
            return Err(format!("{ctx}: max_bytes must be positive when set"));
        }
    }
    Ok(routes)
}

/// Parse `LUMID_JOBS_ESTATE` (comma list of estate instance ids this hub is
/// home to). Unknown ids are refused rather than silently registered.
pub fn parse_estate(raw: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for id in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !ESTATE_INSTANCES.contains(&id) {
            return Err(format!(
                "LUMID_JOBS_ESTATE: unknown instance '{id}' (known: {})",
                ESTATE_INSTANCES.join(", ")
            ));
        }
        if !out.iter().any(|x: &String| x == id) {
            out.push(id.to_string());
        }
    }
    Ok(out)
}

/// What a seed pass did (logged at boot; returned for tests).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SeedReport {
    pub estate_upserted: u64,
    pub routes_upserted: u64,
    pub routes_tombstoned: u64,
}

/// Upsert this hub's estate sources + routes; tombstone its dropped routes.
pub async fn apply(
    pool: &Pool,
    hub_id: &str,
    hub_site: &str,
    estate: &[String],
    routes: &[RouteSpec],
) -> anyhow::Result<SeedReport> {
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let mut rep = SeedReport::default();

    for id in estate {
        // Only (re)claim a row this hub owns, or create it. A peer's row with
        // the same id is left alone (the WHERE makes the UPDATE a no-op).
        rep.estate_upserted += tx
            .execute(
                "INSERT INTO jobs.sources
                   (source_id, home_hub, owner_sub, name, kind, site, visibility)
                 VALUES ($1, $2, 'system', $1, 'estate', $3, 'public')
                 ON CONFLICT (source_id) DO UPDATE
                   SET site = EXCLUDED.site, updated_at = now(), deleted_at = NULL
                 WHERE jobs.sources.home_hub = EXCLUDED.home_hub
                   AND (jobs.sources.site IS DISTINCT FROM EXCLUDED.site
                        OR jobs.sources.deleted_at IS NOT NULL)",
                &[id, &hub_id, &hub_site],
            )
            .await?;
        tx.execute(
            "INSERT INTO jobs.source_locations (source_id, site, role, home_hub)
             SELECT $1, $2, 'primary', $3
              WHERE EXISTS (SELECT 1 FROM jobs.sources WHERE source_id = $1 AND home_hub = $3)
             ON CONFLICT (source_id, site) DO UPDATE
               SET deleted_at = NULL, updated_at = now()
             WHERE jobs.source_locations.home_hub = EXCLUDED.home_hub
               AND jobs.source_locations.deleted_at IS NOT NULL",
            &[id, &hub_site, &hub_id],
        )
        .await?;
    }

    for r in routes {
        rep.routes_upserted += tx
            .execute(
                "INSERT INTO jobs.site_routes
                   (route_id, from_site, source_id, kind, url, cred_mode, max_bytes, pref, home_hub)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (route_id) DO UPDATE
                   SET from_site = EXCLUDED.from_site, source_id = EXCLUDED.source_id,
                       kind = EXCLUDED.kind, url = EXCLUDED.url, cred_mode = EXCLUDED.cred_mode,
                       max_bytes = EXCLUDED.max_bytes, pref = EXCLUDED.pref,
                       updated_at = now(), deleted_at = NULL
                 WHERE jobs.site_routes.home_hub = EXCLUDED.home_hub
                   AND (jobs.site_routes.from_site, jobs.site_routes.source_id,
                        jobs.site_routes.kind, jobs.site_routes.url, jobs.site_routes.cred_mode,
                        jobs.site_routes.max_bytes, jobs.site_routes.pref,
                        jobs.site_routes.deleted_at IS NULL)
                       IS DISTINCT FROM
                       (EXCLUDED.from_site, EXCLUDED.source_id, EXCLUDED.kind, EXCLUDED.url,
                        EXCLUDED.cred_mode, EXCLUDED.max_bytes, EXCLUDED.pref, true)",
                &[
                    &r.route_id,
                    &r.from_site,
                    &r.source_id,
                    &r.kind,
                    &r.url,
                    &r.cred_mode,
                    &r.max_bytes,
                    &r.pref,
                    &hub_id,
                ],
            )
            .await?;
    }

    let keep: Vec<&str> = routes.iter().map(|r| r.route_id.as_str()).collect();
    rep.routes_tombstoned = tx
        .execute(
            "UPDATE jobs.site_routes SET deleted_at = now(), updated_at = now()
              WHERE home_hub = $1 AND deleted_at IS NULL AND NOT (route_id = ANY($2))",
            &[&hub_id, &keep],
        )
        .await?;

    tx.commit().await?;
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_parse_with_defaults() {
        let r = parse_routes(
            r#"[{"route_id":"home-lan-lumid-data","from_site":"home","source_id":"lumid-data",
                 "kind":"lan","url":"http://192.168.7.155:5101"}]"#,
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].cred_mode, "job_token");
        assert_eq!(r[0].pref, 100);
        assert_eq!(r[0].max_bytes, None);
        assert_eq!(parse_routes("  ").unwrap(), vec![]);
    }

    #[test]
    fn routes_are_validated() {
        let base = |extra: &str| {
            format!(
                r#"[{{"route_id":"r1","from_site":"home","source_id":"s","kind":"lan","url":"http://x"{extra}}}]"#
            )
        };
        assert!(parse_routes(&base(r#","kind":"carrier-pigeon""#)).is_err()); // duplicate key → serde error
        assert!(parse_routes(&base(r#","cred_mode":"MESH_PAT""#))
            .unwrap_err()
            .contains("cred_mode"));
        assert!(parse_routes(&base(r#","max_bytes":0"#))
            .unwrap_err()
            .contains("max_bytes"));
        assert!(parse_routes(&base(r#","surprise":1"#)).is_err()); // deny_unknown_fields
        assert!(parse_routes(
            r#"[{"route_id":"a b","from_site":"h","source_id":"s","kind":"lan","url":"http://x"}]"#
        )
        .unwrap_err()
        .contains("route_id"));
        assert!(parse_routes(
            r#"[{"route_id":"r","from_site":"h","source_id":"s","kind":"wifi","url":"http://x"}]"#
        )
        .unwrap_err()
        .contains("kind"));
        assert!(parse_routes(
            r#"[{"route_id":"r","from_site":"h","source_id":"s","kind":"lan","url":"ftp://x"}]"#
        )
        .unwrap_err()
        .contains("url"));
        let dup = r#"[{"route_id":"r","from_site":"h","source_id":"s","kind":"lan","url":"http://x"},
                      {"route_id":"r","from_site":"h","source_id":"s","kind":"lan","url":"http://y"}]"#;
        assert!(parse_routes(dup).unwrap_err().contains("duplicate"));
    }

    #[test]
    fn estate_list() {
        assert_eq!(
            parse_estate(" findata, lumid-data ,lqt,findata").unwrap(),
            vec!["findata", "lumid-data", "lqt"]
        );
        assert_eq!(parse_estate("").unwrap(), Vec::<String>::new());
        assert!(parse_estate("findata,warehouse")
            .unwrap_err()
            .contains("warehouse"));
    }
}
