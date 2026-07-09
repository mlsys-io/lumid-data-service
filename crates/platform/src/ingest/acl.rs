//! Role-based ingress ACL — port of `ingest/acl.py`.
//!
//! Rows in `provenance.ingress_acl` (role, target_schema, target_table,
//! can_write) with '*' wildcards. Lookup priority:
//!   1. (role, schema, table)
//!   2. (role, schema, '*')
//!   3. (role, '*', '*')
//! First can_write=true wins; no match = deny. Cached 30s in-process.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use once_cell::sync::Lazy;

use crate::error::ApiError;

const CACHE_TTL: Duration = Duration::from_secs(30);

struct AclCache {
    map: HashMap<(String, String, String), bool>,
    loaded_at: Option<Instant>,
}

static CACHE: Lazy<Mutex<AclCache>> = Lazy::new(|| {
    Mutex::new(AclCache {
        map: HashMap::new(),
        loaded_at: None,
    })
});

/// Force the next check to re-read from Postgres.
pub fn invalidate() {
    let mut c = CACHE.lock().unwrap();
    c.loaded_at = None;
}

async fn load(pool: &Pool) -> Result<HashMap<(String, String, String), bool>, ApiError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT role, target_schema, target_table, can_write FROM provenance.ingress_acl",
            &[],
        )
        .await?;
    let mut out = HashMap::new();
    for r in &rows {
        let role: String = r.get("role");
        let sch: String = r.get("target_schema");
        let tbl: String = r.get("target_table");
        let can: bool = r.get("can_write");
        out.insert((role, sch, tbl), can);
    }
    Ok(out)
}

/// Get a fresh-enough cache clone (cheap: <100 rows).
async fn cache_snapshot(
    pool: &Pool,
) -> Result<HashMap<(String, String, String), bool>, ApiError> {
    {
        let c = CACHE.lock().unwrap();
        if let Some(at) = c.loaded_at {
            if at.elapsed() < CACHE_TTL && !c.map.is_empty() {
                return Ok(c.map.clone());
            }
        }
    }
    let fresh = load(pool).await?;
    let mut c = CACHE.lock().unwrap();
    c.map = fresh.clone();
    c.loaded_at = Some(Instant::now());
    Ok(fresh)
}

/// Raise Forbidden if `role` cannot write (schema, table).
pub async fn check_can_write(
    pool: &Pool,
    role: &str,
    schema: &str,
    table: &str,
) -> Result<(), ApiError> {
    let cache = cache_snapshot(pool).await?;
    let candidates = [
        (role.to_string(), schema.to_string(), table.to_string()),
        (role.to_string(), schema.to_string(), "*".to_string()),
        (role.to_string(), "*".to_string(), "*".to_string()),
    ];
    for key in candidates {
        if cache.get(&key) == Some(&true) {
            return Ok(());
        }
    }
    Err(ApiError::Forbidden(format!(
        "role {role:?} not authorized to write {schema}.{table}"
    )))
}

/// Narrow, scope-based capability grants — orthogonal to the coarse role ACL.
///
/// Some callers hold an opaque LQT-style *capability* scope on their PAT (e.g.
/// `lqt:universe:refresh`) rather than a platform role/level. Such a scope
/// authorizes writing to ONE specific (schema, table) and nothing else — this
/// is strictly narrower than an `ingress_acl` role grant (which would open a
/// whole schema/table to EVERY PAT of that role). The mapping is a hard-coded
/// allowlist here (not DB-driven) precisely so it stays least-privilege and
/// can't be widened by an accidental `*` ACL row.
///
/// Returns true iff any of the caller's `scopes` grants write to (schema,
/// table). Keep each entry a single concrete (scope, schema, table) triple.
pub fn scope_grants_write(scopes: &[String], schema: &str, table: &str) -> bool {
    // (capability scope, target schema, target table) — one row per capability.
    const CAP_GRANTS: &[(&str, &str, &str)] = &[
        // LQT monitored-universe refresh: the scoped scheduler cred publishes a
        // `universe.refresh` config message into the mailbox inbox. Blessed as a
        // grantable capability by lumid-identity (canGrant allowlist). Narrow:
        // this scope → mailbox.lqt_inbox ONLY. The mailbox-consumer re-checks the
        // scope (lqt-auth `universe.refresh` topic grant) as the real authority.
        ("lqt:universe:refresh", "mailbox", "lqt_inbox"),
    ];
    for (cap, sch, tbl) in CAP_GRANTS {
        if *sch == schema && *tbl == table && scopes.iter().any(|s| s == cap) {
            return true;
        }
    }
    false
}

/// A role can propose a net-new shape if it has ANY can_write=true row.
pub async fn can_propose(pool: &Pool, role: &str) -> Result<bool, ApiError> {
    let cache = cache_snapshot(pool).await?;
    Ok(cache
        .iter()
        .any(|((r, _, _), allow)| r == role && *allow))
}

#[cfg(test)]
mod tests {
    use super::scope_grants_write;

    fn scopes(ss: &[&str]) -> Vec<String> {
        ss.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn universe_refresh_scope_grants_only_mailbox_inbox() {
        let s = scopes(&["lqt:universe:refresh"]);
        // The one blessed (scope, schema, table) triple.
        assert!(scope_grants_write(&s, "mailbox", "lqt_inbox"));
        // Same scope must NOT reach any other table/schema (least privilege).
        assert!(!scope_grants_write(&s, "mailbox", "lqt_outbox"));
        assert!(!scope_grants_write(&s, "mailbox", "processed"));
        assert!(!scope_grants_write(&s, "obs", "runtime_cycles"));
        assert!(!scope_grants_write(&s, "core", "tenant_strategies"));
    }

    #[test]
    fn unrelated_or_empty_scopes_grant_nothing() {
        assert!(!scope_grants_write(&scopes(&[]), "mailbox", "lqt_inbox"));
        assert!(!scope_grants_write(&scopes(&["lumid:read"]), "mailbox", "lqt_inbox"));
        assert!(!scope_grants_write(&scopes(&["lqt:universe"]), "mailbox", "lqt_inbox"));
        assert!(!scope_grants_write(&scopes(&["*"]), "mailbox", "lqt_inbox"));
    }
}
