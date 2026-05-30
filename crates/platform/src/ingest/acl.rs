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

/// A role can propose a net-new shape if it has ANY can_write=true row.
pub async fn can_propose(pool: &Pool, role: &str) -> Result<bool, ApiError> {
    let cache = cache_snapshot(pool).await?;
    Ok(cache
        .iter()
        .any(|((r, _, _), allow)| r == role && *allow))
}
