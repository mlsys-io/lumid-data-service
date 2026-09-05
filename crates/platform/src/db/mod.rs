//! Postgres connection pool (deadpool-postgres / tokio-postgres).
//!
//! All read queries and the write engine run through this single pool. Session
//! GUCs (`statement_timeout`, `application_name`) are set via the libpq
//! `options` connection parameter so no per-checkout hook is needed.

pub mod lineage;
pub mod qb;
pub mod rows;

use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::config::Settings;

pub fn build_pool(s: &Settings) -> anyhow::Result<Pool> {
    let mut cfg = Config::new();
    cfg.host = Some(s.db_host.clone());
    cfg.port = Some(s.db_port);
    cfg.user = Some(s.db_user.clone());
    cfg.password = Some(s.db_password.clone());
    cfg.dbname = Some(s.db_name.clone());
    // Session setup applied to every backend connection.
    cfg.options = Some(format!(
        "-c statement_timeout={} -c application_name=lumid-data-service",
        s.statement_timeout_ms
    ));
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let mut pool_cfg = deadpool_postgres::PoolConfig::default();
    pool_cfg.max_size = s.pool_max;
    // Bound pool-acquire so exhaustion returns 503 instead of hanging forever.
    pool_cfg.timeouts.wait = Some(std::time::Duration::from_secs(5));
    pool_cfg.timeouts.create = Some(std::time::Duration::from_secs(10));
    cfg.pool = Some(pool_cfg);

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}

/// Optional secondary pool for the `xpio.*` / `mailbox.*` schema.
///
/// On some deployments that schema lives in a DIFFERENT Postgres instance than
/// the app's main warehouse DB. findata is the case this exists for: it serves
/// the market warehouse (`fin_ai_world_model_v2`) from the main pool, but the
/// LQT mailbox + `xpio.*` tables are OWNED by the LQT data DB — so every
/// `/xpio/*` route ran against a DB with no `xpio` schema and returned 500.
///
/// Activated only when `LUMID_XPIO_DB_HOST` is set; otherwise `None`, and
/// callers fall back to the main pool via `AppState::xpio()` — byte-equivalent
/// for every app that keeps `xpio.*` in its own DB (e.g. the LQT data service).
/// Unset fields inherit from the main `Settings`, so a co-located override can
/// set only `HOST`/`NAME` and reuse the shared user/password/port.
pub fn build_xpio_pool(s: &Settings) -> anyhow::Result<Option<Pool>> {
    let host = match std::env::var("LUMID_XPIO_DB_HOST")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(h) => h,
        None => return Ok(None),
    };
    let getf = |k: &str, dflt: &str| {
        std::env::var(k)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| dflt.to_string())
    };
    let mut cfg = Config::new();
    cfg.host = Some(host);
    cfg.port = Some(
        std::env::var("LUMID_XPIO_DB_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(s.db_port),
    );
    cfg.user = Some(getf("LUMID_XPIO_DB_USER", &s.db_user));
    cfg.password = Some(getf("LUMID_XPIO_DB_PASSWORD", &s.db_password));
    cfg.dbname = Some(getf("LUMID_XPIO_DB_NAME", &s.db_name));
    cfg.options = Some(format!(
        "-c statement_timeout={} -c application_name=lumid-data-service-xpio",
        s.statement_timeout_ms
    ));
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let mut pool_cfg = deadpool_postgres::PoolConfig::default();
    pool_cfg.max_size = s.pool_max;
    pool_cfg.timeouts.wait = Some(std::time::Duration::from_secs(5));
    pool_cfg.timeouts.create = Some(std::time::Duration::from_secs(10));
    cfg.pool = Some(pool_cfg);

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(Some(pool))
}
