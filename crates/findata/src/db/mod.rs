//! Postgres connection pool (deadpool-postgres / tokio-postgres).
//!
//! All read queries and the write engine run through this single pool. Session
//! GUCs (`statement_timeout`, `application_name`) are set via the libpq
//! `options` connection parameter so no per-checkout hook is needed.

pub mod lineage;
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
    cfg.pool = Some(pool_cfg);

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}
