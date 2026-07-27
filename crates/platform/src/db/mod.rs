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
    let mut options = format!(
        "-c statement_timeout={} -c application_name=lumid-data-service",
        s.statement_timeout_ms
    );
    // Single-tenant pin (field boxes): start every connection with the tenant
    // GUC set so a NOSUPERUSER/NOBYPASSRLS role still sees this tenant's rows via
    // RLS (`current_setting('app.tenant_id', true)`). `app.tenant_id` is a
    // placeholder GUC — settable at startup with no extension. The value is a
    // UUID (hex + hyphens), so it needs no escaping in the options string.
    if !s.db_tenant_id.is_empty() {
        options.push_str(&format!(" -c app.tenant_id={}", s.db_tenant_id));
    }
    cfg.options = Some(options);
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
