//! Runtime configuration, read from env vars. Names mirror the Python
//! services (`FINDATA_*`) so deploy env carries over unchanged.

use std::env;

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub pool_max: usize,
    pub statement_timeout_ms: u32,
    /// host:port the HTTP server binds to.
    pub bind_addr: String,
}

impl Settings {
    pub fn from_env() -> Self {
        Settings {
            db_host: env_str("FINDATA_DB_HOST", "127.0.0.1"),
            db_port: env_u32("FINDATA_DB_PORT", 5433) as u16,
            db_user: env_str("FINDATA_DB_USER", "postgres"),
            db_password: env_str("FINDATA_DB_PASSWORD", ""),
            db_name: env_str("FINDATA_DB_NAME", "fin_ai_world_model_v2"),
            pool_max: env_u32("FINDATA_POOL_MAX", 20) as usize,
            statement_timeout_ms: env_u32("FINDATA_STATEMENT_TIMEOUT_MS", 30000),
            bind_addr: env_str("FINDATA_BIND_ADDR", "0.0.0.0:8088"),
        }
    }
}
