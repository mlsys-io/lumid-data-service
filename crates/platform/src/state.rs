//! Shared application state handed to every handler.

use std::collections::HashMap;
use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::auth::lumid::LumidClient;
use crate::auth::ratelimit::RateLimiter;
use crate::config::Settings;

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub settings: Arc<Settings>,
    pub lumid: Arc<LumidClient>,
    pub local_keys: Arc<HashMap<String, String>>,
    pub rate: Arc<RateLimiter>,
    /// Read-only Redis (quote-snapshot last-tick). None when unconfigured.
    pub redis: Option<redis::aio::MultiplexedConnection>,
    /// Redis client handle for opening pub/sub connections (realtime streams).
    pub redis_client: Option<redis::Client>,
    /// Realtime fan-out hub. None when Redis is unconfigured.
    pub hub: Option<std::sync::Arc<crate::realtime::hub::Hub>>,
    /// Multi-tier response cache backing the config-driven read layer.
    pub read_cache: std::sync::Arc<crate::read::cache::CacheManager>,
    /// Shared HTTP client for the LLM reverse proxy (and any outbound calls).
    pub http: reqwest::Client,
    /// Storage-backend registry — resolves `schema.table → Backend`. Phase A is
    /// Postgres-only (default-to-PG when no `provenance.table_backend` row), so
    /// every existing table resolves to PG with no behavior change.
    pub backends: Arc<crate::backend::Registry>,
}
