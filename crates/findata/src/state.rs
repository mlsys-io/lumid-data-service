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
    /// Shared HTTP client for the LLM reverse proxy (and any outbound calls).
    pub http: reqwest::Client,
}
