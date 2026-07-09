//! Shared application state handed to every handler.

use std::collections::HashMap;
use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::auth::lumid::LumidClient;
use crate::auth::ratelimit::RateLimiter;
use crate::config::Settings;
use crate::retrieve::card_store::CardStore;

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub settings: Arc<Settings>,
    pub lumid: Arc<LumidClient>,
    pub local_keys: Arc<HashMap<String, String>>,
    pub rate: Arc<RateLimiter>,
    pub concurrency: Option<Arc<crate::auth::ratelimit::ConcurrencyLimiter>>,
    /// Read-only Redis (quote-snapshot last-tick). None when unconfigured.
    pub redis: Option<redis::aio::MultiplexedConnection>,
    /// Redis client handle for opening pub/sub connections (realtime streams).
    pub redis_client: Option<redis::Client>,
    /// Realtime fan-out hub. None when Redis is unconfigured.
    pub hub: Option<std::sync::Arc<crate::realtime::hub::Hub>>,
    /// Multi-tier response cache backing the config-driven read layer.
    pub read_cache: std::sync::Arc<crate::read::cache::CacheManager>,
    /// Shared HTTP client for non-streaming outbound calls (300 s total timeout).
    pub http: reqwest::Client,
    /// Long-lived HTTP client for SSE/streaming LLM responses. Connect timeout
    /// only — no total timeout, so multi-minute reasoning streams aren't cut off.
    pub http_stream: reqwest::Client,
    /// Health-aware, least-loaded LLM backend pool with circuit breaker.
    pub llm_pool: Arc<crate::llm_pool::BackendPool>,
    /// Pluggable blob object-store backend (local filesystem by default, or
    /// S3/MinIO when `LUMID_BLOB_BACKEND=s3`). Built once at boot.
    pub blob_store: Arc<dyn object_store::ObjectStore>,
    /// Storage-backend registry — resolves `schema.table → Backend`. Phase A is
    /// Postgres-only (default-to-PG when no `provenance.table_backend` row), so
    /// every existing table resolves to PG with no behavior change.
    pub backends: Arc<crate::backend::Registry>,
    /// App-provided `/status` feed-liveness policy (grouping + expected-live).
    /// Defaults to `realtime`-group, always-expected-live.
    pub feed_liveness: Arc<dyn crate::realtime::FeedLiveness>,
    /// In-process TTL cache for schema cards (built by the retrieval pipeline).
    pub card_store: Arc<CardStore>,
    /// Federation client (F1 mesh core): peer registry + forwarder for reads /
    /// LLM calls this instance doesn't serve locally. Empty peer set ⇒ pure
    /// local (no forwarding). Cheap to clone.
    pub federation: Arc<crate::federation::Federation>,
}
