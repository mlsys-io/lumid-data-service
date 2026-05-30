//! Runtime configuration, read from env vars. Names mirror the Python
//! services (`FINDATA_*`) so deploy env carries over unchanged.

use std::env;

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
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
    pub ohlc_row_cap: i64,
    /// host:port the HTTP server binds to.
    pub bind_addr: String,

    // Auth.
    pub lumid_url: String,
    pub lumid_enabled: bool,
    pub lumid_cache_ttl_s: u64,
    pub lumid_timeout_s: u64,
    /// Raw `FINDATA_API_KEYS` value (`key:label,key:label`); parsed in auth.
    pub api_keys_raw: String,

    // Rate limit, "<n>/<unit>" e.g. "600/minute".
    pub rate_limit_anon: String,
    pub rate_limit_authed: String,

    // Redis (read-only: quote-snapshot last-tick). Empty disables.
    pub redis_url: String,
    /// Max symbols per /quotes request (mirrors rt_sse_request_syms).
    pub quotes_max_symbols: usize,

    // Blob plane (ingest). Empty `blob_root` disables local-FS blob storage.
    pub blob_root: String,
    pub blob_max_bytes: u64,
    /// Public base URL prefix for served blobs (empty → relative `/blobs/...`).
    pub blob_public_base_url: String,
    /// Root dir for served KOL media (empty disables `/kols/media/*` serving).
    pub kol_media_root: String,

    // Realtime hub (SSE/WS fan-out). Mirrors the FINDATA_RT_* knobs.
    pub rt_heartbeat_sec: u64,
    pub rt_ws_lifetime_syms: u64,
    pub rt_sse_request_syms: usize,
    pub rt_slowclient_queue: usize,
    /// Tier-A upstream slot caps + Tier-B/news/KOL poll cadences (R2 upstreams).
    pub rt_tier_a_finnhub_cap: usize,
    pub rt_tier_a_fmp_cap: usize,
    pub rt_tier_b_poll_sec: u64,
    pub rt_news_poll_sec: u64,
    pub rt_kol_poll_sec: u64,
    pub rt_kol_max_per_poll: usize,
    /// Enable the synthetic test publisher (dev only).
    pub rt_synthetic: bool,

    // LLM reverse proxy. Empty `llm_backend_url` disables the /v1/* routes (503).
    pub llm_backend_url: String,
    pub llm_default_model: String,
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
            ohlc_row_cap: env_u32("FINDATA_OHLC_ROW_CAP", 200_000) as i64,
            bind_addr: env_str("FINDATA_BIND_ADDR", "0.0.0.0:8088"),
            lumid_url: env_str("FINDATA_LUMID_URL", "https://lum.id"),
            lumid_enabled: matches!(
                env_str("FINDATA_LUMID_ENABLED", "true").to_lowercase().as_str(),
                "1" | "true" | "yes"
            ),
            lumid_cache_ttl_s: env_u32("FINDATA_LUMID_CACHE_TTL", 300) as u64,
            lumid_timeout_s: env_u32("FINDATA_LUMID_TIMEOUT_S", 5) as u64,
            api_keys_raw: env_str("FINDATA_API_KEYS", ""),
            rate_limit_anon: env_str("FINDATA_RATE_LIMIT_ANON", "60/minute"),
            rate_limit_authed: env_str("FINDATA_RATE_LIMIT_AUTHED", "600/minute"),
            redis_url: env_str("FINDATA_REDIS_URL", ""),
            quotes_max_symbols: env_u32("FINDATA_RT_SSE_REQUEST_SYMS", 100) as usize,
            blob_root: env_str("FINDATA_BLOB_ROOT", "/app/blobs"),
            blob_max_bytes: env_u64("FINDATA_BLOB_MAX_BYTES", 100 * 1024 * 1024),
            blob_public_base_url: env_str("FINDATA_BLOB_PUBLIC_BASE_URL", ""),
            kol_media_root: env_str("FINDATA_KOL_MEDIA_ROOT", ""),
            rt_heartbeat_sec: env_u32("FINDATA_RT_HEARTBEAT_SEC", 30) as u64,
            rt_ws_lifetime_syms: env_u32("FINDATA_RT_WS_LIFETIME_SYMS", 500) as u64,
            rt_sse_request_syms: env_u32("FINDATA_RT_SSE_REQUEST_SYMS", 100) as usize,
            rt_slowclient_queue: env_u32("FINDATA_RT_SLOWCLIENT_QUEUE", 100) as usize,
            rt_tier_a_finnhub_cap: env_u32("FINDATA_RT_TIER_A_FINNHUB_CAP", 60) as usize,
            rt_tier_a_fmp_cap: env_u32("FINDATA_RT_TIER_A_FMP_CAP", 60) as usize,
            rt_tier_b_poll_sec: env_u32("FINDATA_RT_TIER_B_POLL_SEC", 5) as u64,
            rt_news_poll_sec: env_u32("FINDATA_RT_NEWS_POLL_SEC", 60) as u64,
            rt_kol_poll_sec: env_u32("FINDATA_RT_KOL_POLL_SEC", 300) as u64,
            rt_kol_max_per_poll: env_u32("FINDATA_RT_KOL_MAX_PER_POLL", 20) as usize,
            rt_synthetic: matches!(
                env_str("FINDATA_RT_SYNTHETIC", "").to_lowercase().as_str(),
                "1" | "true" | "yes"
            ),
            llm_backend_url: env_str("FINDATA_LLM_BACKEND_URL", ""),
            llm_default_model: env_str("FINDATA_LLM_DEFAULT_MODEL", ""),
        }
    }
}
