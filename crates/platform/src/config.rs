//! Runtime configuration, read from env vars.
//!
//! The crate is `lumid-platform`, so every platform setting is read as
//! `LUMID_<NAME>`. The platform names no app in its config namespace.
//! App-specific config (provider keys, etc.) is named by the app, not here
//! (e.g. `my_ext::cfg`).

use std::env;

/// Read a platform setting by its bare NAME (no prefix): `LUMID_<NAME>`.
pub fn env_var(name: &str) -> Option<String> {
    env::var(format!("LUMID_{name}")).ok()
}

fn env_str(name: &str, default: &str) -> String {
    env_var(name).unwrap_or_else(|| default.to_string())
}

fn env_u32(name: &str, default: u32) -> u32 {
    env_var(name).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env_var(name).and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// A federation peer: another lumid-data instance this one can forward
/// reads / LLM calls to. Part of the F1 mesh core (see the "federated multi-app
/// mesh" design). `token` is the bearer the peer accepts on its own gated
/// routes — a local key on the peer labelled e.g. `peer:<id>` / `sync:<id>`,
/// reusing the existing local-key auth path (no new scheme).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    /// Short peer id (the key in `LUMID_PEERS`), e.g. `primary`.
    pub id: String,
    /// Base URL of the peer's HTTP surface, no trailing slash (e.g.
    /// `http://findata-primary:8088`). The forwarder appends the identical path.
    pub base_url: String,
    /// Bearer token presented to the peer as `Authorization: Bearer <token>`.
    /// Sourced from `LUMID_PEER_<ID>_TOKEN` (uppercased id). May be empty when
    /// the peer needs no auth (dev / anonymous local key set).
    pub token: String,
}

/// Parse `LUMID_PEERS=id=base_url;id=base_url` into a peer registry, reading each
/// peer's bearer from `LUMID_PEER_<ID>_TOKEN` (id uppercased, non-alnum → `_`).
/// Malformed / empty entries are skipped. The `read_env` closure indirection
/// lets tests inject a fake env without touching the process environment.
fn parse_peers(raw: &str, read_env: &dyn Fn(&str) -> Option<String>) -> Vec<Peer> {
    raw.split(';')
        .filter_map(|e| e.trim().split_once('='))
        .filter_map(|(id, url)| {
            let id = id.trim();
            let url = url.trim().trim_end_matches('/');
            if id.is_empty() || url.is_empty() {
                return None;
            }
            let token_var = format!(
                "PEER_{}_TOKEN",
                id.to_uppercase()
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>()
            );
            let token = read_env(&token_var).unwrap_or_default();
            Some(Peer { id: id.to_string(), base_url: url.to_string(), token })
        })
        .collect()
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

    // ClickHouse backend (multi-backend Phase B). Empty `ch_url` disables the
    // CH backend entirely (Phase A behavior — every table resolves to Postgres,
    // a CH approve is rejected 503). When set, the backend registry registers a
    // ClickHouseBackend and tables can be approved onto CH.
    pub ch_url: String,
    pub ch_user: String,
    pub ch_password: String,
    pub ch_database: String,

    // Auth.
    pub lumid_url: String,
    pub lumid_enabled: bool,
    pub lumid_cache_ttl_s: u64,
    pub lumid_timeout_s: u64,
    /// Raw `LUMID_API_KEYS` value (`key:label,key:label`); parsed in auth.
    pub api_keys_raw: String,

    // Rate limit, "<n>/<unit>" e.g. "600/minute".
    pub rate_limit_anon: String,
    pub rate_limit_authed: String,
    /// Max simultaneous in-flight requests per API key. 0 = unlimited.
    pub max_concurrency_per_key: u32,
    /// Max simultaneous in-flight requests per client IP. 0 = unlimited.
    pub max_concurrency_per_ip: u32,

    // Redis (read-only: quote-snapshot last-tick). Empty disables.
    pub redis_url: String,
    /// Max symbols per /quotes request (mirrors rt_sse_request_syms).
    pub quotes_max_symbols: usize,

    // Blob plane (ingest). Empty `blob_root` disables local-FS blob storage.
    pub blob_root: String,
    pub blob_max_bytes: u64,
    /// Max request-body size for the ingest write routes (`/ingest/*`). axum's
    /// default is 2 MB — too small for batch NDJSON/file ingest — but unbounded
    /// buffering is an OOM/DoS risk, so cap it here. Default 256 MB.
    pub ingest_max_bytes: u64,
    /// Public base URL prefix for served blobs (empty → relative `/blobs/...`).
    pub blob_public_base_url: String,
    /// Object-storage backend selector: `"localfs"` (default) or `"s3"` (S3/MinIO).
    pub blob_backend: String,
    /// S3/MinIO endpoint (e.g. `http://minio:9000`). Used only when `blob_backend=="s3"`.
    pub blob_s3_endpoint: String,
    pub blob_s3_bucket: String,
    pub blob_s3_region: String,
    pub blob_s3_access_key: String,
    pub blob_s3_secret_key: String,

    // Realtime hub (SSE/WS fan-out). The LUMID_RT_* knobs.
    pub rt_heartbeat_sec: u64,
    pub rt_ws_lifetime_syms: u64,
    pub rt_sse_request_syms: usize,
    pub rt_slowclient_queue: usize,
    /// Generic Tier-B/poll cadence (provider-specific slot caps + keys + the
    /// per-provider poll cadences live in the app layer — see my_ext::cfg —
    /// so the platform names no provider).
    pub rt_tier_b_poll_sec: u64,
    pub rt_news_poll_sec: u64,
    /// Redis pub/sub channel KINDS the hub fans out (`<kind>:<key>` channels).
    /// The platform names no domain channel — apps declare their kinds via
    /// `LUMID_RT_CHANNEL_KINDS` (comma-separated). Default `tick,news`.
    pub rt_channel_kinds: Vec<String>,
    /// Enable the synthetic test publisher (dev only).
    pub rt_synthetic: bool,
    /// Symbols to keep subscribed independent of client demand, so their
    /// `last:tick` cache stays warm and `/quotes` returns live ticks even with
    /// no active stream. Comma-separated (`LUMID_RT_WARM_SYMBOLS`). Best for
    /// 24/7 venues (crypto/forex). Empty = demand-gated only (default).
    pub rt_warm_symbols: Vec<String>,

    // LLM reverse proxy. Empty `llm_backend_url` disables the /v1/* routes (503).
    // `llm_backend_url` is the PRIMARY/default backend; `llm_default_model` is the
    // model injected when a request omits one (→ primary).
    pub llm_backend_url: String,
    pub llm_default_model: String,
    /// Additional model→backend routes for the multi-backend `/v1/*` proxy.
    /// Format: `model=url;model=url` (one backend) or `model=url1,url2;…` (round-robin).
    /// A request whose `model` matches one of these is proxied to a backend chosen
    /// round-robin across the URL list; everything else goes to the primary.
    /// `/v1/models` aggregates all backends.
    pub llm_backends: Vec<(String, Vec<String>)>,
    /// Catch-all backend URL for any explicitly-specified model that isn't the
    /// primary and isn't in `llm_backends`. When set (e.g. `https://openrouter.ai/api`),
    /// unknown model IDs are forwarded there rather than rejected or sent to local.
    /// Empty (default) = fall through to primary.
    pub llm_openrouter_url: String,
    /// Optional bearer for upstream LLM calls made by the agent loop. When
    /// non-empty, the platform injects `Authorization: Bearer <key>` on the
    /// requests it originates from the agent loop. The `/v1/*` proxy does NOT
    /// inject this key — it forwards client requests verbatim. Required for
    /// hosted endpoints like `https://api.anthropic.com` reached by the agent.
    pub llm_api_key: String,
    /// Schemas the agent's tools surface. If unset, all non-system schemas.
    pub user_schemas: Vec<String>,

    // Retrieval pipeline knobs.
    /// TTL for in-process schema-card cache (seconds). Default 300.
    pub retrieval_card_ttl_s: u64,
    /// Per-session `statement_timeout` for SQL ops (milliseconds). Default 30000.
    pub retrieval_stmt_timeout_ms: u32,
    /// Maximum rows a single SQL op may return before the call is rejected. Default 1_000_000.
    pub retrieval_row_cap: u64,
    /// Object-storage key prefix for materialised retrieval outputs. Default `"retrievals"`.
    pub retrieval_prefix: String,
    /// Number of sample values to include per column in schema cards. Default 5.
    pub retrieval_sample_rows: usize,
    /// Optional Postgres role the retrieval SQL path de-escalates to via
    /// `SET LOCAL ROLE` inside its `READ ONLY` transaction. The platform shares
    /// one connection pool for reads, retrieval, ingest, and admin DDL — so the
    /// pool role itself often has write/DDL (and, in many deployments, is a
    /// superuser). Setting this to a NOSUPERUSER read-only role (e.g. one granted
    /// only `pg_read_all_data`, or scoped per-schema SELECT) confines just the
    /// retrieval/agent SELECTs: it removes the superuser file-read vector
    /// (`pg_read_file`) and makes `user_schemas` a real access boundary when the
    /// role's grants match it. Empty (default) = no `SET ROLE` (current behavior).
    pub retrieval_db_role: String,

    // Data-push / sync plane (generic). `enable_sync` (ServeParts) mounts the
    // inbox + admin routes; these knobs drive the optional push helper and the
    // inbox peer gate. Empty `sync_target_url` ⇒ the push helper is a no-op.
    /// Target instance base URL the push helper POSTs to (no trailing slash).
    pub sync_target_url: String,
    /// Bearer token presented to the target's `/sync/apply` (a local key on the
    /// target labelled `sync:<peer>`).
    pub sync_target_token: String,
    /// This instance's peer id, sent as `X-Lumid-Sync-Peer` by the push helper.
    pub sync_peer_id: String,
    /// Inbox allowlist: local-key labels (without the `local:` prefix) permitted
    /// to call `/sync/apply`. Empty ⇒ any label starting with `sync:` is allowed.
    pub sync_peer_labels: Vec<String>,
    /// Rows per push batch (push helper). Default 1000.
    pub sync_batch_rows: u64,
    /// Max delivery attempts before the push helper gives up a batch. Default 5.
    pub sync_max_attempts: u32,
    /// Base backoff (ms) for push retries; doubles per attempt. Default 500.
    pub sync_backoff_ms: u64,

    // ── Federation / mesh core (F1) ──────────────────────────────────────────
    // The peer-forward plane. When no peers + no `*_federate` are set, behavior
    // is IDENTICAL to a pure-local instance (every knob defaults to off).
    /// This instance's id in the mesh (`LUMID_INSTANCE_ID`). Default `"local"`.
    /// Stamped on forwarded requests (loop guard groundwork for F3) and useful
    /// for logs. F2/F3 tag peers `parent|child|peer` off this identity.
    pub instance_id: String,
    /// The app/tenant this instance serves (`LUMID_APP_ID`, default `"findata"`).
    /// Stamped as `X-Lumid-App` on forwarded requests so a downstream peer can
    /// later enforce per-app separation (F3). MVP: header contract only, no RBAC.
    pub app_id: String,
    /// Federation peers, from `LUMID_PEERS=id=base_url;…` + `LUMID_PEER_<ID>_TOKEN`.
    /// Empty ⇒ no forwarding is possible (pure local).
    pub peers: Vec<Peer>,
    /// Shadow default-route for reads: the peer id (must be in `peers`) that
    /// declarative reads this instance doesn't own are forwarded to. `None`
    /// (unset) ⇒ all reads served locally (unchanged). `LUMID_READ_FEDERATE`.
    pub read_federate: Option<String>,
    /// LLM default-route: the peer id the `/v1/*` proxy targets instead of the
    /// local `llm_backends`. `None` ⇒ local LLM backends (unchanged).
    /// `LUMID_LLM_FEDERATE`.
    pub llm_federate: Option<String>,
    /// TTL (seconds) for the shadow catch-all forward cache — the moka cache
    /// backing `federation::shadow_forward`. Only consulted in shadow mode
    /// (`read_federate` set). `LUMID_SHADOW_CACHE_TTL_S`, default 30.
    pub shadow_cache_ttl_s: u64,
}

impl Settings {
    pub fn from_env() -> Self {
        Settings {
            db_host: env_str("DB_HOST", "127.0.0.1"),
            db_port: env_u32("DB_PORT", 5433) as u16,
            db_user: env_str("DB_USER", "postgres"),
            db_password: env_str("DB_PASSWORD", ""),
            db_name: env_str("DB_NAME", "postgres"),
            pool_max: env_u32("POOL_MAX", 20) as usize,
            statement_timeout_ms: env_u32("STATEMENT_TIMEOUT_MS", 30000),
            ohlc_row_cap: env_u32("OHLC_ROW_CAP", 200_000) as i64,
            bind_addr: env_str("BIND_ADDR", "0.0.0.0:8088"),
            ch_url: env_str("CLICKHOUSE_URL", ""),
            ch_user: env_str("CLICKHOUSE_USER", "default"),
            ch_password: env_str("CLICKHOUSE_PASSWORD", ""),
            ch_database: env_str("CLICKHOUSE_DB", "default"),
            // Auth-service (Lumid identity) config — named AUTH_* so the env
            // var isn't a doubled prefix under the platform's LUMID_ namespace.
            lumid_url: env_str("AUTH_URL", "https://lum.id"),
            lumid_enabled: matches!(
                env_str("AUTH_ENABLED", "true").to_lowercase().as_str(),
                "1" | "true" | "yes"
            ),
            lumid_cache_ttl_s: env_u32("AUTH_CACHE_TTL", 300) as u64,
            lumid_timeout_s: env_u32("AUTH_TIMEOUT_S", 5) as u64,
            api_keys_raw: env_str("API_KEYS", ""),
            rate_limit_anon: env_str("RATE_LIMIT_ANON", "60/minute"),
            rate_limit_authed: env_str("RATE_LIMIT_AUTHED", "600/minute"),
            max_concurrency_per_key: env_u32("MAX_CONCURRENCY_PER_KEY", 0),
            max_concurrency_per_ip: env_u32("MAX_CONCURRENCY_PER_IP", 0),
            redis_url: env_str("REDIS_URL", ""),
            quotes_max_symbols: env_u32("RT_SSE_REQUEST_SYMS", 100) as usize,
            blob_root: env_str("BLOB_ROOT", "/app/blobs"),
            blob_max_bytes: env_u64("BLOB_MAX_BYTES", 100 * 1024 * 1024),
            ingest_max_bytes: env_u64("INGEST_MAX_BYTES", 256 * 1024 * 1024),
            blob_public_base_url: env_str("BLOB_PUBLIC_BASE_URL", ""),
            blob_backend: env_str("BLOB_BACKEND", "localfs"),
            blob_s3_endpoint: env_str("BLOB_S3_ENDPOINT", ""),
            blob_s3_bucket: env_str("BLOB_S3_BUCKET", ""),
            blob_s3_region: env_str("BLOB_S3_REGION", "us-east-1"),
            blob_s3_access_key: env_str("BLOB_S3_ACCESS_KEY", ""),
            blob_s3_secret_key: env_str("BLOB_S3_SECRET_KEY", ""),
            rt_heartbeat_sec: env_u32("RT_HEARTBEAT_SEC", 30) as u64,
            rt_ws_lifetime_syms: env_u32("RT_WS_LIFETIME_SYMS", 500) as u64,
            rt_sse_request_syms: env_u32("RT_SSE_REQUEST_SYMS", 100) as usize,
            rt_slowclient_queue: env_u32("RT_SLOWCLIENT_QUEUE", 100) as usize,
            rt_tier_b_poll_sec: env_u32("RT_TIER_B_POLL_SEC", 5) as u64,
            rt_news_poll_sec: env_u32("RT_NEWS_POLL_SEC", 60) as u64,
            rt_channel_kinds: env_str("RT_CHANNEL_KINDS", "tick,news")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            rt_synthetic: matches!(
                env_str("RT_SYNTHETIC", "").to_lowercase().as_str(),
                "1" | "true" | "yes"
            ),
            rt_warm_symbols: env_str("RT_WARM_SYMBOLS", "")
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
            llm_backend_url: env_str("LLM_BACKEND_URL", ""),
            llm_default_model: env_str("LLM_DEFAULT_MODEL", ""),
            llm_backends: env_str("LLM_BACKENDS", "")
                .split(';')
                .filter_map(|e| e.trim().split_once('='))
                .map(|(m, urls_str)| {
                    let urls: Vec<String> = urls_str
                        .split(',')
                        .map(|u| u.trim().trim_end_matches('/').to_string())
                        .filter(|u| !u.is_empty())
                        .collect();
                    (m.trim().to_string(), urls)
                })
                .filter(|(m, urls)| !m.is_empty() && !urls.is_empty())
                .collect(),
            llm_openrouter_url: env_str("LLM_OPENROUTER_URL", "")
                .trim_end_matches('/')
                .to_string(),
            llm_api_key: env_str("LLM_API_KEY", ""),
            user_schemas: env_str("USER_SCHEMAS", "")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            retrieval_card_ttl_s: env_u64("RETRIEVAL_CARD_TTL_S", 300),
            retrieval_stmt_timeout_ms: env_u32("RETRIEVAL_STMT_TIMEOUT_MS", 30_000),
            retrieval_row_cap: env_u64("RETRIEVAL_ROW_CAP", 1_000_000),
            retrieval_prefix: env_str("RETRIEVAL_PREFIX", "retrievals"),
            retrieval_sample_rows: env_u32("RETRIEVAL_SAMPLE_ROWS", 5) as usize,
            retrieval_db_role: env_str("RETRIEVAL_DB_ROLE", ""),
            sync_target_url: env_str("SYNC_TARGET_URL", "").trim_end_matches('/').to_string(),
            sync_target_token: env_str("SYNC_TARGET_TOKEN", ""),
            sync_peer_id: env_str("SYNC_PEER_ID", ""),
            sync_peer_labels: env_str("SYNC_PEER_LABELS", "")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            sync_batch_rows: env_u64("SYNC_BATCH_ROWS", 1000),
            sync_max_attempts: env_u32("SYNC_MAX_ATTEMPTS", 5),
            sync_backoff_ms: env_u64("SYNC_BACKOFF_MS", 500),
            instance_id: env_str("INSTANCE_ID", "local"),
            app_id: env_str("APP_ID", "findata"),
            peers: parse_peers(&env_str("PEERS", ""), &|name| env_var(name)),
            read_federate: env_var("READ_FEDERATE")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            llm_federate: env_var("LLM_FEDERATE")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            shadow_cache_ttl_s: env_u64("SHADOW_CACHE_TTL_S", 30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peers_basic() {
        let env = |name: &str| match name {
            "PEER_PRIMARY_TOKEN" => Some("tok-primary".to_string()),
            "PEER_HUB_TOKEN" => Some("tok-hub".to_string()),
            _ => None,
        };
        let peers = parse_peers(
            "primary=http://findata-primary:8088;hub=https://hub.example/",
            &env,
        );
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].id, "primary");
        // Trailing slash stripped.
        assert_eq!(peers[0].base_url, "http://findata-primary:8088");
        assert_eq!(peers[0].token, "tok-primary");
        assert_eq!(peers[1].id, "hub");
        assert_eq!(peers[1].base_url, "https://hub.example");
        assert_eq!(peers[1].token, "tok-hub");
    }

    #[test]
    fn parse_peers_missing_token_is_empty_not_dropped() {
        let peers = parse_peers("primary=http://p:8088", &|_| None);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].token, "");
    }

    #[test]
    fn parse_peers_non_alnum_id_uppercased_for_token_var() {
        // id `us-east` → token var PEER_US_EAST_TOKEN.
        let env = |name: &str| {
            if name == "PEER_US_EAST_TOKEN" {
                Some("t".to_string())
            } else {
                None
            }
        };
        let peers = parse_peers("us-east=http://p:8088", &env);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "us-east");
        assert_eq!(peers[0].token, "t");
    }

    #[test]
    fn parse_peers_skips_malformed_and_empty() {
        let peers = parse_peers("  ;=http://x;id=;good=http://g:1;garbage", &|_| None);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "good");
        assert_eq!(peers[0].base_url, "http://g:1");
    }

    #[test]
    fn parse_peers_empty_input() {
        assert!(parse_peers("", &|_| None).is_empty());
    }
}
