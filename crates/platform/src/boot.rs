//! One-call boot for an application built on the platform. `serve()` does all
//! the wiring (settings, pool, auth, redis, hub+workers, read specs, cache +
//! invalidation, auto-MCP from specs, router, listener) so an app's `main` is:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     lumid_platform::serve(lumid_platform::ServeParts {
//!         ext_routes: findata_ext::routes(),   // or axum::Router::new()
//!         workers: findata_ext::workers(),     // or vec![]
//!     }).await
//! }
//! ```

use std::sync::Arc;

use axum::Router;
use tracing_subscriber::EnvFilter;

use crate::realtime::upstream::UpstreamWorker;
use crate::{app, auth, config, db, mcp, read, realtime, state};

/// The only app-specific inputs: compiled ext routes + realtime workers.
/// Everything else (config, MCP tools, the generic routes) is derived.
pub struct ServeParts {
    pub ext_routes: Router<state::AppState>,
    /// Public (un-gated) routes contributed by the app — merged into the
    /// platform's public group alongside the landing surfaces. Use for
    /// app-owned docs / static pages that should need no auth. Default empty.
    pub public_routes: Router<state::AppState>,
    /// Public landing routes (`/`, and whatever else the app wants there) —
    /// override the platform's generic fallback landing. Default is the
    /// platform's domain-free `GET /` page (so the platform names no domain and
    /// a config-only app still gets a landing). An app passes its own marketing
    /// / reference pages here.
    pub landing: Router<state::AppState>,
    /// App-contributed OpenAPI paths object, merged into `/openapi.json` — so an
    /// app can document routes the platform doesn't name (e.g. the realtime
    /// SSE/WS routes it mounts). Shape: `{ "/path": { "get": {<operation>} } }`.
    /// Default empty.
    pub openapi_paths: serde_json::Value,
    pub workers: Vec<Box<dyn UpstreamWorker>>,
    /// Enable the platform's LLM reverse-proxy feature (`/v1/*`). Optional per
    /// app — off by default; an app flips this to serve LLM (proxies to
    /// `LUMID_LLM_BACKEND_URL`). The capability lives in the platform; only
    /// the decision to expose it is the app's.
    pub enable_llm: bool,
}

impl Default for ServeParts {
    fn default() -> Self {
        Self {
            ext_routes: Router::new(),
            public_routes: Router::new(),
            landing: crate::handlers::landing::default_routes(),
            openapi_paths: serde_json::json!({}),
            workers: Vec::new(),
            enable_llm: false,
        }
    }
}

/// Boot + serve until shutdown. Reads `LUMID_*` from env (legacy `FINDATA_*`
/// accepted as a fallback — see `config::env_var`), incl. `LUMID_READ_CONFIG`
/// for the read specs (legacy `FINDATA_FINANCIAL_CONFIG`), default `read.toml`.
pub async fn serve(parts: ServeParts) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let settings = Arc::new(config::Settings::from_env());
    let bind_addr = settings.bind_addr.clone();
    let pool = db::build_pool(&settings)?;
    let lumid = Arc::new(auth::lumid::LumidClient::new(&settings));
    let local_keys = Arc::new(auth::parse_local_keys(&settings.api_keys_raw));
    let rate = Arc::new(auth::ratelimit::RateLimiter::new(
        &settings.rate_limit_anon,
        &settings.rate_limit_authed,
    ));
    tracing::info!(
        "auth: {} local key(s), lumid {}",
        local_keys.len(),
        if settings.lumid_enabled { "enabled" } else { "disabled" }
    );

    let (redis, redis_client) = if settings.redis_url.is_empty() {
        (None, None)
    } else {
        match redis::Client::open(settings.redis_url.clone()) {
            Ok(c) => match c.get_multiplexed_async_connection().await {
                Ok(conn) => {
                    tracing::info!("redis connected ({})", settings.redis_url);
                    (Some(conn), Some(c))
                }
                Err(e) => {
                    tracing::warn!("redis connect failed: {e}");
                    (None, None)
                }
            },
            Err(e) => {
                tracing::warn!("redis url invalid: {e}");
                (None, None)
            }
        }
    };

    let hub = match (&redis, &redis_client) {
        (Some(mux), Some(client)) => {
            let h =
                realtime::start(settings.clone(), client.clone(), mux.clone(), pool.clone(), parts.workers).await;
            // Warm a baseline subscription set so `/quotes` stays live without a
            // client stream (demand-gating otherwise leaves last:tick cold).
            h.warm(&settings.rt_warm_symbols).await;
            Some(h)
        }
        _ => None,
    };

    let http = reqwest::Client::builder()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let blob_store = build_blob_store(&settings);

    // Canonical: LUMID_READ_CONFIG. Legacy fallbacks: FINDATA_READ_CONFIG (via
    // env_var) and the original domain-named FINDATA_FINANCIAL_CONFIG.
    let spec_path = config::env_var("READ_CONFIG")
        .or_else(|| std::env::var("FINDATA_FINANCIAL_CONFIG").ok())
        .unwrap_or_else(|| "read.toml".to_string());
    let specs = match read::load_specs(&spec_path) {
        Ok(s) => {
            tracing::info!("read layer: {} declarative endpoints from {spec_path}", s.len());
            s
        }
        Err(e) => {
            tracing::warn!("read layer disabled: {e}");
            Vec::new()
        }
    };
    let reverse = read::build_reverse(&specs);
    let read_cache = read::cache::CacheManager::new(
        256 * 1024 * 1024,
        std::time::Duration::from_secs(86_400),
        redis.clone(),
        reverse,
    );
    if let Some(client) = &redis_client {
        read_cache.start_invalidation_listener(client.clone());
    }

    // Storage-backend registry. Postgres is always the default; ClickHouse is
    // registered as an additional backend when `LUMID_CLICKHOUSE_URL` is set
    // (Phase B). With CH unconfigured this is the Phase-A zero-behavior-change
    // wrapper — every table resolves to Postgres.
    let backends = if settings.ch_url.is_empty() {
        Arc::new(crate::backend::Registry::new_postgres_only(pool.clone()))
    } else {
        let ch = crate::backend::ClickHouseBackend::new(
            &settings.ch_url,
            &settings.ch_user,
            &settings.ch_password,
            &settings.ch_database,
        );
        tracing::info!(
            "backends: ClickHouse enabled ({} db={})",
            settings.ch_url,
            settings.ch_database
        );
        Arc::new(crate::backend::Registry::new_with_clickhouse(pool.clone(), ch))
    };

    let state = state::AppState {
        pool, settings, lumid, local_keys, rate, redis, redis_client, hub, http, read_cache, blob_store, backends,
    };

    // Auto-MCP: one tool per declarative read endpoint, merged into ext routes.
    let mcp_registry = Arc::new(mcp::registry_from_specs(&specs));
    tracing::info!("mcp: {} tools (POST /mcp)", mcp_registry.len());
    let mut ext_router = parts.ext_routes.merge(mcp::build_router(mcp_registry));
    if parts.enable_llm {
        ext_router = ext_router.merge(crate::llm::routes());
        tracing::info!("llm proxy enabled (/v1/*)");
    }

    let read_router = read::exec::build_router(&specs);
    let openapi_router = crate::openapi::build_router(&specs, &parts.openapi_paths);
    let router = app::build_router(
        state,
        read_router,
        ext_router,
        openapi_router,
        parts.public_routes,
        parts.landing,
    );
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("listening on {bind_addr}");
    axum::serve(listener, router).await?;
    Ok(())
}

/// Build the blob object-store backend from settings. Default is the local
/// filesystem rooted at `blob_root` (on-disk layout identical to the legacy
/// `tokio::fs` path). When `blob_backend=="s3"` an S3/MinIO store is built; if
/// that build fails we log and fall back to localfs rather than panicking.
fn build_blob_store(settings: &config::Settings) -> Arc<dyn object_store::ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    use object_store::local::LocalFileSystem;

    if settings.blob_backend.eq_ignore_ascii_case("s3") {
        match AmazonS3Builder::new()
            .with_endpoint(&settings.blob_s3_endpoint)
            .with_bucket_name(&settings.blob_s3_bucket)
            .with_region(&settings.blob_s3_region)
            .with_access_key_id(&settings.blob_s3_access_key)
            .with_secret_access_key(&settings.blob_s3_secret_key)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
        {
            Ok(s3) => {
                tracing::info!(
                    "blob store: s3/minio (endpoint={}, bucket={})",
                    settings.blob_s3_endpoint,
                    settings.blob_s3_bucket
                );
                return Arc::new(s3);
            }
            Err(e) => {
                tracing::warn!("blob s3 backend build failed ({e}); falling back to localfs");
            }
        }
    }

    // Local filesystem (default + s3 fallback). The prefix root must exist.
    if let Err(e) = std::fs::create_dir_all(&settings.blob_root) {
        tracing::warn!("blob root mkdir {} failed: {e}", settings.blob_root);
    }
    let local = LocalFileSystem::new_with_prefix(&settings.blob_root)
        .expect("blob localfs init (blob_root must be a valid directory)");
    tracing::info!("blob store: localfs (root={})", settings.blob_root);
    Arc::new(local)
}
