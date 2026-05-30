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
    pub workers: Vec<Box<dyn UpstreamWorker>>,
    /// Enable the platform's LLM reverse-proxy feature (`/v1/*`). Optional per
    /// app — off by default; an app flips this to serve LLM (proxies to
    /// `FINDATA_LLM_BACKEND_URL`). The capability lives in the platform; only
    /// the decision to expose it is the app's.
    pub enable_llm: bool,
}

impl Default for ServeParts {
    fn default() -> Self {
        Self { ext_routes: Router::new(), public_routes: Router::new(), workers: Vec::new(), enable_llm: false }
    }
}

/// Boot + serve until shutdown. Reads `FINDATA_*` from env (incl.
/// `FINDATA_FINANCIAL_CONFIG` for the read specs, default `financial.toml`).
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

    let spec_path = std::env::var("FINDATA_FINANCIAL_CONFIG").unwrap_or_else(|_| "financial.toml".to_string());
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

    let state = state::AppState {
        pool, settings, lumid, local_keys, rate, redis, redis_client, hub, http, read_cache,
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
    let openapi_router = crate::openapi::build_router(&specs);
    let router = app::build_router(state, read_router, ext_router, openapi_router, parts.public_routes);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("listening on {bind_addr}");
    axum::serve(listener, router).await?;
    Ok(())
}
