//! One-call boot for an application built on the platform. `serve()` does all
//! the wiring (settings, pool, auth, redis, hub+workers, read specs, cache +
//! invalidation, auto-MCP from specs, router, listener) so an app's `main` is:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     lumid_platform::serve(lumid_platform::ServeParts {
//!         ext_routes: my_ext::routes(),   // or axum::Router::new()
//!         workers: my_ext::workers(),     // or vec![]
//!     }).await
//! }
//! ```

use std::sync::Arc;

use axum::Router;
use tracing_subscriber::EnvFilter;

use crate::realtime::upstream::UpstreamWorker;
use crate::retrieve::card_store::CardStore;
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
    /// App `/status` feed-liveness policy (warm-symbol grouping + expected-live).
    /// `None` ⇒ the platform default (`realtime` group, always expected live).
    pub feed_liveness: Option<Arc<dyn realtime::FeedLiveness>>,
    pub workers: Vec<Box<dyn UpstreamWorker>>,
    /// Enable the platform's LLM reverse-proxy feature (`/v1/*`). Optional per
    /// app — off by default; an app flips this to serve LLM (proxies to
    /// `LUMID_LLM_BACKEND_URL`). The capability lives in the platform; only
    /// the decision to expose it is the app's.
    pub enable_llm: bool,
    /// Enable the agent tool-use loop (`POST /agent/v1`). Requires `enable_llm`
    /// to also be `true` — the loop calls the `/v1/chat/completions` proxy.
    /// `serve()` returns an error at startup when `enable_agent=true` but
    /// `enable_llm=false`.
    pub enable_agent: bool,
    /// Enable the generic data-push plane: the inbox (`POST /sync/apply/...`) +
    /// admin push routes, and create the `sync` bookkeeping tables at boot. Off
    /// by default. A target (inbox) must enable this; a pure producer that only
    /// uses the push helper can also enable it to expose `/admin/sync/*`.
    pub enable_sync: bool,
}

impl Default for ServeParts {
    fn default() -> Self {
        Self {
            ext_routes: Router::new(),
            public_routes: Router::new(),
            landing: crate::handlers::landing::default_routes(),
            openapi_paths: serde_json::json!({}),
            feed_liveness: None,
            workers: Vec::new(),
            enable_llm: false,
            enable_agent: false,
            enable_sync: false,
        }
    }
}

/// Validate `ServeParts` for incompatible flag combinations.
///
/// Extracted so tests can call this without spinning up a full server.
/// Returns an error when `enable_agent=true` but `enable_llm=false`.
pub fn check_serve_parts(parts: &ServeParts) -> anyhow::Result<()> {
    if parts.enable_agent && !parts.enable_llm {
        anyhow::bail!(
            "enable_agent requires enable_llm to be true \
             (the agent loop calls the /v1/* proxy)"
        );
    }
    Ok(())
}

/// Validate critical settings before starting the server. Fails fast with a
/// clear message rather than surfacing misconfiguration on the first real request.
fn validate_settings(s: &config::Settings) -> anyhow::Result<()> {
    if s.db_password.is_empty() {
        anyhow::bail!("LUMID_DB_PASSWORD is required but not set");
    }
    if s.db_name.is_empty() {
        anyhow::bail!("LUMID_DB_NAME is required but not set");
    }
    if s.blob_backend == "s3" {
        if s.blob_s3_bucket.is_empty() {
            anyhow::bail!("LUMID_BLOB_S3_BUCKET is required when LUMID_BLOB_BACKEND=s3");
        }
        if s.blob_s3_endpoint.is_empty() {
            anyhow::bail!("LUMID_BLOB_S3_ENDPOINT is required when LUMID_BLOB_BACKEND=s3");
        }
    }
    if !s.lumid_enabled && s.api_keys_raw.is_empty() {
        tracing::warn!(
            "LUMID_AUTH_ENABLED=false and no LUMID_API_KEYS configured — \
             all authenticated requests will return 401"
        );
    }
    Ok(())
}

/// Boot + serve until shutdown. Reads `LUMID_*` from env, incl.
/// `LUMID_READ_CONFIG` for the read specs (default `read.toml`).
pub async fn serve(parts: ServeParts) -> anyhow::Result<()> {
    check_serve_parts(&parts)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // rustls 0.23 needs a process-global crypto provider. Both `ring` and
    // `aws-lc-rs` are in the dependency graph (the latter via the ClickHouse
    // client), so rustls can't auto-determine one and the FIRST TLS connection
    // (a WS upstream / reqwest / ClickHouse) panics its task. Install ring once,
    // up front, so every TLS user shares it. Idempotent; ignore "already set".
    let _ = rustls::crypto::ring::default_provider().install_default();

    let settings = Arc::new(config::Settings::from_env());
    validate_settings(&settings)?;
    let bind_addr = settings.bind_addr.clone();
    let pool = db::build_pool(&settings)?;
    // Optional secondary pool for xpio/mailbox when that schema lives in a
    // separate Postgres instance (findata: warehouse in the main pool, LQT
    // mailbox + xpio.* in the LQT data DB). None ⇒ handlers use the main pool.
    let xpio_pool = db::build_xpio_pool(&settings)?;
    if xpio_pool.is_some() {
        tracing::info!("xpio: dedicated secondary DB pool active (LUMID_XPIO_DB_HOST set)");
    }
    let lumid = Arc::new(auth::lumid::LumidClient::new(&settings));
    let local_keys = Arc::new(auth::parse_local_keys(&settings.api_keys_raw));
    let rate = Arc::new(auth::ratelimit::RateLimiter::new(
        &settings.rate_limit_anon,
        &settings.rate_limit_authed,
    ));
    let concurrency = if settings.max_concurrency_per_key > 0 || settings.max_concurrency_per_ip > 0 {
        let pk = settings.max_concurrency_per_key.max(1);
        let pi = settings.max_concurrency_per_ip.max(1);
        tracing::info!("concurrency limits: {pk}/key  {pi}/ip");
        Some(Arc::new(auth::ratelimit::ConcurrencyLimiter::new(pk, pi)))
    } else {
        None
    };
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
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    // Streaming LLM client: connect timeout only. No total timeout — reasoning
    // models can legitimately take several minutes before the first token.
    let http_stream = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    // LLM backend pool — health-aware, least-loaded, with circuit breaker.
    let llm_pool = {
        let pool = Arc::new(crate::llm_pool::BackendPool::from_settings(&settings));
        pool.clone().start_health_prober(http.clone());
        // Scrape each backend's engine queue depth so the resolver can spill to
        // OpenRouter BEFORE queueing. The in-flight roof only counts requests
        // this process issued and is blind to other clients on the same GPU.
        pool.clone().start_queue_scraper(http.clone());
        tracing::info!(
            "llm pool: {} unique backend(s)",
            pool.all.len()
        );
        pool
    };

    let blob_store = build_blob_store(&settings);

    let spec_path = config::env_var("READ_CONFIG").unwrap_or_else(|| "read.toml".to_string());
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

    let feed_liveness = parts
        .feed_liveness
        .unwrap_or_else(|| Arc::new(realtime::DefaultFeedLiveness));

    // Data-push plane: create the `sync` bookkeeping tables (idempotent) when
    // enabled. A target that can't migrate its inbox shouldn't start.
    if parts.enable_sync {
        crate::sync::migrate(&pool).await?;
        if settings.sync_target_url.is_empty() {
            tracing::info!("sync: inbox enabled (/sync/apply); no push target configured");
        } else {
            tracing::info!(
                "sync: inbox + push enabled (target {})",
                settings.sync_target_url
            );
        }
    }

    let card_store = std::sync::Arc::new(CardStore::new(
        pool.clone(),
        settings.retrieval_card_ttl_s,
        settings.retrieval_sample_rows,
    ));

    // Federation (F1 mesh core): peer registry + forwarder. Reuses the shared
    // `http` client. Empty peer set / no `*_federate` ⇒ pure local (no-op).
    let federation = Arc::new(crate::federation::Federation::new(
        &settings.peers,
        http.clone(),
        settings.instance_id.clone(),
        settings.app_id.clone(),
    ));
    if federation.has_peers() {
        tracing::info!(
            "federation: instance={} app={} peers={} read_federate={:?} llm_federate={:?}",
            settings.instance_id,
            settings.app_id,
            settings.peers.len(),
            settings.read_federate,
            settings.llm_federate,
        );
    }

    // Shadow catch-all forward cache — only consulted when `read_federate` is set
    // (shadow mode). Built unconditionally (cheap); the middleware short-circuits
    // to a passthrough when not shadowing.
    let shadow_cache = crate::federation::ShadowCache::new(
        std::time::Duration::from_secs(settings.shadow_cache_ttl_s.max(1)),
    );
    if settings.read_federate.is_some() {
        tracing::info!(
            "shadow catch-all forward active (read_federate={:?}, cache_ttl={}s)",
            settings.read_federate,
            settings.shadow_cache_ttl_s,
        );
    }

    let state = state::AppState {
        pool, xpio_pool, settings, lumid, local_keys, rate, concurrency, redis, redis_client, hub, http,
        http_stream, llm_pool, read_cache, blob_store, backends, feed_liveness, card_store,
        federation, shadow_cache,
    };

    // Auto-MCP: one tool per declarative read endpoint, merged into ext routes.
    let mcp_registry = Arc::new(mcp::registry_from_specs(&specs));
    tracing::info!("mcp: {} tools (POST /mcp)", mcp_registry.len());
    let mut ext_router = parts.ext_routes.merge(mcp::build_router(mcp_registry));
    if parts.enable_llm {
        // A large-context turn exceeds axum's 2 MiB default and comes back 413
        // ("Failed to buffer the request body: length limit exceeded"). Measured
        // on this path: p99 2.08 MB and max 2.16 MB, so the default was
        // rejecting the top ~1% of real requests outright. Same treatment the
        // sync plane already gets below.
        ext_router = ext_router.merge(
            crate::llm::routes()
                .layer(axum::extract::DefaultBodyLimit::max(state.settings.llm_max_body_bytes as usize)),
        );
        tracing::info!(
            "llm proxy enabled (/v1/*), max body {} MiB",
            state.settings.llm_max_body_bytes / (1024 * 1024)
        );
    }
    if parts.enable_agent {
        ext_router = ext_router.merge(crate::agent::routes());
        tracing::info!("agent tool-use loop enabled (POST /agent/v1)");
    }
    if parts.enable_sync {
        // Inbox batches can exceed axum's 2 MB default; bound to the same limit
        // as the ingest write plane.
        ext_router = ext_router.merge(
            crate::sync::routes()
                .layer(axum::extract::DefaultBodyLimit::max(state.settings.ingest_max_bytes as usize)),
        );
        tracing::info!("sync plane enabled (POST /sync/apply, /admin/sync/*)");
    }

    let read_router = read::exec::build_router(&specs);
    // Document the /v1 surface whenever the LLM plane is on. openapi.rs builds
    // its doc from the DECLARATIVE read specs, so a compiled route like
    // /v1/chat/completions is invisible to it -- which is how lum.id/llm ended
    // up serving an /openapi.json of 18 paths, none of them LLM. The app can
    // still contribute its own paths; those WIN on a key collision, since an
    // app overriding a platform route's docs is deliberate.
    let mut openapi_paths = parts.openapi_paths.clone();
    if parts.enable_llm {
        if let (Some(dst), Some(src)) = (
            openapi_paths.as_object_mut(),
            crate::llm::openapi_paths().as_object(),
        ) {
            for (k, v) in src {
                dst.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }
    let openapi_router = crate::openapi::build_router(&specs, &openapi_paths);
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
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async { signal::ctrl_c().await.expect("ctrl-c handler") };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received, draining in-flight requests");
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
