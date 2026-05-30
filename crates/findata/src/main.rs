//! lumid-data-service — the unified Rust (axum) data service.
//!
//! Phase 0: skeleton + DB foundation. Phase 1: auth + tiered rate limit + the
//! canary read set. Subsequent phases add the full read surface, the write
//! engine, and the reverse-proxy gateway to the Python sidecars.

mod app;
mod auth;
mod config;
mod db;
mod error;
mod handlers;
mod queries;
mod state;

use std::sync::Arc;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let settings = config::Settings::from_env();
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

    let state = state::AppState {
        pool,
        settings: Arc::new(settings),
        lumid,
        local_keys,
        rate,
    };

    let router = app::build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("lumid-data-service listening on {bind_addr}");
    axum::serve(listener, router).await?;
    Ok(())
}
