//! lumid-data-service — the unified Rust (axum) data service.
//!
//! Phase 0: skeleton + DB foundation (pool, dynamic row→JSON, lineage strip,
//! health, one real read). Subsequent phases add auth, the full read surface,
//! the write engine, and the reverse-proxy gateway to the Python sidecars.

mod app;
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
    let state = state::AppState {
        pool,
        settings: Arc::new(settings),
    };

    let router = app::build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("lumid-data-service listening on {bind_addr}");
    axum::serve(listener, router).await?;
    Ok(())
}
