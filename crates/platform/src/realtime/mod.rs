//! Realtime streaming subsystem — port of `api/realtime/`.
//!
//! - `hub`       — per-connection registry + Redis pub/sub fan-out
//! - `synthetic` — dev-only test publisher
//! - `upstream`  — provider workers (finnhub_ws, fmp_ws, news, kol, polling)

pub mod hub;
pub mod synthetic;
pub mod upstream;

use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::config::Settings;

/// Build the hub, start its listener, the optional synthetic publisher, and
/// all provider upstreams. Returns the hub (None is handled by the caller when
/// Redis is unconfigured).
pub async fn start(
    settings: Arc<Settings>,
    client: redis::Client,
    mux: redis::aio::MultiplexedConnection,
    pool: Pool,
    workers: Vec<Box<dyn upstream::UpstreamWorker>>,
) -> Arc<hub::Hub> {
    let hub = hub::Hub::new(mux.clone());
    hub.start_listener(client);
    if settings.rt_synthetic {
        synthetic::run(mux.clone());
    }

    // Drive the registered upstreams in order. Each registers its demand
    // listener synchronously before returning, so registration order sets the
    // tier-claim precedence (FMP → Finnhub → news → kol → polling; bite #28).
    for w in &workers {
        if let Err(e) = w
            .start(hub.clone(), mux.clone(), settings.clone(), pool.clone())
            .await
        {
            tracing::warn!("{} upstream start failed: {e}", w.name());
        }
    }

    hub
}
