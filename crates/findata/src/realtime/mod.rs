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
) -> Arc<hub::Hub> {
    let hub = hub::Hub::new(mux.clone());
    hub.start_listener(client);
    if settings.rt_synthetic {
        synthetic::run(mux.clone());
    }

    // Upstreams register their demand listeners in this order. Listeners fire
    // in registration order, so FMP gets first refusal on crypto/forex, then
    // Finnhub (equities + crypto/forex shadow), then the additive news/kol
    // overlays, and finally Tier-B polling for whatever Tier-A didn't claim.
    if let Err(e) = upstream::fmp_ws::start(hub.clone(), mux.clone(), settings.clone()).await {
        tracing::warn!("fmp_ws upstream start failed: {e}");
    }
    if let Err(e) = upstream::finnhub_ws::start(hub.clone(), mux.clone(), settings.clone()).await {
        tracing::warn!("finnhub_ws upstream start failed: {e}");
    }
    if let Err(e) = upstream::news::start(hub.clone(), mux.clone(), settings.clone()).await {
        tracing::warn!("news upstream start failed: {e}");
    }
    if let Err(e) =
        upstream::kol::start(hub.clone(), mux.clone(), settings.clone(), pool).await
    {
        tracing::warn!("kol upstream start failed: {e}");
    }
    if let Err(e) = upstream::polling::start(hub.clone(), mux.clone(), settings.clone()).await {
        tracing::warn!("polling upstream start failed: {e}");
    }

    hub
}
