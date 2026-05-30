//! Realtime streaming subsystem — port of `api/realtime/`.
//!
//! - `hub`       — per-connection registry + Redis pub/sub fan-out
//! - `synthetic` — dev-only test publisher
//!
//! The provider upstream workers (finnhub_ws, fmp_ws, news, kol, polling) land
//! in a later batch; until then the hub fans out whatever any publisher writes
//! to the `tick:* / news:* / kol:*` Redis channels (the synthetic publisher,
//! the prediction-market WS recorder, etc.).

pub mod hub;
pub mod synthetic;

use std::sync::Arc;

use crate::config::Settings;

/// Build the hub, start its listener, and (optionally) the synthetic publisher.
/// Returns None when Redis is unconfigured (realtime endpoints then 503).
pub fn start(
    settings: &Settings,
    client: redis::Client,
    mux: redis::aio::MultiplexedConnection,
) -> Arc<hub::Hub> {
    let hub = hub::Hub::new(mux.clone());
    hub.start_listener(client);
    if settings.rt_synthetic {
        synthetic::run(mux);
    }
    hub
}
