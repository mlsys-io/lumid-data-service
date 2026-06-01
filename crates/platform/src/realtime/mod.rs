//! Realtime streaming subsystem — port of `api/realtime/`.
//!
//! - `hub`       — per-connection registry + Redis pub/sub fan-out
//! - `synthetic` — dev-only test publisher
//! - `upstream`  — the `UpstreamWorker` trait; concrete provider workers are
//!   supplied by the app layer (the platform names no provider)

pub mod health;
pub mod hub;
pub mod synthetic;
pub mod upstream;

use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::config::Settings;

/// App-provided policy for the `/status` realtime board. The board samples the
/// warm-symbol `last:tick` freshness; this policy decides **how to group** those
/// symbols (the pill label) and **whether a group is expected to be live now**
/// (so "no fresh ticks" reads as a failure vs an expected quiet period).
///
/// The platform names no asset class or market calendar: the default groups
/// everything as `realtime` and always expects live ticks (a warmed feed with no
/// ticks ⇒ fail). An app with market-hours knowledge overrides this via
/// `ServeParts.feed_liveness` (e.g. forex/equity sessions, 24/7 crypto).
pub trait FeedLiveness: Send + Sync {
    /// Group label for a warm symbol (becomes the status pill name).
    fn group(&self, _symbol: &str) -> String {
        "realtime".to_string()
    }
    /// Whether `group` is expected to be delivering live ticks at `now`.
    fn expected_live(&self, _group: &str, _now: chrono::DateTime<chrono::Utc>) -> bool {
        true
    }
}

/// Default policy: one `realtime` group, always expected live.
pub struct DefaultFeedLiveness;
impl FeedLiveness for DefaultFeedLiveness {}

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
    let hub = hub::Hub::new(mux.clone(), settings.rt_channel_kinds.clone());
    hub.start_listener(client);
    if settings.rt_synthetic {
        synthetic::run(mux.clone());
    }

    // Drive the registered upstreams in order. Each registers its demand
    // listener synchronously before returning, so registration order sets the
    // app-defined tier-claim precedence (the app orders its `workers()`).
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
