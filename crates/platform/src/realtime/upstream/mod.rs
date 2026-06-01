//! Realtime upstream seam (platform). The `UpstreamWorker` trait is the IoC
//! boundary; the concrete provider modules live in the app layer (e.g. the
//! `my_ext` crate) and register via `realtime::start`. The platform names
//! no provider.

use std::sync::Arc;

use deadpool_postgres::Pool;
use futures_util::future::BoxFuture;

use crate::config::Settings;
use crate::realtime::hub::Hub;

/// A realtime provider upstream — the IoC seam between the generic hub
/// (platform) and the app's domain feeds. The platform's `realtime::start`
/// drives a `Vec<Box<dyn UpstreamWorker>>` in registration order; the app crate
/// supplies the concrete workers (via `ServeParts.workers`). The trait stays in
/// the platform; the concrete impls live in the app (e.g. `my_ext`).
pub trait UpstreamWorker: Send + Sync {
    fn name(&self) -> &'static str;
    fn start(
        &self,
        hub: Arc<Hub>,
        mux: redis::aio::MultiplexedConnection,
        settings: Arc<Settings>,
        pool: Pool,
    ) -> BoxFuture<'static, anyhow::Result<()>>;
}
