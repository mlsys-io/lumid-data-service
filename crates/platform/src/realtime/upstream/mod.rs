//! Realtime upstream seam (platform). The `UpstreamWorker` trait is the IoC
//! boundary; the concrete provider modules (fmp_ws/finnhub_ws/news/kol/polling)
//! live in the `findata-ext` crate and register via `realtime::start`.

use std::sync::Arc;

use deadpool_postgres::Pool;
use futures_util::future::BoxFuture;

use crate::config::Settings;
use crate::realtime::hub::Hub;

/// A realtime provider upstream — the IoC seam between the generic hub
/// (platform) and the domain providers (financial). The platform's
/// `realtime::start` drives a `Vec<Box<dyn UpstreamWorker>>` in registration
/// order; the financial crate supplies the concrete workers. (Trait stays in
/// the platform; the concrete impls + `financial_workers()` move to
/// findata-ext on extraction.)
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
