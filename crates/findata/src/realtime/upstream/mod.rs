//! Realtime provider upstream workers — port of `api/realtime/upstream/`.
//!
//! Each module exposes `start(hub, redis, settings, …)` which registers its
//! demand listener(s) with the hub and spawns its long-running worker tasks,
//! then returns. They publish to the `tick:* / news:* / kol:*` Redis channels
//! the hub fans out. Registration order matters: FMP claims crypto/forex
//! first, Finnhub shadows them + serves equities, and Tier-B polling registers
//! last so it only picks up symbols no Tier-A upstream claimed.

pub mod finnhub_ws;
pub mod fmp_ws;
pub mod kol;
pub mod news;
pub mod polling;

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
