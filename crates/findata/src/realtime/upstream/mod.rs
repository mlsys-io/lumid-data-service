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

macro_rules! worker {
    ($ty:ident, $name:literal, |$h:ident, $m:ident, $s:ident, $p:ident| $body:expr) => {
        pub struct $ty;
        impl UpstreamWorker for $ty {
            fn name(&self) -> &'static str {
                $name
            }
            fn start(
                &self,
                $h: Arc<Hub>,
                $m: redis::aio::MultiplexedConnection,
                $s: Arc<Settings>,
                $p: Pool,
            ) -> BoxFuture<'static, anyhow::Result<()>> {
                Box::pin(async move { $body })
            }
        }
    };
}

worker!(FmpWs, "fmp_ws", |hub, mux, settings, _pool| fmp_ws::start(hub, mux, settings).await);
worker!(FinnhubWs, "finnhub_ws", |hub, mux, settings, _pool| finnhub_ws::start(hub, mux, settings).await);
worker!(News, "news", |hub, mux, settings, _pool| news::start(hub, mux, settings).await);
worker!(Kol, "kol", |hub, mux, settings, pool| kol::start(hub, mux, settings, pool).await);
worker!(Polling, "polling", |hub, mux, settings, _pool| polling::start(hub, mux, settings).await);

/// The financial provider set, in registration order (FMP → Finnhub → news →
/// kol → polling) — preserves the crypto/forex claim precedence (bite #28).
/// Moves to findata-ext on extraction.
pub fn financial_workers() -> Vec<Box<dyn UpstreamWorker>> {
    vec![
        Box::new(FmpWs),
        Box::new(FinnhubWs),
        Box::new(News),
        Box::new(Kol),
        Box::new(Polling),
    ]
}
