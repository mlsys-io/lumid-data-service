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
