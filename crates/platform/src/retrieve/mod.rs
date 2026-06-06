//! Deterministic data-retrieval pipeline: schema cards, in-process cache,
//! plan types, replayer, and materializer.

pub mod card_builder;
pub mod card_store;
pub mod materialize;
pub mod plan;
pub mod replayer;
pub mod schema_card;
