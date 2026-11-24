//! Multi-user jobs over federated data hubs.
//!
//! lum.id/data is the entrance: users and agents submit jobs here, each job
//! declares the data sources it reads and writes, and the platform plans where
//! it runs (next to its data) and how it reaches each source. Sources are
//! user-owned and shareable. Data never moves between hubs; only catalog
//! metadata, the registry, and small user data sync — and every synced row has
//! exactly one writer, its `home_hub`.
//!
//! * Phase 1: the schema, the source-ref grammar, boot-time seeding of this
//!   hub's estate sources and routes.
//! * Phase 2: the access rule (`access`), per-job tokens (`token`), and
//!   job-token enforcement (`enforce`, with the pure checks in `scope`).
//!
//! Everything is off unless `LUMID_JOBS_ENABLED` is set: no schema, no seed,
//! and `ljt_` tokens are refused — deploying it changes nothing.

pub mod access;
pub mod enforce;
pub mod refs;
pub mod schema;
pub mod scope;
pub mod seed;
pub mod token;

use deadpool_postgres::Pool;

use crate::config::Settings;

/// Boot hook: migrate the schema and seed this hub. A no-op unless enabled.
/// Invalid seed config fails boot loudly — a hub with a wrong route table
/// would plan jobs onto paths that don't exist.
pub async fn boot(pool: &Pool, settings: &Settings) -> anyhow::Result<()> {
    if !settings.jobs_enabled {
        return Ok(());
    }
    let estate = seed::parse_estate(&settings.jobs_estate).map_err(anyhow::Error::msg)?;
    let routes = seed::parse_routes(&settings.jobs_routes_json).map_err(anyhow::Error::msg)?;
    schema::migrate(pool, &settings.retrieval_db_role).await?;
    let rep = seed::apply(pool, &settings.hub_id, &settings.hub_site, &estate, &routes).await?;
    tracing::info!(
        hub = %settings.hub_id,
        site = %settings.hub_site,
        estate = ?estate,
        routes = routes.len(),
        estate_upserted = rep.estate_upserted,
        routes_upserted = rep.routes_upserted,
        routes_tombstoned = rep.routes_tombstoned,
        "jobs: registry ready"
    );
    Ok(())
}
