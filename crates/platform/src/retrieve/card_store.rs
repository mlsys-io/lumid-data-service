//! In-process TTL cache for schema cards.
//!
//! Uses `moka` (already in the dependency tree). Key = a SHA-256 prefix
//! of the sorted, normalised schema-scope string. Value = `Arc<Vec<SchemaCard>>`.
//! TTL is read from `LUMID_RETRIEVAL_CARD_TTL_S` (default 300 s).

use std::sync::Arc;

use deadpool_postgres::Pool;
use moka::future::Cache;

use crate::error::ApiResult;

use super::card_builder::build_cards;
use super::schema_card::SchemaCard;

/// In-process get-or-build cache keyed on the scope hash.
pub struct CardStore {
    cache: Cache<String, Arc<Vec<SchemaCard>>>,
    pool: Pool,
    sample_rows: usize,
}

impl CardStore {
    pub fn new(pool: Pool, ttl_secs: u64, sample_rows: usize) -> Self {
        let cache = Cache::builder()
            .max_capacity(256)
            .time_to_live(std::time::Duration::from_secs(ttl_secs))
            .build();
        Self {
            cache,
            pool,
            sample_rows,
        }
    }

    /// Return cached cards for `scope`, building and storing them if absent or expired.
    pub async fn get_or_build(&self, scope: &[String]) -> ApiResult<Arc<Vec<SchemaCard>>> {
        let key = scope_key(scope);
        if let Some(cached) = self.cache.get(&key).await {
            return Ok(cached);
        }
        let cards = build_cards(&self.pool, scope, self.sample_rows).await?;
        let arc = Arc::new(cards);
        self.cache.insert(key, arc.clone()).await;
        Ok(arc)
    }
}

/// Stable cache key: SHA-256 of sorted, normalised scope strings (first 32 hex chars).
pub fn scope_key(scope: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut parts: Vec<String> = scope.iter().map(|s| s.trim().to_string()).collect();
    parts.sort();
    let normalised = parts.join(",");
    let hash = Sha256::digest(normalised.as_bytes());
    hex::encode(&hash[..16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_key_is_order_independent() {
        let a = scope_key(&["star".to_string(), "market".to_string()]);
        let b = scope_key(&["market".to_string(), "star".to_string()]);
        assert_eq!(a, b);
    }

    #[test]
    fn scope_key_empty_vs_nonempty() {
        let empty = scope_key(&[]);
        let nonempty = scope_key(&["star".to_string()]);
        assert_ne!(empty, nonempty);
    }
}
