//! Multi-tier response cache for the read layer — read speed is the priority.
//!
//! - **L1**: in-process `moka::future::Cache`, byte-weighted (LRU under a hard
//!   cap), with a per-entry TTL via an `Expiry` impl. Single-flight via
//!   `try_get_with` so concurrent cold-key misses coalesce into one DB query.
//! - **L2**: optional shared Redis (`SETEX`/`GET`), value = `etag\n<body>`.
//!   Survives restarts + shared across replicas; best-effort (errors = miss).
//! - **Invalidation**: a `table → endpoint_ids` reverse index drives L1 drops
//!   on writes; a Redis pub/sub channel fans the drop to other replicas.
//! - **Edge**: each body carries a strong ETag (sha256) for `If-None-Match`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use moka::future::Cache;
use moka::Expiry;
use redis::AsyncCommands;
use sha2::{Digest, Sha256};

use crate::error::{ApiError, ApiResult};

pub const INVALIDATE_CHANNEL: &str = "cache:invalidate";
const KEY_NS: &str = "rc:v1";

/// Cache key: endpoint id + generation + canonical (post-coercion) params.
/// The `gen` is bumped on invalidation, so all prior L1/L2 entries for the
/// endpoint become instantly unreachable (no lazy moka sweep, no L2 scan).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub endpoint_id: Arc<str>,
    pub gen: u64,
    pub params_canon: Arc<str>,
}

impl CacheKey {
    pub fn new(endpoint_id: Arc<str>, gen: u64, params_canon: String) -> Self {
        Self { endpoint_id, gen, params_canon: params_canon.into() }
    }
    fn redis_key(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.params_canon.as_bytes());
        format!("{KEY_NS}:{}:{}:{:x}", self.endpoint_id, self.gen, h.finalize())
    }
}

/// A cached response body + its precomputed ETag and TTL.
pub struct CachedBody {
    pub bytes: Bytes,
    pub etag: Arc<str>,
    pub ttl: Duration,
}

impl CachedBody {
    pub fn new(bytes: Vec<u8>, ttl: Duration) -> Arc<Self> {
        let mut h = Sha256::new();
        h.update(&bytes);
        let etag: Arc<str> = format!("\"{:x}\"", h.finalize()).into();
        Arc::new(Self { bytes: Bytes::from(bytes), etag, ttl })
    }
}

/// Per-entry TTL: moka's global `time_to_live` is one value for the whole
/// cache, so we drive expiry from each entry's own `ttl`.
struct PerEntryTtl;
impl Expiry<CacheKey, Arc<CachedBody>> for PerEntryTtl {
    fn expire_after_create(
        &self,
        _k: &CacheKey,
        v: &Arc<CachedBody>,
        _now: std::time::Instant,
    ) -> Option<Duration> {
        Some(v.ttl)
    }
}

pub struct CacheManager {
    l1: Cache<CacheKey, Arc<CachedBody>>,
    redis: Option<redis::aio::MultiplexedConnection>,
    /// "schema.table" → endpoint ids that read it (for invalidation).
    reverse: HashMap<String, HashSet<Arc<str>>>,
    /// Per-endpoint generation; bumped on invalidation (lock-free reads).
    generations: dashmap::DashMap<Arc<str>, u64>,
}

impl CacheManager {
    /// `reverse` maps each "schema.table" to the endpoint ids that read it.
    pub fn new(
        max_bytes: u64,
        ttl_ceiling: Duration,
        redis: Option<redis::aio::MultiplexedConnection>,
        reverse: HashMap<String, HashSet<Arc<str>>>,
    ) -> Arc<Self> {
        let l1 = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_k: &CacheKey, v: &Arc<CachedBody>| {
                v.bytes.len().min(u32::MAX as usize) as u32
            })
            .expire_after(PerEntryTtl)
            .time_to_live(ttl_ceiling) // safety ceiling
            .build();
        Arc::new(Self { l1, redis, reverse, generations: dashmap::DashMap::new() })
    }

    /// Current generation for an endpoint (0 until first invalidation).
    pub fn generation(&self, endpoint_id: &str) -> u64 {
        self.generations.get(endpoint_id).map(|g| *g).unwrap_or(0)
    }

    /// Fetch from L1 → L2 → `compute`, filling both. Single-flight per key.
    /// `compute` returns the serialized JSON body; errors are NOT cached.
    pub async fn get_or_compute<F, Fut>(
        &self,
        key: CacheKey,
        ttl: Duration,
        use_l2: bool,
        compute: F,
    ) -> ApiResult<Arc<CachedBody>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = ApiResult<Vec<u8>>> + Send,
    {
        // L2 promotion happens inside the single-flight init so only one task
        // does the Redis round-trip / compute per cold key.
        let redis = self.redis.clone();
        let rkey = key.redis_key();
        let init = async move {
            if use_l2 {
                if let Some(mut conn) = redis.clone() {
                    if let Ok(Some(raw)) = conn.get::<_, Option<Vec<u8>>>(&rkey).await {
                        if let Some(body) = decode_l2(&raw, ttl) {
                            return Ok(body);
                        }
                    }
                }
            }
            let bytes = compute().await?;
            let body = CachedBody::new(bytes, ttl);
            if use_l2 {
                if let Some(mut conn) = redis {
                    let payload = encode_l2(&body);
                    let _: Result<(), _> =
                        conn.set_ex(&rkey, payload, ttl.as_secs().max(1)).await;
                }
            }
            Ok::<Arc<CachedBody>, ApiError>(body)
        };
        self.l1
            .try_get_with(key, init)
            .await
            .map_err(|arc: Arc<ApiError>| arc.clone_lite())
    }

    /// Invalidate every endpoint that reads `schema.table` by bumping its
    /// generation — instantly orphaning all prior L1+L2 entries (their keys
    /// carry the old gen and are never looked up again; LRU/TTL reclaims them).
    /// Immediate, unlike moka's lazy `invalidate_entries_if`. Returns the
    /// affected endpoint ids (for the pub/sub fanout).
    pub async fn invalidate_table_local(&self, schema: &str, table: &str) -> Vec<Arc<str>> {
        let full = format!("{schema}.{table}");
        let Some(ids) = self.reverse.get(&full) else {
            return Vec::new();
        };
        for id in ids {
            *self.generations.entry(id.clone()).or_insert(0) += 1;
        }
        ids.iter().cloned().collect()
    }

    /// Full invalidation: local L1+L2 + cross-replica publish. Call after a
    /// committed write to `schema.table`.
    pub async fn invalidate_table(&self, schema: &str, table: &str) {
        let affected = self.invalidate_table_local(schema, table).await;
        if affected.is_empty() {
            return;
        }
        if let Some(mut conn) = self.redis.clone() {
            let _: Result<(), _> = conn
                .publish(INVALIDATE_CHANNEL, format!("{schema}.{table}"))
                .await;
        }
    }

    /// Subscribe to `cache:invalidate` and drop L1+L2 for the named table on
    /// each message. This is how writers that bypass the ingress plane — the
    /// cron loaders/scrapers writing directly to Postgres — keep cached reads
    /// fresh: after a batch they `PUBLISH cache:invalidate "schema.table"`.
    /// Also delivers cross-replica invalidations. Reconnects on drop.
    pub fn start_invalidation_listener(self: &Arc<Self>, client: redis::Client) {
        let mgr = self.clone();
        tokio::spawn(async move {
            loop {
                match client.get_async_pubsub().await {
                    Ok(mut ps) => {
                        if ps.subscribe(INVALIDATE_CHANNEL).await.is_ok() {
                            tracing::info!("cache invalidation listener subscribed");
                            let mut stream = ps.on_message();
                            while let Some(msg) = stream.next().await {
                                if let Ok(p) = msg.get_payload::<String>() {
                                    if let Some((schema, table)) = p.split_once('.') {
                                        mgr.invalidate_table_local(schema, table).await;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!("cache invalidate pubsub connect failed: {e}"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

fn encode_l2(body: &CachedBody) -> Vec<u8> {
    // value = etag + '\n' + json bytes
    let mut out = Vec::with_capacity(body.etag.len() + 1 + body.bytes.len());
    out.extend_from_slice(body.etag.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(&body.bytes);
    out
}

fn decode_l2(raw: &[u8], ttl: Duration) -> Option<Arc<CachedBody>> {
    let nl = raw.iter().position(|&b| b == b'\n')?;
    let etag: Arc<str> = String::from_utf8_lossy(&raw[..nl]).into_owned().into();
    let bytes = Bytes::copy_from_slice(&raw[nl + 1..]);
    Some(Arc::new(CachedBody { bytes, etag, ttl }))
}
