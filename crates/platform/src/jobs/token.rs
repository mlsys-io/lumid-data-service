//! Per-job tokens (`ljt_…`).
//!
//! * Opaque, 244 random bits; only the sha256 is stored (`jobs.job_tokens`),
//!   so a database read never yields a usable credential.
//! * Issued by the hub that owns the job; local-only (never synced).
//! * Never placed in a dispatch payload: a runtime fetches one at run time with
//!   its own service key, so a task record or a restarted server never holds
//!   it (the FlowMesh `credential_not_retained` failure mode).
//! * Live while the job is active, plus [`GRACE_AFTER_END`] for final writes;
//!   never past `expires_at`; revoked on cancel/finish.
//! * Resolving a token loads the job's SCOPE: its declared sources, each
//!   re-checked against [`access::can`] as the job's owner. The scope is cached
//!   for [`SCOPE_TTL`], so revoking a share reaches running jobs within that.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use moka::future::Cache;
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::access::{self, Access};
use super::refs;
use super::scope::{Scope, ScopedRef};

/// Token prefix, so the auth gate can route it without a lookup elsewhere.
pub const PREFIX: &str = "ljt_";
/// How long a resolved scope is reused before grants are re-checked.
pub const SCOPE_TTL: Duration = Duration::from_secs(15);
/// A finished job's token keeps working this long (final writes, log flush).
pub const GRACE_AFTER_END: chrono::Duration = chrono::Duration::minutes(10);
/// Longest lifetime of any one token (renewable).
pub const MAX_TTL: chrono::Duration = chrono::Duration::hours(24);

/// A resolved, live job token.
#[derive(Clone, Debug)]
pub struct JobCtx {
    pub job_id: Uuid,
    pub owner_sub: String,
    pub app: String,
    pub scope: Scope,
}

/// sha256 of the token string — the only form ever stored or cached.
pub fn hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// A fresh token string: `ljt_` + 64 hex chars from two v4 UUIDs (OS RNG).
pub fn generate() -> String {
    let mut s = String::with_capacity(PREFIX.len() + 64);
    s.push_str(PREFIX);
    s.push_str(&hex::encode(Uuid::new_v4().as_bytes()));
    s.push_str(&hex::encode(Uuid::new_v4().as_bytes()));
    s
}

/// Is `token` shaped like a job token (cheap pre-check before any lookup)?
pub fn looks_like(token: &str) -> bool {
    token.len() == PREFIX.len() + 64
        && token.starts_with(PREFIX)
        && token[PREFIX.len()..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// Issue a token for `job_id`, valid for `ttl` (capped at [`MAX_TTL`]).
/// Returns the token (shown to the caller once) and its expiry.
pub async fn mint(
    pool: &Pool,
    job_id: Uuid,
    ttl: chrono::Duration,
) -> anyhow::Result<(String, DateTime<Utc>)> {
    let token = generate();
    let expires = Utc::now() + ttl.min(MAX_TTL);
    let client = pool.get().await?;
    client
        .execute(
            "INSERT INTO jobs.job_tokens (token_sha256, job_id, expires_at) VALUES ($1, $2, $3)",
            &[&hash(&token).as_slice(), &job_id, &expires],
        )
        .await?;
    Ok((token, expires))
}

/// Revoke every token of a job (cancel / terminal status).
pub async fn revoke_job(pool: &Pool, job_id: Uuid) -> anyhow::Result<u64> {
    let client = pool.get().await?;
    let n = client
        .execute(
            "UPDATE jobs.job_tokens SET revoked_at = now()
              WHERE job_id = $1 AND revoked_at IS NULL",
            &[&job_id],
        )
        .await?;
    CACHE.invalidate_all();
    Ok(n)
}

/// Resolved scopes, keyed by token hash. `None` = known-invalid (cached too,
/// so a bad token cannot be used to hammer the DB).
static CACHE: Lazy<Cache<[u8; 32], Option<Arc<JobCtx>>>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(50_000)
        .time_to_live(SCOPE_TTL)
        .build()
});

/// Resolve a job token to its live context, or `None` when it is unknown,
/// expired, revoked, or its job is no longer active.
pub async fn resolve(
    pool: &Pool,
    hub_id: &str,
    instance: &str,
    token: &str,
) -> anyhow::Result<Option<Arc<JobCtx>>> {
    if !looks_like(token) {
        return Ok(None);
    }
    let key = hash(token);
    if let Some(hit) = CACHE.get(&key).await {
        return Ok(hit);
    }
    let ctx = load(pool, hub_id, instance, &key).await?.map(Arc::new);
    CACHE.insert(key, ctx.clone()).await;
    Ok(ctx)
}

async fn load(
    pool: &Pool,
    hub_id: &str,
    instance: &str,
    key: &[u8; 32],
) -> anyhow::Result<Option<JobCtx>> {
    let client = pool.get().await?;
    let Some(row) = client
        .query_opt(
            "SELECT j.job_id, j.owner_sub, j.app
               FROM jobs.job_tokens t JOIN jobs.jobs j ON j.job_id = t.job_id
              WHERE t.token_sha256 = $1
                AND t.revoked_at IS NULL
                AND t.expires_at > now()
                AND j.deleted_at IS NULL
                AND (j.status IN ('planned','submitted','running')
                     OR (j.ended_at IS NOT NULL AND j.ended_at > $2))",
            &[&key.as_slice(), &(Utc::now() - GRACE_AFTER_END)],
        )
        .await?
    else {
        return Ok(None);
    };
    let job_id: Uuid = row.get("job_id");
    let owner_sub: String = row.get("owner_sub");
    let app: String = row.get("app");
    let declared = client
        .query(
            "SELECT ref, access, source_id FROM jobs.job_sources WHERE job_id = $1",
            &[&job_id],
        )
        .await?;
    drop(client);

    // Keep only what the owner may STILL use. A ref with no resolved source
    // id is dropped: every ref is resolved to a source at submit time, so a
    // missing one means the row was not written by the submit path.
    let mut scope = Scope {
        hub_id: hub_id.to_string(),
        instance: instance.to_string(),
        refs: Vec::new(),
    };
    for r in declared {
        let raw: String = r.get("ref");
        let Some(acc) = Access::parse(r.get::<_, &str>("access")) else {
            continue;
        };
        let Some(source_id) = r.get::<_, Option<String>>("source_id") else {
            continue;
        };
        let Ok(parsed) = refs::parse(&raw) else {
            continue;
        };
        if access::can(pool, &owner_sub, &source_id, acc).await?.allow {
            scope.refs.push(ScopedRef {
                raw,
                parsed,
                access: acc,
            });
        }
    }
    Ok(Some(JobCtx {
        job_id,
        owner_sub,
        app,
        scope,
    }))
}

/// Drop every cached scope (tests; also after a share is revoked, so the
/// local process doesn't wait out [`SCOPE_TTL`]).
pub fn invalidate_all() {
    CACHE.invalidate_all();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_well_formed_and_distinct() {
        let a = generate();
        let b = generate();
        assert!(looks_like(&a) && looks_like(&b));
        assert_ne!(a, b);
        assert_eq!(a.len(), 68);
        assert_ne!(hash(&a), hash(&b));
    }

    #[test]
    fn shape_check_rejects_lookalikes() {
        assert!(!looks_like("ljt_"));
        assert!(!looks_like(&format!("ljt_{}", "z".repeat(64))));
        assert!(!looks_like(&format!("lm_pat_{}", "a".repeat(64))));
        assert!(!looks_like(&format!("ljt_{}", "a".repeat(63))));
    }
}
