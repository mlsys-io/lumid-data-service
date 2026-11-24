//! `POST /retrieve` — direct (non-LLM) SQL/storage retrieval endpoint.
//!
//! Exposes `retrieve::replayer::replay` directly over HTTP, bypassing the agent
//! loop. Identical safety boundaries apply: SELECT-only parser, READ ONLY
//! transaction, row cap, key sanitization.

use axum::extract::State;
use axum::{Extension, Json};
use deadpool_postgres::Pool;
use serde_json::Value;
use uuid::Uuid;

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::retrieve::materialize::OutputFormat;
use crate::retrieve::plan::{RetrievalOp, RetrievalPlan, SqlOp};
use crate::retrieve::replayer;
use crate::state::AppState;

/// Parse the request body `{sql, plan, output_format}` into
/// `(RetrievalPlan, OutputFormat)`.
///
/// Extracted as a pure function so it can be unit-tested without `AppState`.
pub fn parse_request(body: &Value) -> ApiResult<(RetrievalPlan, OutputFormat)> {
    let has_sql = body.get("sql").is_some();
    let has_plan = body.get("plan").is_some();

    if has_sql == has_plan {
        // both present or both absent
        return Err(ApiError::BadRequest(
            "provide exactly one of 'sql' or 'plan'".into(),
        ));
    }

    let plan = if has_sql {
        let query = body["sql"]
            .as_str()
            .ok_or_else(|| ApiError::BadRequest("'sql' must be a string".into()))?
            .to_string();
        RetrievalPlan {
            plan: vec![RetrievalOp::Sql(SqlOp { query })],
            expected_rowcount_or_size: None,
            rationale: None,
        }
    } else {
        let plan_val = body["plan"].clone();
        serde_json::from_value(plan_val)
            .map_err(|e| ApiError::BadRequest(format!("invalid plan JSON: {e}")))?
    };

    let fmt_str = body
        .get("output_format")
        .and_then(|v| v.as_str())
        .unwrap_or("jsonl");

    let output_format = OutputFormat::from_str(fmt_str).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unknown output_format: '{fmt_str}' (supported: jsonl, csv, raw — parquet is not implemented)"
        ))
    })?;

    Ok((plan, output_format))
}

// ── retrieval ownership ──────────────────────────────────────────────────────
//
// A retrieval materializes `<retrieval_prefix>/<run_id>/result.<ext>` and hands
// the caller that key. The run id used to be the only protection ("unguessable
// UUID"), so anyone holding — or logging, or being forwarded — a URI could read
// another user's result. Each run now records its owner here, and both readers
// (`GET /blobs/<key>` and a `storage_get` op inside a later plan) require the
// owner or a privileged role.

/// Idempotent DDL, applied at boot when blob storage is configured. One
/// simple-query batch = one implicit transaction, serialized on the platform
/// bootstrap lock: concurrent `CREATE TABLE IF NOT EXISTS` from replicas
/// booting together can otherwise race on the catalog and fail boot.
pub const RETRIEVAL_RUNS_DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);
CREATE TABLE IF NOT EXISTS provenance.retrieval_runs (
  run_id      uuid        PRIMARY KEY,
  owner_sub   text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);";

/// Create `provenance.retrieval_runs` (idempotent; `provenance` already exists
/// once `platform_tables::migrate` has run).
pub async fn migrate(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client.batch_execute(RETRIEVAL_RUNS_DDL).await?;
    Ok(())
}

/// Roles that may read any run's output (same set as tenant blobs).
fn is_privileged(identity: &Identity) -> bool {
    matches!(identity.role.as_str(), "local" | "admin" | "super_admin")
}

/// The run id segment of a key under the retrieval prefix:
/// `<prefix>/<run_id>/result.jsonl` → `<run_id>`.
pub fn retrieval_run_of<'a>(key: &'a str, retrieval_prefix: &str) -> Option<&'a str> {
    let pfx = retrieval_prefix.trim_matches('/');
    if pfx.is_empty() {
        return None;
    }
    let rest = key.strip_prefix(pfx)?.strip_prefix('/')?;
    let run = rest.split('/').next()?;
    (!run.is_empty()).then_some(run)
}

/// Record `owner` for `run_id` before anything is materialized, so a result
/// never exists without an owner.
pub async fn record_owner(pool: &Pool, run_id: &str, owner: &str) -> ApiResult<()> {
    let uuid = Uuid::parse_str(run_id)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("retrieval run id: {e}")))?;
    let client = pool.get().await?;
    client
        .execute(
            "INSERT INTO provenance.retrieval_runs (run_id, owner_sub) VALUES ($1, $2) \
             ON CONFLICT (run_id) DO NOTHING",
            &[&uuid, &owner],
        )
        .await?;
    Ok(())
}

async fn owner_of(pool: &Pool, run_id: &str) -> ApiResult<Option<String>> {
    let Ok(uuid) = Uuid::parse_str(run_id) else {
        return Ok(None);
    };
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT owner_sub FROM provenance.retrieval_runs WHERE run_id = $1",
            &[&uuid],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, String>(0)))
}

/// May `caller` read the object at `key`? Applies both private-object rules:
/// tenant blobs (`tenants/<sub>/…`) and materialized retrievals
/// (`<retrieval_prefix>/<run_id>/…`). Every other key is unrestricted here.
/// An anonymous caller may read neither kind.
pub async fn may_read_key(
    pool: &Pool,
    caller: Option<&Identity>,
    key: &str,
    retrieval_prefix: &str,
) -> ApiResult<bool> {
    if let Some(owner) = crate::handlers::me_blobs::tenant_owner_of(key) {
        return Ok(caller.is_some_and(|id| crate::handlers::me_blobs::may_read_tenant(id, owner)));
    }
    if crate::handlers::blobs::is_hidden_key(key, retrieval_prefix) {
        let Some(id) = caller else {
            return Ok(false);
        };
        if is_privileged(id) {
            return Ok(true);
        }
        let Some(run) = retrieval_run_of(key, retrieval_prefix) else {
            return Ok(false);
        };
        // A run with no ownership row predates this check (or was produced
        // anonymously): only privileged roles may read it.
        return Ok(owner_of(pool, run).await?.is_some_and(|o| o == id.sub));
    }
    Ok(true)
}

pub async fn post_retrieve(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let (plan, output_format) = parse_request(&body)?;
    let run_id = Uuid::new_v4().to_string();
    let caller = identity.as_ref().map(|Extension(id)| id);
    let result = replayer::replay(&plan, &st, &output_format, &run_id, caller).await?;
    Ok(Json(
        serde_json::to_value(&result)
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("serializing result: {e}")))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// LUMID-005: the error should name the supported enum, not just echo
    /// the caller's bad value back.
    #[test]
    fn unknown_output_format_names_supported_values() {
        let err = parse_request(&json!({"sql": "select 1", "output_format": "parquet"}))
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("parquet"), "{msg}");
        assert!(msg.contains("jsonl"), "{msg}");
        assert!(msg.contains("csv"), "{msg}");
        assert!(msg.contains("raw"), "{msg}");
    }

    fn ident(sub: &str, role: &str) -> Identity {
        Identity {
            sub: sub.into(),
            role: role.into(),
            email: None,
            active: true,
            scopes: vec![],
        }
    }

    #[test]
    fn retrieval_ddl_takes_the_platform_bootstrap_lock() {
        let lock = format!(
            "SELECT pg_advisory_xact_lock({});",
            crate::platform_tables::MIGRATION_LOCK_KEY
        );
        assert!(RETRIEVAL_RUNS_DDL.starts_with(&lock));
    }

    #[test]
    fn retrieval_run_of_extracts_the_run_segment() {
        assert_eq!(
            retrieval_run_of("retrievals/abc-1/result.jsonl", "retrievals"),
            Some("abc-1")
        );
        assert_eq!(
            retrieval_run_of("retrievals/abc-1", "/retrievals/"),
            Some("abc-1")
        );
        assert_eq!(retrieval_run_of("retrievals/", "retrievals"), None);
        assert_eq!(
            retrieval_run_of("retrievals-archive/x/y", "retrievals"),
            None
        );
        assert_eq!(retrieval_run_of("retrievals/x", ""), None);
    }

    #[test]
    fn privileged_roles_match_tenant_blob_rule() {
        assert!(is_privileged(&ident("x", "local")));
        assert!(is_privileged(&ident("x", "admin")));
        assert!(is_privileged(&ident("x", "super_admin")));
        assert!(!is_privileged(&ident("x", "user")));
        assert!(!is_privileged(&ident("x", "job")));
    }

    #[test]
    fn known_output_formats_still_accepted() {
        for fmt in ["jsonl", "csv", "raw"] {
            let (_, out) =
                parse_request(&json!({"sql": "select 1", "output_format": fmt})).unwrap();
            assert_eq!(out.format_name(), fmt);
        }
    }
}
