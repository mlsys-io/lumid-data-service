//! Generic data-push plane: an **inbox** (`POST /sync/apply/:schema/:table`)
//! that applies a provenance-carrying batch idempotently, plus an optional
//! **push helper** (`sync::push`) for producers that drain a local table.
//!
//! Fan-in shape: N producers → one target inbox. One-off / repeatable, not a
//! live CDC stream. The platform names no app — producers/targets are config.
//!
//! Reuse: the inbox applies through the normal ingest pipeline
//! (`ingest::core::ingest_records` → `write::engine::copy_and_merge`), so it
//! inherits validation, the idempotent `ON CONFLICT … WHERE IS DISTINCT` merge,
//! and provenance stamping for free. Exactly-once-effective = at-least-once
//! delivery + (`sync.inbox_ledger` dedup AND the idempotent merge).

pub mod inbox;
pub mod push;

use axum::routing::{get, post};
use axum::Router;
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Identity;
use crate::config::Settings;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// Idempotent DDL for the sync bookkeeping tables. Applied at boot (when sync is
/// enabled) by `migrate`. Column names avoid the reserved words `schema`/`table`.
pub const SYNC_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS sync;
CREATE TABLE IF NOT EXISTS sync.inbox_ledger (
  peer          text        NOT NULL,
  batch_id      uuid        NOT NULL,
  source_run_id uuid,
  target_schema text,
  target_table  text,
  inserted      bigint      NOT NULL DEFAULT 0,
  updated       bigint      NOT NULL DEFAULT 0,
  n_rows        bigint      NOT NULL DEFAULT 0,
  applied_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (peer, batch_id)
);
CREATE TABLE IF NOT EXISTS sync.push_cursor (
  target_url    text        NOT NULL,
  schema_name   text        NOT NULL,
  table_name    text        NOT NULL,
  watermark     text,
  watermark_key text,
  rows_pushed   bigint      NOT NULL DEFAULT 0,
  last_result   text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target_url, schema_name, table_name)
);
ALTER TABLE sync.push_cursor ADD COLUMN IF NOT EXISTS watermark_key text;";

/// Create the `sync` schema + bookkeeping tables (idempotent).
pub async fn migrate(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client.batch_execute(SYNC_DDL).await?;
    Ok(())
}

/// Gated routes mounted when `ServeParts.enable_sync` is set. Merged into the
/// platform's gated router (auth + rate limit) like the LLM proxy.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sync/apply/:schema/:table", post(inbox::post_sync_apply))
        .route("/admin/sync/push", post(push::admin_push))
        .route("/admin/sync/status", get(push::admin_status))
}

/// Provenance preamble shipped with a batch: verbatim row objects (as produced
/// by `to_jsonb(<row>)` on the source) for the FK chain the data rows need.
/// Upserted in order `api_sources → endpoints → runs` before the data.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Preamble {
    #[serde(default)]
    pub api_sources: Vec<serde_json::Value>,
    #[serde(default)]
    pub endpoints: Vec<serde_json::Value>,
    #[serde(default)]
    pub runs: Vec<serde_json::Value>,
}

/// Body of `POST /sync/apply/:schema/:table`. The batch is lineage-homogeneous:
/// one `(source, source_endpoint, source_run_id)` for all `records` (the push
/// helper groups by run so this holds; direct producers must do likewise).
#[derive(Debug, Deserialize)]
pub struct SyncApplyBody {
    pub batch_id: Uuid,
    pub source: String,
    pub source_endpoint: String,
    pub source_run_id: Uuid,
    #[serde(default)]
    pub provenance: Preamble,
    pub records: Vec<serde_json::Value>,
}

/// Durable ACK returned by the inbox.
#[derive(Debug, Serialize)]
pub struct SyncAck {
    pub batch_id: Uuid,
    pub inserted: i64,
    pub updated: i64,
    /// Records the target rejected at validation (a partial apply). 0 on a clean
    /// batch. A fully-rejected batch is an error (the inbox never ACKs it).
    pub failed: i64,
    pub duplicate: bool,
}

/// Schemas a sync peer may never write to via `/sync/apply` (and the push helper
/// never drains): the sync bookkeeping itself, the provenance registry (reached
/// only via the preamble, never as data), and the system catalogs. Without this
/// a peer's blast radius would be the whole DB — the normal `/ingest` path's
/// `gate_target` ACL is intentionally bypassed by the inbox, so this is its
/// replacement guard.
pub fn is_denied_schema(schema: &str) -> bool {
    matches!(schema, "sync" | "provenance" | "information_schema")
        || schema.starts_with("pg_")
}

/// Gate `/sync/apply` to authenticated sync peers; returns the peer id.
///
/// Accepts a Lumid identity with role `sync_peer`, or a local key whose label
/// is in `LUMID_SYNC_PEER_LABELS` (when set) or — when that list is empty —
/// whose label starts with `sync:`. Normal `/ingest` callers can't reach this
/// route, so lineage can't be forged. The peer id is the label minus a `sync:`
/// prefix (or the Lumid `sub`).
pub fn require_sync_peer(settings: &Settings, identity: &Identity) -> ApiResult<String> {
    peer_from(&settings.sync_peer_labels, &identity.role, &identity.sub).ok_or_else(|| {
        ApiError::Forbidden(
            "sync-peer credential required (local key labelled `sync:<peer>`, or role `sync_peer`)"
                .into(),
        )
    })
}

/// Pure peer-resolution: `Some(peer_id)` if this identity may push, else `None`.
/// Lumid role `sync_peer` → the `sub`. Local key (`sub = "local:<label>"`) →
/// allowed when `label` is in `labels` (or, when `labels` is empty, when it
/// starts with `sync:`); peer id is `label` minus a `sync:` prefix.
fn peer_from(labels: &[String], role: &str, sub: &str) -> Option<String> {
    if role == "sync_peer" {
        return Some(sub.to_string());
    }
    if role == "local" {
        if let Some(label) = sub.strip_prefix("local:") {
            let allowed = if labels.is_empty() {
                label.starts_with("sync:")
            } else {
                labels.iter().any(|l| l == label)
            };
            if allowed {
                return Some(label.strip_prefix("sync:").unwrap_or(label).to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::peer_from;

    fn sv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn local_sync_label_allowed_when_no_allowlist() {
        // Empty allowlist → any `sync:` label is a peer; id strips the prefix.
        assert_eq!(peer_from(&[], "local", "local:sync:findata"), Some("findata".into()));
    }

    #[test]
    fn local_non_sync_label_rejected_without_allowlist() {
        assert_eq!(peer_from(&[], "local", "local:dev"), None);
    }

    #[test]
    fn allowlist_is_exact_label_match() {
        let labels = sv(&["sync:findata", "worker7"]);
        assert_eq!(peer_from(&labels, "local", "local:sync:findata"), Some("findata".into()));
        // a label not in the list is rejected even if it starts with sync:
        assert_eq!(peer_from(&labels, "local", "local:sync:other"), None);
        // non-`sync:` label in the allowlist is honored, peer id == label
        assert_eq!(peer_from(&labels, "local", "local:worker7"), Some("worker7".into()));
    }

    #[test]
    fn lumid_sync_peer_role() {
        assert_eq!(peer_from(&[], "sync_peer", "agent-42"), Some("agent-42".into()));
    }

    #[test]
    fn ordinary_roles_rejected() {
        assert_eq!(peer_from(&[], "user", "user:alice"), None);
        assert_eq!(peer_from(&[], "super_admin", "admin"), None);
    }
}
