//! Hub-to-hub sync: the jobs registry, the catalog mirror, and small tenant
//! blobs move between the federated data hubs (`home`, `nus`, …).
//!
//! **Driven from one side.** The entrance hub (`home`) reaches its peers; a
//! peer (`nus`) cannot reach home. So home does both directions:
//!
//! * push — collect home-owned rows locally, `POST {peer}/sync/inbox`;
//! * pull — `GET {peer}/sync/outbox?scope=registry&since=<cursor>`, apply the
//!   returned batches locally through the same apply path as the inbox.
//!
//! Both directions move the same [`registry::RegistryBatch`] format, dedup on
//! the same `(peer, batch_id)` ledger as the data-push plane, and advance
//! their cursor only after the batch is applied (cursor-after-ack).
//!
//! **Scopes.** `registry` = every `jobs.*` relation except the local-only
//! `jobs.job_tokens` (see [`registry::REGISTRY_TABLES`]), including
//! `jobs.catalog_mirror`, which each hub fills with its OWN tables
//! ([`catalog`]). `blobs` = tenant blobs up to `LUMID_HUB_SMALL_MAX`, copied
//! when missing, with their `raw.tenant_blobs` index row ([`blobs`]).
//! `provenance.*` and user tables never move on this plane.
//!
//! **Auth.** Only a local key labelled exactly `sync:<peer hub id>` reaches
//! the hub endpoints, and with the jobs plane on every `sync:*` key is
//! confined to `/sync/*` ([`confine_sync_keys`]) — a peer key carries
//! metadata, never a user's data-plane access.

pub mod blobs;
pub mod catalog;
pub mod registry;
pub mod sched;

use axum::extract::{Query, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::config::Settings;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

use registry::{RegistryAck, RegistryBatch};

/// Boot DDL: bookkeeping + the columns/indexes sync needs on the registry.
/// ONE batch under the platform bootstrap lock; relations and columns are
/// created only when absent (`to_regclass`, `pg_attribute`), so an up-to-date
/// DB runs no DDL. The reader revoke is appended in the same batch by
/// [`boot_sql`].
pub const HUB_DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);

DO $hub$
BEGIN
  IF to_regnamespace('sync') IS NULL THEN
    CREATE SCHEMA sync;
  END IF;
END
$hub$;

DO $hub$
BEGIN
  IF to_regclass('sync.hub_state') IS NULL THEN
    CREATE TABLE sync.hub_state (
      peer             text        NOT NULL,
      scope            text        NOT NULL,
      direction        text        NOT NULL,
      cursor           text,
      cursor_at        timestamptz,
      last_attempt_at  timestamptz,
      last_success_at  timestamptz,
      last_error_at    timestamptz,
      last_error       text,
      rows_total       bigint      NOT NULL DEFAULT 0,
      PRIMARY KEY (peer, scope, direction)
    );
  END IF;
END
$hub$;

DO $hub$
DECLARE r record;
BEGIN
  -- Uniform sync columns: home_hub (the one writer; NULL = written here
  -- before sync existed), updated_at (the version), deleted_at (tombstone).
  FOR r IN SELECT * FROM (VALUES
      ('jobs.source_grants',  'deleted_at', 'timestamptz'),
      ('jobs.job_sources',    'deleted_at', 'timestamptz'),
      ('jobs.plans',          'home_hub',   'text'),
      ('jobs.plans',          'updated_at', 'timestamptz NOT NULL DEFAULT now()'),
      ('jobs.plans',          'deleted_at', 'timestamptz'),
      ('jobs.quotas',         'home_hub',   'text'),
      ('jobs.quotas',         'deleted_at', 'timestamptz'),
      ('jobs.catalog_mirror', 'home_hub',   'text'),
      ('jobs.job_events',     'origin_id',  'bigint'),
      ('raw.tenant_blobs',    'home_hub',   'text')) v(rel, col, typ)
  LOOP
    IF to_regclass(r.rel) IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM pg_attribute
          WHERE attrelid = to_regclass(r.rel) AND attname = r.col AND NOT attisdropped) THEN
      EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s', r.rel, r.col, r.typ);
    END IF;
  END LOOP;

  -- Outbox paging reads `version > cursor` per table: index the version.
  FOR r IN SELECT * FROM (VALUES
      ('jobs.hubs',             'jobs', 'hubs_by_updated',             'updated_at'),
      ('jobs.sources',          'jobs', 'sources_by_updated',          'updated_at'),
      ('jobs.source_grants',    'jobs', 'source_grants_by_updated',    'updated_at'),
      ('jobs.source_locations', 'jobs', 'source_locations_by_updated', 'updated_at'),
      ('jobs.site_routes',      'jobs', 'site_routes_by_updated',      'updated_at'),
      ('jobs.jobs',             'jobs', 'jobs_by_updated',             'updated_at'),
      ('jobs.job_sources',      'jobs', 'job_sources_by_updated',      'updated_at'),
      ('jobs.job_events',       'jobs', 'job_events_by_at',            'at'),
      ('jobs.plans',            'jobs', 'plans_by_updated',            'updated_at'),
      ('jobs.quotas',           'jobs', 'quotas_by_updated',           'updated_at'),
      ('jobs.catalog_mirror',   'jobs', 'catalog_mirror_by_updated',   'updated_at'),
      ('raw.tenant_blobs',      'raw',  'tenant_blobs_by_created',     'created_at')) v(rel, nsp, idx, col)
  LOOP
    IF to_regclass(r.rel) IS NOT NULL AND to_regclass(r.nsp || '.' || r.idx) IS NULL THEN
      EXECUTE format('CREATE INDEX %I ON %s (%I)', r.idx, r.rel, r.col);
    END IF;
  END LOOP;

  -- A peer's events arrive with their origin id; dedup on (owner, origin).
  IF to_regclass('jobs.job_events') IS NOT NULL
     AND to_regclass('jobs.job_events_by_origin') IS NULL THEN
    CREATE UNIQUE INDEX job_events_by_origin ON jobs.job_events (home_hub, origin_id)
      WHERE origin_id IS NOT NULL;
  END IF;
END
$hub$;";

/// `REVOKE` the `sync` schema from the reader role: `hub_state` holds peer
/// errors and cursors, and `ALTER DEFAULT PRIVILEGES … GRANT SELECT ON TABLES`
/// would otherwise hand every new table to caller-shaped `/retrieve` SQL.
pub fn revoke_from_reader_sql(reader_role: &str) -> Option<String> {
    let role = reader_role.trim();
    if role.is_empty() {
        return None;
    }
    let ident = format!("\"{}\"", role.replace('"', "\"\""));
    let literal = role.replace('\'', "''");
    Some(format!(
        "DO $revoke$ BEGIN \
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{literal}') \
              AND to_regnamespace('sync') IS NOT NULL THEN \
             REVOKE ALL ON ALL TABLES IN SCHEMA sync FROM {ident}; \
             REVOKE ALL ON SCHEMA sync FROM {ident}; \
           END IF; \
         END $revoke$;"
    ))
}

/// DDL + reader revoke as ONE simple-query batch (one lock, one transaction):
/// a separate revoke batch races concurrent boots ("tuple concurrently updated").
pub fn boot_sql(reader_role: &str) -> String {
    match revoke_from_reader_sql(reader_role) {
        Some(revoke) => format!("{HUB_DDL}\n\n{revoke}"),
        None => HUB_DDL.to_string(),
    }
}

pub async fn migrate(pool: &Pool, reader_role: &str) -> anyhow::Result<()> {
    let client = pool.get().await?;
    client
        .batch_execute(&boot_sql(reader_role))
        .await
        .map_err(|e| anyhow::anyhow!("hub sync schema: {e}"))?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────── peers / auth

/// A hub this hub drives (`LUMID_HUB_SYNC_PEERS`).
#[derive(Clone, PartialEq, Eq)]
pub struct HubPeer {
    pub hub_id: String,
    pub base_url: String,
    /// Bearer for the peer's `/sync/*`: a key on the peer labelled
    /// `sync:<this hub id>`. Read from `key_env` at boot; never logged.
    pub token: String,
}

// Hand-written so a `{:?}` can never print the bearer.
impl std::fmt::Debug for HubPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubPeer")
            .field("hub_id", &self.hub_id)
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Deserialize)]
struct HubPeerSpec {
    hub_id: String,
    base_url: String,
    #[serde(default)]
    key_env: Option<String>,
}

fn valid_hub_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Parse `LUMID_HUB_SYNC_PEERS`. `read_env` resolves a FULL env var name
/// (tests inject a fake). Errors are config errors: boot fails loudly.
pub fn parse_hub_peers(
    raw: &str,
    self_hub: &str,
    read_env: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<HubPeer>, String> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let specs: Vec<HubPeerSpec> =
        serde_json::from_str(raw).map_err(|e| format!("LUMID_HUB_SYNC_PEERS: {e}"))?;
    let mut out: Vec<HubPeer> = Vec::new();
    for s in specs {
        let id = s.hub_id.trim();
        if !valid_hub_id(id) {
            return Err(format!("LUMID_HUB_SYNC_PEERS: bad hub_id {id:?}"));
        }
        if id == self_hub {
            return Err(format!("LUMID_HUB_SYNC_PEERS: {id} is this hub"));
        }
        if out.iter().any(|p| p.hub_id == id) {
            return Err(format!("LUMID_HUB_SYNC_PEERS: duplicate hub_id {id}"));
        }
        let url = s.base_url.trim().trim_end_matches('/');
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(format!(
                "LUMID_HUB_SYNC_PEERS: {id}: base_url must be http(s)"
            ));
        }
        let key_env = s.key_env.unwrap_or_else(|| {
            format!(
                "LUMID_SYNC_KEY_{}",
                id.to_uppercase()
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>()
            )
        });
        let token = read_env(&key_env).unwrap_or_default();
        if token.trim().is_empty() {
            return Err(format!("LUMID_HUB_SYNC_PEERS: {id}: {key_env} is unset"));
        }
        out.push(HubPeer {
            hub_id: id.to_string(),
            base_url: url.to_string(),
            token,
        });
    }
    Ok(out)
}

/// Hub ids treated as hub peers here: `LUMID_HUB_PEER_IDS` ∪ the ids in
/// `LUMID_HUB_SYNC_PEERS` (parsed loosely — the strict parse runs at boot).
pub fn hub_peer_ids(settings: &Settings) -> Vec<String> {
    let mut ids = settings.hub_peer_ids.clone();
    if let Ok(Value::Array(a)) = serde_json::from_str::<Value>(&settings.hub_sync_peers_json) {
        for p in a {
            if let Some(id) = p.get("hub_id").and_then(Value::as_str) {
                if !ids.iter().any(|x| x == id) {
                    ids.push(id.to_string());
                }
            }
        }
    }
    ids
}

/// Pure: the peer hub id of a `sync:<id>` local key, if this hub accepts it.
pub fn hub_peer_of(allowed: &[String], self_hub: &str, role: &str, sub: &str) -> Option<String> {
    if role != "local" {
        return None;
    }
    let id = sub.strip_prefix("local:")?.strip_prefix("sync:")?;
    if !valid_hub_id(id) || id == self_hub {
        return None;
    }
    if !allowed.is_empty() && !allowed.iter().any(|a| a == id) {
        return None;
    }
    Some(id.to_string())
}

/// Gate the hub endpoints: exactly a local key labelled `sync:<peer>`.
pub fn require_hub_peer(settings: &Settings, identity: &Identity) -> ApiResult<String> {
    hub_peer_of(
        &hub_peer_ids(settings),
        &settings.hub_id,
        &identity.role,
        &identity.sub,
    )
    .ok_or_else(|| {
        ApiError::Forbidden("hub peer credential required (local key labelled `sync:<hub>`)".into())
    })
}

/// Middleware (inside the gate): with the jobs plane on, a `sync:*` local key
/// may call `/sync/*` only. Such keys otherwise carry the full `local` role
/// (admin everywhere), which a hub peer — or any push peer — never needs.
pub async fn confine_sync_keys(State(st): State<AppState>, req: Request, next: Next) -> Response {
    if st.settings.jobs_enabled && st.settings.hub_sync_confine {
        if let Some(id) = req.extensions().get::<Identity>() {
            if is_sync_key(id) && !req.uri().path().starts_with("/sync/") {
                return ApiError::Forbidden("sync keys may only call /sync/*".into())
                    .into_response();
            }
        }
    }
    next.run(req).await
}

fn is_sync_key(id: &Identity) -> bool {
    id.role == "local" && id.sub.starts_with("local:sync:")
}

// ─────────────────────────────────────────────────────────────── bookkeeping

/// Record one attempt in `sync.hub_state`. `Ok(rows)` stamps success (and the
/// cursor, when given); `Err` stamps the error and leaves the cursor alone.
pub async fn record(
    pool: &Pool,
    peer: &str,
    scope: &str,
    direction: &str,
    outcome: Result<i64, &str>,
    cursor: Option<(&str, Option<DateTime<Utc>>)>,
) {
    let res = async {
        let c = pool.get().await?;
        match outcome {
            Ok(rows) => {
                let (cur, cur_at) = match cursor {
                    Some((c, at)) => (Some(c.to_string()), at),
                    None => (None, None),
                };
                c.execute(
                    "INSERT INTO sync.hub_state AS s
                       (peer, scope, direction, cursor, cursor_at, last_attempt_at, last_success_at, rows_total)
                     VALUES ($1, $2, $3, $4, $5, now(), now(), $6)
                     ON CONFLICT (peer, scope, direction) DO UPDATE SET
                       cursor = COALESCE(EXCLUDED.cursor, s.cursor),
                       cursor_at = COALESCE(EXCLUDED.cursor_at, s.cursor_at),
                       last_attempt_at = now(), last_success_at = now(),
                       rows_total = s.rows_total + EXCLUDED.rows_total",
                    &[&peer, &scope, &direction, &cur, &cur_at, &rows],
                )
                .await?;
            }
            Err(msg) => {
                let msg: String = msg.chars().take(500).collect();
                c.execute(
                    "INSERT INTO sync.hub_state AS s
                       (peer, scope, direction, last_attempt_at, last_error_at, last_error)
                     VALUES ($1, $2, $3, now(), now(), $4)
                     ON CONFLICT (peer, scope, direction) DO UPDATE SET
                       last_attempt_at = now(), last_error_at = now(), last_error = EXCLUDED.last_error",
                    &[&peer, &scope, &direction, &msg],
                )
                .await?;
            }
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(e) = res {
        tracing::warn!(
            peer,
            scope,
            direction,
            "hub sync: state write failed: {e:#}"
        );
    }
}

/// The stored cursor for `(peer, scope, direction)` (raw JSON text).
pub async fn load_cursor(
    pool: &Pool,
    peer: &str,
    scope: &str,
    direction: &str,
) -> ApiResult<Option<String>> {
    let c = pool.get().await?;
    Ok(c.query_opt(
        "SELECT cursor FROM sync.hub_state WHERE peer = $1 AND scope = $2 AND direction = $3",
        &[&peer, &scope, &direction],
    )
    .await?
    .and_then(|r| r.get::<_, Option<String>>(0)))
}

// ─────────────────────────────────────────────────────────────── routes

/// Hub routes, merged when `enable_sync` and the jobs plane are both on.
pub fn routes(small_max: u64) -> Router<AppState> {
    Router::new()
        .route("/sync/inbox", post(post_inbox))
        .route("/sync/outbox", get(get_outbox))
        .route("/sync/status", get(get_status))
        .route("/sync/blobs/have", post(blobs::post_have))
        .merge(
            Router::new()
                .route(
                    "/sync/blobs/:owner/:sha",
                    put(blobs::put_blob).get(blobs::get_blob),
                )
                .layer(axum::extract::DefaultBodyLimit::max(
                    (small_max as usize).saturating_add(1 << 20),
                )),
        )
}

/// `POST /sync/inbox` — apply one registry batch pushed by a peer hub.
pub async fn post_inbox(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(batch): Json<RegistryBatch>,
) -> ApiResult<Json<RegistryAck>> {
    let peer = require_hub_peer(&st.settings, &identity)?;
    match registry::apply_batch(&st, &peer, &batch).await {
        Ok(ack) => {
            let n = ack.inserted + ack.updated;
            if ack.rejected > 0 {
                let msg = format!(
                    "{}: {} row(s) rejected: {}",
                    batch.table,
                    ack.rejected,
                    ack.first_error.as_deref().unwrap_or("")
                );
                record(&st.pool, &peer, "registry", "inbound", Err(&msg), None).await;
            } else {
                record(&st.pool, &peer, "registry", "inbound", Ok(n), None).await;
            }
            Ok(Json(ack))
        }
        Err(e) => {
            record(
                &st.pool,
                &peer,
                "registry",
                "inbound",
                Err(&e.to_string()),
                None,
            )
            .await;
            Err(e)
        }
    }
}

#[derive(Deserialize)]
pub struct OutboxQuery {
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default = "default_scope")]
    pub scope: String,
    #[serde(default)]
    pub limit: Option<i64>,
}

fn default_scope() -> String {
    "registry".into()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OutboxResponse {
    pub scope: String,
    pub hub: String,
    #[serde(default)]
    pub batches: Vec<RegistryBatch>,
    #[serde(default)]
    pub blobs: Vec<blobs::BlobIndexRow>,
    /// Opaque: pass back as `since` once the batches are applied.
    pub cursor: String,
    /// A page filled: call again with `cursor`.
    pub more: bool,
}

/// `GET /sync/outbox?scope=registry|blobs&since=<cursor>` — this hub's OWN
/// rows past the cursor, for the driving peer to pull.
pub async fn get_outbox(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Query(q): Query<OutboxQuery>,
) -> ApiResult<Json<OutboxResponse>> {
    let peer = require_hub_peer(&st.settings, &identity)?;
    let limit = q
        .limit
        .unwrap_or(st.settings.sync_batch_rows as i64)
        .clamp(1, 5000);
    let cursor = registry::parse_cursor(q.since.as_deref())?;
    let resp = match q.scope.as_str() {
        "registry" => {
            let (batches, next, more) = registry::collect(&st, &cursor, limit).await?;
            let n: i64 = batches.iter().map(|b| b.records.len() as i64).sum();
            record(&st.pool, &peer, "registry", "outbound", Ok(n), None).await;
            OutboxResponse {
                scope: q.scope,
                hub: st.settings.hub_id.clone(),
                batches,
                blobs: Vec::new(),
                cursor: registry::encode_cursor(&next),
                more,
            }
        }
        "blobs" => {
            let (rows, next, more) = blobs::collect_index(&st, &cursor, limit).await?;
            record(
                &st.pool,
                &peer,
                "blobs",
                "outbound",
                Ok(rows.len() as i64),
                None,
            )
            .await;
            OutboxResponse {
                scope: q.scope,
                hub: st.settings.hub_id.clone(),
                batches: Vec::new(),
                blobs: rows,
                cursor: registry::encode_cursor(&next),
                more,
            }
        }
        other => return Err(ApiError::BadRequest(format!("unknown scope `{other}`"))),
    };
    Ok(Json(resp))
}

fn age_s(at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<i64> {
    at.map(|t| (now - t).num_seconds().max(0))
}

/// `GET /sync/status` — per peer, per scope and direction: last success /
/// last error / cursor age. `synced_age_s` (the oldest last success across a
/// peer's directions) drives a "NUS hub: synced 20 s ago" badge. Any
/// authenticated caller may read it; error text only for admins/local keys.
pub async fn get_status(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    let privileged = matches!(identity.role.as_str(), "local" | "admin" | "super_admin");
    let c = st.pool.get().await?;
    let rows = c
        .query(
            "SELECT peer, scope, direction, cursor_at, last_attempt_at, last_success_at,
                    last_error_at, last_error, rows_total, now() AS now
               FROM sync.hub_state ORDER BY peer, scope, direction",
            &[],
        )
        .await?;
    let mut peers: Vec<Value> = Vec::new();
    let mut ages: std::collections::HashMap<String, Vec<Option<i64>>> = Default::default();
    for r in &rows {
        let now: DateTime<Utc> = r.get("now");
        let peer: String = r.get("peer");
        let scope: String = r.get("scope");
        let direction: String = r.get("direction");
        let last_success: Option<DateTime<Utc>> = r.get("last_success_at");
        let last_error_at: Option<DateTime<Utc>> = r.get("last_error_at");
        let cursor_at: Option<DateTime<Utc>> = r.get("cursor_at");
        let failing = match (last_error_at, last_success) {
            (Some(e), Some(s)) => e > s,
            (Some(_), None) => true,
            _ => false,
        };
        let mut d = json!({
            "last_attempt_at": r.get::<_, Option<DateTime<Utc>>>("last_attempt_at"),
            "last_success_at": last_success,
            "last_success_age_s": age_s(last_success, now),
            "last_error_at": last_error_at,
            "cursor_at": cursor_at,
            "cursor_age_s": age_s(cursor_at, now),
            "rows_total": r.get::<_, i64>("rows_total"),
            "failing": failing,
        });
        if privileged {
            d["last_error"] = json!(r.get::<_, Option<String>>("last_error"));
        }
        let idx = match peers.iter().position(|p| p["peer"] == peer.as_str()) {
            Some(i) => i,
            None => {
                peers.push(
                    json!({"peer": peer, "scopes": {}, "synced_age_s": null, "failing": false}),
                );
                peers.len() - 1
            }
        };
        let p = &mut peers[idx];
        if p["scopes"].get(&scope).is_none() {
            p["scopes"][&scope] = json!({});
        }
        p["scopes"][&scope][&direction] = d;
        if failing {
            p["failing"] = json!(true);
        }
        ages.entry(peer).or_default().push(age_s(last_success, now));
    }
    // Worst (oldest) success across a peer's directions; any direction that
    // never succeeded keeps the badge at null ("never synced").
    for p in peers.iter_mut() {
        let a = p["peer"].as_str().and_then(|id| ages.get(id));
        p["synced_age_s"] = match a {
            Some(v) if v.iter().all(Option::is_some) => json!(v.iter().flatten().max()),
            _ => Value::Null,
        };
    }
    Ok(Json(json!({
        "hub_id": st.settings.hub_id,
        "interval_s": st.settings.hub_sync_interval_s,
        "peers": peers,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_is_locked_guarded_and_revoke_rides_along() {
        assert!(HUB_DDL.starts_with("SELECT pg_advisory_xact_lock(7815273027795448930);"));
        assert!(!HUB_DDL.contains("::regclass"), "use to_regclass in guards");
        assert!(!HUB_DDL.contains("IF NOT EXISTS"));
        let sql = boot_sql("lumid_reader");
        assert_eq!(sql.matches("pg_advisory_xact_lock").count(), 1);
        assert!(sql.contains("REVOKE ALL ON ALL TABLES IN SCHEMA sync FROM \"lumid_reader\""));
        assert_eq!(boot_sql(" "), HUB_DDL);
        // the token table is never touched by sync DDL
        assert!(!HUB_DDL.contains("job_tokens"));
    }

    #[test]
    fn peers_parse() {
        let env = |k: &str| match k {
            "LUMID_SYNC_KEY_NUS" => Some("tok".to_string()),
            "OTHER" => Some("t2".to_string()),
            _ => None,
        };
        let p = parse_hub_peers(
            r#"[{"hub_id":"nus","base_url":"http://10.0.0.1:5101/"},
                {"hub_id":"office","base_url":"https://o.example","key_env":"OTHER"}]"#,
            "home",
            &env,
        )
        .unwrap();
        assert_eq!(
            p[0],
            HubPeer {
                hub_id: "nus".into(),
                base_url: "http://10.0.0.1:5101".into(),
                token: "tok".into()
            }
        );
        assert_eq!(p[1].token, "t2");
        assert!(
            !format!("{:?}", p[0]).contains("\"tok\""),
            "Debug must redact the bearer"
        );
        assert!(parse_hub_peers("", "home", &env).unwrap().is_empty());
        assert!(
            parse_hub_peers(r#"[{"hub_id":"home","base_url":"http://x"}]"#, "home", &env).is_err()
        );
        assert!(
            parse_hub_peers(r#"[{"hub_id":"x","base_url":"http://x"}]"#, "home", &env)
                .unwrap_err()
                .contains("LUMID_SYNC_KEY_X")
        );
        assert!(
            parse_hub_peers(r#"[{"hub_id":"nus","base_url":"ftp://x"}]"#, "home", &env).is_err()
        );
        assert!(parse_hub_peers("{", "home", &env).is_err());
    }

    #[test]
    fn hub_peer_requires_exact_sync_local_key() {
        assert_eq!(
            hub_peer_of(&[], "nus", "local", "local:sync:home"),
            Some("home".into())
        );
        assert_eq!(hub_peer_of(&[], "nus", "local", "local:dev"), None);
        assert_eq!(
            hub_peer_of(&[], "nus", "sync_peer", "home"),
            None,
            "lumid role is not enough"
        );
        assert_eq!(
            hub_peer_of(&[], "nus", "local", "local:sync:nus"),
            None,
            "not its own peer"
        );
        assert_eq!(hub_peer_of(&[], "nus", "local", "local:sync:a/b"), None);
        let allow = vec!["home".to_string()];
        assert_eq!(
            hub_peer_of(&allow, "nus", "local", "local:sync:home"),
            Some("home".into())
        );
        assert_eq!(
            hub_peer_of(&allow, "nus", "local", "local:sync:findata"),
            None
        );
    }

    #[test]
    fn hub_peer_ids_union() {
        let mut s = Settings::from_env();
        s.hub_peer_ids = vec!["home".into()];
        s.hub_sync_peers_json = r#"[{"hub_id":"nus","base_url":"http://x"}]"#.into();
        assert_eq!(
            hub_peer_ids(&s),
            vec!["home".to_string(), "nus".to_string()]
        );
    }
}
