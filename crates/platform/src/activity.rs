//! Table activity: who read or wrote which table, from which app, workflow and
//! run — so a person can see what a run produced, what a workflow reads and
//! writes, and who keeps a table up to date.
//!
//! Recorded by one middleware (after the auth gate) on the two data paths:
//! `POST /ingest/{schema}/{table}[/stream|/file]` (a write; rows = inserted +
//! updated) and `POST /retrieve` (a read of every relation its SQL names; rows
//! = the result's rowcount). The workflow context rides on optional request
//! headers the LumidOS runner sets — `X-Lumid-App`, `X-Lumid-Loop`,
//! `X-Lumid-Run` — and is a DISPLAY hint, never authorization.
//!
//! Stored as one row per (day, table, kind, caller, app, loop, run) with a
//! request counter, so volume follows distinct runs, not requests.
//!
//! Read back with:
//!   GET /activity/runs/{run_id}              what one run read and wrote
//!   GET /activity/apps/{app}?loop=&days=     what an app (or one workflow) reads and writes
//!   GET /catalog/tables/{schema}/{table}/activity?days=   who writes and reads a table
//! A caller sees their own activity; admins see everyone's. Other people's
//! activity on a table is summarized as a count, without names.

use axum::body::{to_bytes, Body};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, Method};
use axum::middleware::Next;
use axum::response::Response;
use axum::{Extension, Json};
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::retrieve::plan::RetrievalOp;
use crate::state::AppState;

const MAX_REQ_BODY: usize = 256 * 1024;
const MAX_RESP_BODY: usize = 2 * 1024 * 1024;
const HINT_MAX: usize = 128;

const DDL: &str = "\
SELECT pg_advisory_xact_lock(7815273027795448930);
DO $activity$
BEGIN
  IF to_regnamespace('provenance') IS NULL THEN
    CREATE SCHEMA provenance;
  END IF;
  IF to_regclass('provenance.table_activity') IS NULL THEN
    CREATE TABLE provenance.table_activity (
      day         date        NOT NULL,
      schema_name text        NOT NULL,
      table_name  text        NOT NULL,
      kind        text        NOT NULL CHECK (kind IN ('read','write')),
      sub         text        NOT NULL DEFAULT '',
      app         text        NOT NULL DEFAULT '',
      loop        text        NOT NULL DEFAULT '',
      run_id      text        NOT NULL DEFAULT '',
      requests    bigint      NOT NULL DEFAULT 0,
      rows        bigint      NOT NULL DEFAULT 0,
      last_at     timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (day, schema_name, table_name, kind, sub, app, loop, run_id)
    );
    CREATE INDEX table_activity_by_run ON provenance.table_activity (run_id) WHERE run_id <> '';
    CREATE INDEX table_activity_by_app ON provenance.table_activity (app, loop, day) WHERE app <> '';
    CREATE INDEX table_activity_by_table ON provenance.table_activity (schema_name, table_name, day);
  END IF;
END
$activity$;";

/// Create the activity table (one locked batch, reader revoke included — the
/// reader role runs caller SQL through `/retrieve` and must not read who
/// queried what).
pub async fn migrate(pool: &Pool, reader_role: &str) -> anyhow::Result<()> {
    let client = pool.get().await?;
    let role = reader_role.trim();
    let sql = if role.is_empty() {
        DDL.to_string()
    } else {
        let ident = format!("\"{}\"", role.replace('"', "\"\""));
        let literal = role.replace('\'', "''");
        format!(
            "{DDL}\nDO $revoke$ BEGIN \
               IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{literal}') THEN \
                 REVOKE ALL ON provenance.table_activity FROM {ident}; \
               END IF; \
             END $revoke$;"
        )
    };
    client.batch_execute(&sql).await?;
    Ok(())
}

/// The workflow context a request carries (display hints only).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Context {
    pub app: String,
    pub loop_: String,
    pub run_id: String,
}

fn hint(h: &HeaderMap, name: &str) -> String {
    h.get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.chars()
                .filter(|c| !c.is_control())
                .take(HINT_MAX)
                .collect::<String>()
                .trim()
                .to_string()
        })
        .unwrap_or_default()
}

pub fn context_from(h: &HeaderMap) -> Context {
    Context {
        app: hint(h, "x-lumid-app"),
        loop_: hint(h, "x-lumid-loop"),
        run_id: hint(h, "x-lumid-run"),
    }
}

/// `(schema, table)` for an ingest path, else None.
pub fn ingest_target(path: &str) -> Option<(String, String)> {
    let rest = path.strip_prefix("/ingest/")?;
    let mut it = rest.split('/');
    let schema = it.next()?;
    let table = it.next()?;
    let tail = it.next();
    if schema.is_empty() || table.is_empty() || schema == "blob" {
        return None;
    }
    match (tail, it.next()) {
        (None, None) | (Some("stream"), None) | (Some("file"), None) => {
            Some((schema.to_string(), table.to_string()))
        }
        _ => None,
    }
}

/// Every schema-qualified relation the retrieve plan's SQL reads.
fn retrieve_tables(body: &[u8]) -> Vec<(String, String)> {
    let Ok(json) = serde_json::from_slice::<Value>(body) else {
        return vec![];
    };
    let Ok((plan, _)) = crate::handlers::retrieve::parse_request(&json) else {
        return vec![];
    };
    let mut out: Vec<(String, String)> = Vec::new();
    for op in &plan.plan {
        if let RetrievalOp::Sql(sql) = op {
            if let Ok((rels, _ctes)) = crate::jobs::scope::extract_relations(&sql.query) {
                for (schema, name) in rels {
                    if let Some(s) = schema {
                        let t = (s.to_lowercase(), name.to_lowercase());
                        if !out.contains(&t) {
                            out.push(t);
                        }
                    }
                }
            }
        }
    }
    out
}

fn rows_written(v: &Value) -> i64 {
    v.get("inserted").and_then(Value::as_i64).unwrap_or(0)
        + v.get("updated").and_then(Value::as_i64).unwrap_or(0)
}

/// Middleware: record table activity for ingest writes and retrieve reads.
/// Never changes the response, and never fails a request it could not record.
pub async fn record(State(st): State<AppState>, req: Request, next: Next) -> Response {
    if req.method() != Method::POST {
        return next.run(req).await;
    }
    let path = req.uri().path().to_string();
    let write_target = ingest_target(&path);
    let is_retrieve = path == "/retrieve";
    if write_target.is_none() && !is_retrieve {
        return next.run(req).await;
    }
    let Some(sub) = req.extensions().get::<Identity>().map(|i| i.sub.clone()) else {
        return next.run(req).await;
    };
    let ctx = context_from(req.headers());

    // Reads: name the tables from the request before it runs.
    let (req, read_tables) = if is_retrieve {
        let (parts, body) = req.into_parts();
        match to_bytes(body, MAX_REQ_BODY).await {
            Ok(b) => {
                let tables = retrieve_tables(&b);
                (Request::from_parts(parts, Body::from(b)), tables)
            }
            // Too large to buffer for attribution: we already consumed the
            // stream, so answer as the handler would for an oversize body.
            Err(_) => {
                return ApiError::BadRequest("request body too large".into()).into_response_ext()
            }
        }
    } else {
        (req, vec![])
    };

    let resp = next.run(req).await;
    if !resp.status().is_success() {
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let bytes = match to_bytes(body, MAX_RESP_BODY).await {
        Ok(b) => b,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let entries: Vec<(String, String, &'static str, i64)> = match &write_target {
        Some((s, t)) => vec![(s.clone(), t.clone(), "write", rows_written(&v))],
        None => {
            let rows = v.get("rowcount").and_then(Value::as_i64).unwrap_or(0);
            // A multi-table read attributes its rows to each table it named.
            read_tables
                .into_iter()
                .map(|(s, t)| (s, t, "read", rows))
                .collect()
        }
    };
    if !entries.is_empty() {
        let pool = st.pool.clone();
        tokio::spawn(async move {
            for (schema, table, kind, rows) in entries {
                if let Err(e) = upsert(&pool, &schema, &table, kind, &sub, &ctx, rows).await {
                    tracing::debug!("activity: not recorded: {e}");
                }
            }
        });
    }
    Response::from_parts(parts, Body::from(bytes))
}

trait IntoResponseExt {
    fn into_response_ext(self) -> Response;
}
impl IntoResponseExt for ApiError {
    fn into_response_ext(self) -> Response {
        axum::response::IntoResponse::into_response(self)
    }
}

async fn upsert(
    pool: &Pool,
    schema: &str,
    table: &str,
    kind: &str,
    sub: &str,
    ctx: &Context,
    rows: i64,
) -> anyhow::Result<()> {
    let c = pool.get().await?;
    c.execute(
        "INSERT INTO provenance.table_activity
           (day, schema_name, table_name, kind, sub, app, loop, run_id, requests, rows, last_at)
         VALUES (current_date, $1, $2, $3, $4, $5, $6, $7, 1, $8, now())
         ON CONFLICT (day, schema_name, table_name, kind, sub, app, loop, run_id)
         DO UPDATE SET requests = provenance.table_activity.requests + 1,
                       rows = provenance.table_activity.rows + EXCLUDED.rows,
                       last_at = now()",
        &[
            &schema,
            &table,
            &kind,
            &sub,
            &ctx.app,
            &ctx.loop_,
            &ctx.run_id,
            &rows.max(0),
        ],
    )
    .await?;
    Ok(())
}

fn privileged(id: &Identity) -> bool {
    matches!(id.role.as_str(), "admin" | "super_admin" | "local")
}

fn days_of(d: Option<i64>) -> i64 {
    d.unwrap_or(30).clamp(1, 365)
}

/// Group rows into `{wrote: [...], read: [...]}`, newest first.
fn split(rows: &[tokio_postgres::Row], table_key: bool) -> Value {
    let mut wrote = vec![];
    let mut read = vec![];
    for r in rows {
        let kind: String = r.get("kind");
        let last: chrono::DateTime<chrono::Utc> = r.get("last_at");
        let mut o = json!({
            "rows": r.get::<_, i64>("rows"),
            "requests": r.get::<_, i64>("requests"),
            "last_at": last,
        });
        if table_key {
            o["schema"] = json!(r.get::<_, String>("schema_name"));
            o["table"] = json!(r.get::<_, String>("table_name"));
        } else {
            o["app"] = json!(r.get::<_, String>("app"));
            o["loop"] = json!(r.get::<_, String>("loop"));
        }
        if kind == "write" {
            wrote.push(o)
        } else {
            read.push(o)
        }
    }
    json!({"wrote": wrote, "read": read})
}

/// `GET /activity/runs/{run_id}` — what one run read and wrote (its owner, or an admin).
pub async fn run_activity(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let c = st
        .pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let all = privileged(&id);
    let rows = c
        .query(
            "SELECT schema_name, table_name, kind, sum(rows)::bigint AS rows,
                    sum(requests)::bigint AS requests, max(last_at) AS last_at
               FROM provenance.table_activity
              WHERE run_id = $1 AND ($2 OR sub = $3)
              GROUP BY schema_name, table_name, kind
              ORDER BY max(last_at) DESC LIMIT 200",
            &[&run_id, &all, &id.sub],
        )
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let mut v = split(&rows, true);
    v["run_id"] = json!(run_id);
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct AppQ {
    #[serde(default)]
    pub loop_: Option<String>,
    #[serde(default, rename = "loop")]
    pub loop_name: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
}

/// `GET /activity/apps/{app}?loop=&days=` — the tables an app (or one of its
/// workflows) read and wrote: the caller's own runs, or everyone's for an admin.
pub async fn app_activity(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(app): Path<String>,
    Query(q): Query<AppQ>,
) -> ApiResult<Json<Value>> {
    let c = st
        .pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let all = privileged(&id);
    let lp = q.loop_name.or(q.loop_).unwrap_or_default();
    let days = days_of(q.days);
    let rows = c
        .query(
            "SELECT schema_name, table_name, kind, sum(rows)::bigint AS rows,
                    sum(requests)::bigint AS requests, max(last_at) AS last_at
               FROM provenance.table_activity
              WHERE app = $1 AND ($2 = '' OR loop = $2)
                AND day >= current_date - ($3::int)
                AND ($4 OR sub = $5)
              GROUP BY schema_name, table_name, kind
              ORDER BY max(last_at) DESC LIMIT 200",
            &[&app, &lp, &(days as i32), &all, &id.sub],
        )
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let mut v = split(&rows, true);
    v["app"] = json!(app);
    v["loop"] = json!(lp);
    v["days"] = json!(days);
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct DaysQ {
    #[serde(default)]
    pub days: Option<i64>,
}

/// `GET /catalog/tables/{schema}/{table}/activity?days=` — which apps and
/// workflows write and read a table. Names for the caller's own activity
/// (everyone's for an admin); other people's as counts only.
pub async fn table_activity(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    Query(q): Query<DaysQ>,
) -> ApiResult<Json<Value>> {
    let c = st
        .pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let all = privileged(&id);
    let days = days_of(q.days);
    let (s, t) = (schema.to_lowercase(), table.to_lowercase());
    let mine = c
        .query(
            "SELECT app, loop, kind, sum(rows)::bigint AS rows,
                    sum(requests)::bigint AS requests, max(last_at) AS last_at
               FROM provenance.table_activity
              WHERE schema_name = $1 AND table_name = $2
                AND day >= current_date - ($3::int)
                AND ($4 OR sub = $5)
              GROUP BY app, loop, kind
              ORDER BY max(last_at) DESC LIMIT 100",
            &[&s, &t, &(days as i32), &all, &id.sub],
        )
        .await
        .map_err(|e| ApiError::Internal(e.into()))?;
    let others = if all {
        None
    } else {
        let r = c
            .query_one(
                "SELECT count(DISTINCT sub) FILTER (WHERE kind = 'write') AS writers,
                        count(DISTINCT sub) FILTER (WHERE kind = 'read')  AS readers
                   FROM provenance.table_activity
                  WHERE schema_name = $1 AND table_name = $2
                    AND day >= current_date - ($3::int) AND sub <> $4",
                &[&s, &t, &(days as i32), &id.sub],
            )
            .await
            .map_err(|e| ApiError::Internal(e.into()))?;
        Some(json!({"writers": r.get::<_, i64>("writers"), "readers": r.get::<_, i64>("readers")}))
    };
    let mut v = split(&mine, false);
    v["schema"] = json!(s);
    v["table"] = json!(t);
    v["days"] = json!(days);
    if let Some(o) = others {
        v["others"] = o;
    }
    Ok(Json(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_paths() {
        assert_eq!(
            ingest_target("/ingest/instrument/proposals"),
            Some(("instrument".into(), "proposals".into()))
        );
        assert_eq!(
            ingest_target("/ingest/instrument/proposals/stream"),
            Some(("instrument".into(), "proposals".into()))
        );
        assert_eq!(
            ingest_target("/ingest/a/b/file"),
            Some(("a".into(), "b".into()))
        );
        assert_eq!(ingest_target("/ingest/blob"), None);
        assert_eq!(ingest_target("/ingest/a/b/other"), None);
        assert_eq!(ingest_target("/retrieve"), None);
    }

    #[test]
    fn hints_are_trimmed_and_bounded() {
        let mut h = HeaderMap::new();
        h.insert("x-lumid-app", "  venue-link-matcher ".parse().unwrap());
        h.insert("x-lumid-run", "x".repeat(500).parse().unwrap());
        let c = context_from(&h);
        assert_eq!(c.app, "venue-link-matcher");
        assert_eq!(c.run_id.len(), HINT_MAX);
        assert_eq!(c.loop_, "");
    }

    #[test]
    fn retrieve_names_only_qualified_tables() {
        let b = br#"{"sql": "select a.x from findata.markets a join prices p on true join instrument.links l on true"}"#;
        let t = retrieve_tables(b);
        assert!(t.contains(&("findata".into(), "markets".into())), "{t:?}");
        assert!(t.contains(&("instrument".into(), "links".into())), "{t:?}");
        assert_eq!(t.len(), 2, "{t:?}");
    }

    #[test]
    fn write_rows_add_inserted_and_updated() {
        assert_eq!(rows_written(&json!({"inserted": 3, "updated": 2})), 5);
        assert_eq!(rows_written(&json!({})), 0);
    }
}
