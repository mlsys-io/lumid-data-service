//! Optional push helper (Mode 1): drain a local table to a target's inbox.
//!
//! Pages rows since a durable per-(target,table) watermark cursor, groups each
//! page by `source_run_id` so every pushed batch is lineage-homogeneous, ships
//! each group (with its provenance preamble) to `{target}/sync/apply/...`, and
//! advances the cursor only after a durable ACK. Runs to completion and returns;
//! re-running resumes from the cursor and is idempotent at the inbox.
//!
//! Producers without a local table (stateless workers) skip this entirely and
//! POST batches to the inbox directly.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::extract::State;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_postgres::Client;
use uuid::Uuid;

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::handlers::ingest::require_admin;
use crate::state::AppState;

/// SQL-identifier guard for interpolated schema/table/column names.
fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn dberr(e: tokio_postgres::Error) -> ApiError {
    ApiError::Internal(anyhow::anyhow!("{e}"))
}

fn default_wm() -> String {
    "ingest_ts".to_string()
}

/// Strip the server-stamped columns from a record body. The inbox's ingest
/// plane sets these server-side and **rejects** any record that carries one
/// (422 "field '<col>' is set server-side"); they ride in the batch envelope /
/// are re-derived on ingest. Uses the single-source-of-truth
/// [`crate::validation::SERVER_STAMPED_COLS`] so the strip set can never drift
/// from the validator's reject set (notably it includes `id` — every table with
/// an `id bigint` PK would otherwise have every record rejected — and excludes
/// `raw`, which the validator accepts, so the synced copy keeps its `raw`).
pub(crate) fn strip_server_side_cols(rec: &Value) -> Value {
    use crate::validation::SERVER_STAMPED_COLS;
    match rec {
        Value::Object(map) => {
            let mut m = map.clone();
            m.retain(|k, _| !SERVER_STAMPED_COLS.contains(k.as_str()));
            Value::Object(m)
        }
        other => other.clone(),
    }
}

#[derive(Deserialize)]
pub struct PushReq {
    pub schema: String,
    pub table: String,
    /// Timestamp-typed watermark column to page + checkpoint on. Default `ingest_ts`.
    #[serde(default = "default_wm")]
    pub watermark_col: String,
}

#[derive(Serialize)]
pub struct PushSummary {
    pub schema: String,
    pub table: String,
    pub batches: u64,
    pub rows_pushed: i64,
    pub status: String,
    pub last_error: Option<String>,
}

/// `POST /admin/sync/push` — drain `schema.table` to the configured target.
pub async fn admin_push(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(req): Json<PushReq>,
) -> ApiResult<Json<PushSummary>> {
    require_admin(&identity)?;
    Ok(Json(run_push(&st, &req.schema, &req.table, &req.watermark_col).await?))
}

/// `GET /admin/sync/status` — per-table push cursors.
pub async fn admin_status(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    let client = st.pool.get().await?;
    let rows = client
        .query(
            "SELECT target_url, schema_name, table_name, watermark, rows_pushed, last_result, updated_at \
             FROM sync.push_cursor ORDER BY updated_at DESC",
            &[],
        )
        .await
        .map_err(dberr)?;
    let cursors: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "target_url": r.get::<_, String>("target_url"),
                "schema": r.get::<_, String>("schema_name"),
                "table": r.get::<_, String>("table_name"),
                "watermark": r.get::<_, Option<String>>("watermark"),
                "rows_pushed": r.get::<_, i64>("rows_pushed"),
                "last_result": r.get::<_, Option<String>>("last_result"),
                "updated_at": r.get::<_, DateTime<Utc>>("updated_at").to_rfc3339(),
            })
        })
        .collect();
    Ok(Json(json!({ "target_url": st.settings.sync_target_url, "cursors": cursors })))
}

/// Drain `schema.table` (rows with `wm_col > cursor`) to the target inbox,
/// grouped by `source_run_id` so each pushed batch is lineage-homogeneous.
pub async fn run_push(
    st: &AppState,
    schema: &str,
    table: &str,
    wm_col: &str,
) -> ApiResult<PushSummary> {
    if st.settings.sync_target_url.is_empty() {
        return Err(ApiError::BadRequest("LUMID_SYNC_TARGET_URL not configured".into()));
    }
    if !valid_ident(schema) || !valid_ident(table) || !valid_ident(wm_col) {
        return Err(ApiError::BadRequest("invalid schema/table/watermark identifier".into()));
    }

    let target = st.settings.sync_target_url.clone();
    let token = st.settings.sync_target_token.clone();
    let peer = if st.settings.sync_peer_id.is_empty() {
        "default".to_string()
    } else {
        st.settings.sync_peer_id.clone()
    };
    let batch_rows = st.settings.sync_batch_rows as i64;
    let url = format!("{target}/sync/apply/{schema}/{table}");

    let client = st.pool.get().await?;

    // The watermark column must be timestamp-typed (we cast `$1::timestamptz`
    // and read it as a DateTime). Validate up front so a wrong/absent column is
    // a clear 400, not a runtime 500 mid-drain.
    let wm_type: Option<String> = client
        .query_opt(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema=$1 AND table_name=$2 AND column_name=$3",
            &[&schema, &table, &wm_col],
        )
        .await
        .map_err(dberr)?
        .map(|r| r.get::<_, String>("data_type"));
    match wm_type.as_deref() {
        Some("timestamp with time zone") | Some("timestamp without time zone") => {}
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "watermark column {schema}.{table}.{wm_col} is `{other}`, must be a timestamp"
            )))
        }
        None => {
            return Err(ApiError::BadRequest(format!(
                "watermark column {schema}.{table}.{wm_col} does not exist"
            )))
        }
    }

    // Keyset cursor: (watermark, ctid). The ctid tiebreaker makes paging total
    // and unique even when many rows share one watermark timestamp (the common
    // case — a COPY/merge stamps one `ingest_ts` per batch); strict `>` on the
    // timestamp alone would silently drop ties that straddle a page boundary.
    let cur_row = client
        .query_opt(
            "SELECT watermark, watermark_key FROM sync.push_cursor \
             WHERE target_url=$1 AND schema_name=$2 AND table_name=$3",
            &[&target, &schema, &table],
        )
        .await
        .map_err(dberr)?;
    let (mut cur_wm, mut cur_key): (Option<DateTime<Utc>>, Option<String>) = match cur_row {
        None => (None, None),
        Some(r) => {
            let wm_text: Option<String> = r.get("watermark");
            let key: Option<String> = r.get("watermark_key");
            match wm_text {
                None => (None, None),
                Some(s) => {
                    let parsed = DateTime::parse_from_rfc3339(&s)
                        .map(|d| d.with_timezone(&Utc))
                        .map_err(|e| {
                            // Surface corruption instead of silently resetting to
                            // NULL (which would re-drain the whole table).
                            ApiError::Internal(anyhow::anyhow!(
                                "corrupt push_cursor watermark {s:?}: {e}"
                            ))
                        })?;
                    (Some(parsed), key)
                }
            }
        }
    };

    let select_sql = format!(
        "SELECT to_jsonb(t) AS row, t.{wm} AS wm, t.ctid::text AS rk FROM {schema}.{table} t \
         WHERE ($1::timestamptz IS NULL \
                OR t.{wm} > $1::timestamptz \
                OR (t.{wm} = $1::timestamptz AND t.ctid > $2::text::tid)) \
         ORDER BY t.{wm} ASC, t.ctid ASC LIMIT $3",
        wm = wm_col,
        schema = schema,
        table = table
    );

    let mut batches = 0u64;
    let mut total = 0i64;

    loop {
        // Param-type inference note: `$1::timestamptz` makes Postgres infer $1 as
        // timestamptz (bind DateTime<Utc> — fine). The ctid tiebreaker uses
        // `$2::text::tid`, NOT `$2::tid`: a bare `$2::tid` makes Postgres infer $2
        // as `tid`, and binding the `String` ctid there fails with "error
        // serializing parameter". Routing through `::text` infers $2 as text
        // (String binds cleanly) and the chained `::tid` restores proper tid
        // ordering for the keyset comparison.
        let rows = client
            .query(&select_sql, &[&cur_wm, &cur_key, &batch_rows])
            .await
            .map_err(dberr)?;
        if rows.is_empty() {
            break;
        }
        let page_wm: DateTime<Utc> = rows.last().unwrap().get("wm");
        let page_key: String = rows.last().unwrap().get("rk");
        let records: Vec<Value> = rows.iter().map(|r| r.get::<_, Value>("row")).collect();

        // Group the page by source_run_id so each batch is lineage-homogeneous.
        let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for rec in records {
            let run = rec
                .get("source_run_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    ApiError::BadRequest(format!(
                        "{schema}.{table} row missing string source_run_id; not syncable"
                    ))
                })?
                .to_string();
            groups.entry(run).or_default().push(rec);
        }

        for (run_id_str, group) in groups {
            // Lineage is read from the raw row (which carries the provenance
            // columns) BEFORE stripping.
            let (src, src_ep, run_id) = lineage_of(&group[0])?;
            let preamble = build_preamble(&client, &run_id).await?;
            // The server-stamped columns (source, source_endpoint, source_run_id,
            // ingest_ts, id) are set server-side: the inbox's ingest plane REJECTS
            // records that carry them (422 "field 'source' is set server-side").
            // They ride in the batch envelope above, so strip them from each record
            // body. `raw` is NOT stripped — the validator accepts it, so the synced
            // copy keeps it. Strip before hashing so the batch_id matches what's sent.
            let clean: Vec<Value> = group.iter().map(strip_server_side_cols).collect();
            let batch_id = deterministic_batch_id(&peer, schema, table, &run_id_str, &clean);
            let body = json!({
                "batch_id": batch_id,
                "source": src,
                "source_endpoint": src_ep,
                "source_run_id": run_id,
                "provenance": preamble,
                "records": clean,
            });

            match post_with_retry(st, &url, &token, &peer, &body).await {
                Ok(apply) if apply.rejected() > 0 => {
                    // A per-record reject is DETERMINISTIC — the same row pushed
                    // again alone won't pass validation, so retrying buys nothing.
                    // Treating it as a clean push (advancing the cursor past these
                    // rows) would silently drop them forever. Stop loud instead:
                    // do NOT advance the cursor past this page, surface the reject
                    // so the operator can fix the schema mismatch and re-push (the
                    // deterministic batch_id makes the re-push idempotent).
                    let detail = apply.reject_detail(batch_id);
                    tracing::warn!("sync push {schema}.{table}: {detail}");
                    save_cursor(
                        &client,
                        &target,
                        schema,
                        table,
                        cur_wm,
                        cur_key.clone(),
                        total,
                        &format!("partial: {detail}"),
                    )
                    .await?;
                    return Ok(PushSummary {
                        schema: schema.into(),
                        table: table.into(),
                        batches,
                        rows_pushed: total,
                        status: "partial".into(),
                        last_error: Some(detail),
                    });
                }
                Ok(_) => {}
                Err(msg) => {
                    save_cursor(&client, &target, schema, table, cur_wm, cur_key.clone(), total, &format!("failed: {msg}")).await?;
                    return Ok(PushSummary {
                        schema: schema.into(),
                        table: table.into(),
                        batches,
                        rows_pushed: total,
                        status: "failed".into(),
                        last_error: Some(msg),
                    });
                }
            }
            total += group.len() as i64;
            batches += 1;
        }

        // Whole page delivered → advance the durable keyset cursor.
        cur_wm = Some(page_wm);
        cur_key = Some(page_key);
        save_cursor(&client, &target, schema, table, cur_wm, cur_key.clone(), total, "ok").await?;
    }

    Ok(PushSummary {
        schema: schema.into(),
        table: table.into(),
        batches,
        rows_pushed: total,
        status: "ok".into(),
        last_error: None,
    })
}

/// Extract the lineage triplet from a row's JSON.
fn lineage_of(rec: &Value) -> ApiResult<(String, String, Uuid)> {
    let src = rec.get("source").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ep = rec.get("source_endpoint").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let run = rec
        .get("source_run_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| ApiError::BadRequest("row has no valid source_run_id".into()))?;
    if src.is_empty() || ep.is_empty() {
        return Err(ApiError::BadRequest("row missing source/source_endpoint".into()));
    }
    Ok((src, ep, run))
}

/// Build the provenance preamble for one run: its run row + endpoint + api_source.
async fn build_preamble(client: &Client, run_id: &Uuid) -> ApiResult<super::Preamble> {
    let mut pre = super::Preamble::default();
    let run_row = client
        .query_opt(
            "SELECT to_jsonb(r) AS j, r.endpoint_id FROM provenance.runs r WHERE r.run_id=$1",
            &[run_id],
        )
        .await
        .map_err(dberr)?;
    if let Some(rr) = run_row {
        pre.runs.push(rr.get::<_, Value>("j"));
        let endpoint_id: String = rr.get("endpoint_id");
        if let Some(er) = client
            .query_opt(
                "SELECT to_jsonb(e) AS j, e.source FROM provenance.endpoints e WHERE e.endpoint_id=$1",
                &[&endpoint_id],
            )
            .await
            .map_err(dberr)?
        {
            pre.endpoints.push(er.get::<_, Value>("j"));
            let source: String = er.get("source");
            if let Some(sr) = client
                .query_opt(
                    "SELECT to_jsonb(s) AS j FROM provenance.api_sources s WHERE s.source=$1",
                    &[&source],
                )
                .await
                .map_err(dberr)?
            {
                pre.api_sources.push(sr.get::<_, Value>("j"));
            }
        }
    }
    Ok(pre)
}

/// Deterministic batch id over (peer, table, run, and EVERY row's content) so a
/// re-run before cursor-advance redelivers the SAME batch_id (the inbox dedups
/// it) while any change to the group's contents yields a different id. Hashing
/// the full content — not just first/last+len — prevents a distinct group from
/// colliding with an already-applied batch_id and being silently dropped.
fn deterministic_batch_id(
    peer: &str,
    schema: &str,
    table: &str,
    run_id: &str,
    group: &[Value],
) -> Uuid {
    let mut h = Sha256::new();
    h.update(peer.as_bytes());
    h.update(b"|");
    h.update(format!("{schema}.{table}").as_bytes());
    h.update(b"|");
    h.update(run_id.as_bytes());
    h.update(b"|");
    h.update((group.len() as u64).to_le_bytes());
    for row in group {
        h.update(b"\x1e"); // record separator
        h.update(row.to_string().as_bytes());
    }
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Parsed `/sync/apply` ACK body. The inbox returns a per-batch apply summary;
/// we care about how many records it rejected at validation so a partial reject
/// can be surfaced as a hard push failure (rather than silently skipped).
#[derive(Debug, Default, Clone)]
struct ApplyAck {
    /// Records the target rejected at validation (0 on a clean batch). The inbox
    /// names this `failed`; older shapes used `rejected` — accept either.
    failed: i64,
    /// First reject reason, if the body carries one — surfaced in `last_error`.
    sample_reason: Option<String>,
}

impl ApplyAck {
    /// Extract the reject count + a sample reason from a 2xx ACK body.
    fn from_value(ack: &Value) -> Self {
        let failed = ack
            .get("failed")
            .or_else(|| ack.get("rejected"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        // Look for a sample reason under a few plausible shapes the inbox uses:
        // `errors[0].reason` / `errors[0]` (string) / `rejects[0].reason` / `error`.
        let sample_reason = ack
            .get("errors")
            .or_else(|| ack.get("rejects"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|e| {
                e.get("reason")
                    .and_then(|r| r.as_str())
                    .or_else(|| e.as_str())
                    .map(|s| s.to_string())
            })
            .or_else(|| ack.get("error").and_then(|v| v.as_str()).map(|s| s.to_string()));
        ApplyAck { failed, sample_reason }
    }

    fn rejected(&self) -> i64 {
        self.failed
    }

    /// Human-readable detail for a partial-reject push, used in logs +
    /// `PushSummary.last_error` so the operator sees count + a sample reason.
    fn reject_detail(&self, batch_id: Uuid) -> String {
        match &self.sample_reason {
            Some(r) => format!(
                "target rejected {} record(s) in batch {batch_id}: {}",
                self.failed,
                r.chars().take(200).collect::<String>()
            ),
            None => format!("target rejected {} record(s) in batch {batch_id}", self.failed),
        }
    }
}

/// Classify a single non-success HTTP response into retry-vs-fail. A
/// DETERMINISTIC 4xx (422 schema mismatch, 400, 401, 404, …) will never succeed
/// on retry, so it fast-fails. 5xx is transient → retry. 429 (Too Many Requests)
/// is the one 4xx that IS transient: the push is idempotent (deterministic
/// `batch_id` → the inbox dedups a true duplicate, and a partial-reject leaves no
/// ledger so a re-apply is safe), so backing off and retrying is correct and
/// avoids a needless gap until the next scheduler interval. Pure (no I/O) so it
/// can be unit-tested without a live server.
fn should_retry_status(code: u16) -> bool {
    code == 429 || (500..600).contains(&code)
}

/// Parse a `Retry-After` header value into a backoff in milliseconds. Supports
/// the delta-seconds form (`Retry-After: 5`); the HTTP-date form is ignored
/// (returns `None`) — we fall back to exponential backoff rather than pull in a
/// date parser for a rarely-used shape. The result is clamped to `max_ms` so a
/// hostile/huge value can't park the drain indefinitely. Pure for unit testing.
fn retry_after_ms(header: Option<&str>, max_ms: u64) -> Option<u64> {
    let secs: u64 = header?.trim().parse().ok()?;
    Some(secs.saturating_mul(1000).min(max_ms))
}

/// Exponential backoff in ms for `attempt` (1-based): `base * 2^(attempt-1)`,
/// with the shift clamped at 16 (so the schedule caps at `base * 65536`). This
/// is the historical formula extracted to a pure fn so the cap doubles as the
/// `Retry-After` clamp ceiling. Pure for unit testing.
fn exp_backoff(base_ms: u64, attempt: u32, max_ms: u64) -> u64 {
    base_ms
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(16))
        .min(max_ms)
}

/// POST a batch with bounded exponential-backoff retry. `Ok(ApplyAck)` on a 2xx
/// ACK (carries the reject count + a sample reason; 0 on a clean batch);
/// `Err(msg)` on a deterministic 4xx (no retry) or once retries are exhausted on
/// transient classes (429 + 5xx + transport/timeout/connection errors). A 429
/// honors `Retry-After` (delta-seconds) when present, clamped to the max backoff.
async fn post_with_retry(
    st: &AppState,
    url: &str,
    token: &str,
    peer: &str,
    body: &Value,
) -> Result<ApplyAck, String> {
    let max = st.settings.sync_max_attempts.max(1);
    // Ceiling for both the exponential schedule and any honored `Retry-After`.
    let max_backoff = st.settings.sync_backoff_ms.saturating_mul(1u64 << 16);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let mut rb = st.http.post(url).json(body).header("X-Lumid-Sync-Peer", peer);
        if !token.is_empty() {
            rb = rb.bearer_auth(token);
        }
        match rb.send().await {
            Ok(resp) if resp.status().is_success() => {
                let ack: Value = resp.json().await.unwrap_or(Value::Null);
                return Ok(ApplyAck::from_value(&ack));
            }
            Ok(resp) => {
                let code = resp.status().as_u16();
                // Read a server-advertised backoff BEFORE consuming the body.
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let txt = resp.text().await.unwrap_or_default();
                let msg = format!("target HTTP {code}: {}", txt.chars().take(200).collect::<String>());
                // Fast-fail deterministic 4xx — retrying just burns
                // max_attempts × backoff for a response that can never succeed.
                // 429 is the exception: transient, and the push is idempotent, so
                // it retries (honoring Retry-After when present).
                if !should_retry_status(code) || attempt >= max {
                    return Err(msg);
                }
                // A 429 with a Retry-After: honor it (clamped to max backoff)
                // instead of the exponential schedule for this one sleep.
                if let Some(ms) = retry_after_ms(retry_after.as_deref(), max_backoff) {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    continue;
                }
            }
            Err(e) => {
                // Transport/timeout/connection errors are genuinely transient.
                if attempt >= max {
                    return Err(format!("send error: {e}"));
                }
            }
        }
        let backoff = exp_backoff(st.settings.sync_backoff_ms, attempt, max_backoff);
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn save_cursor(
    client: &Client,
    target_url: &str,
    schema: &str,
    table: &str,
    cur_wm: Option<DateTime<Utc>>,
    cur_key: Option<String>,
    rows_pushed: i64,
    result: &str,
) -> ApiResult<()> {
    let wm = cur_wm.map(|d| d.to_rfc3339());
    client
        .execute(
            "INSERT INTO sync.push_cursor \
               (target_url, schema_name, table_name, watermark, watermark_key, rows_pushed, last_result, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7, now()) \
             ON CONFLICT (target_url, schema_name, table_name) DO UPDATE \
               SET watermark=EXCLUDED.watermark, watermark_key=EXCLUDED.watermark_key, \
                   rows_pushed=EXCLUDED.rows_pushed, last_result=EXCLUDED.last_result, updated_at=now()",
            &[&target_url, &schema, &table, &wm, &cur_key, &rows_pushed, &result],
        )
        .await
        .map_err(dberr)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_guard_rejects_injection() {
        assert!(valid_ident("market"));
        assert!(valid_ident("ohlc_1min"));
        assert!(valid_ident("ingest_ts"));
        assert!(!valid_ident("market; drop table x"));
        assert!(!valid_ident("a.b"));
        assert!(!valid_ident("\"quoted\""));
        assert!(!valid_ident(""));
    }

    #[test]
    fn batch_id_is_deterministic_and_run_scoped() {
        let g = vec![json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})];
        let a = deterministic_batch_id("p", "market", "dividends", "run-1", &g);
        let b = deterministic_batch_id("p", "market", "dividends", "run-1", &g);
        assert_eq!(a, b, "same inputs → same batch_id (redelivery dedups)");

        // different run → different id
        let c = deterministic_batch_id("p", "market", "dividends", "run-2", &g);
        assert_ne!(a, c);
        // different peer → different id
        let d = deterministic_batch_id("q", "market", "dividends", "run-1", &g);
        assert_ne!(a, d);
        // different content → different id
        let g2 = vec![json!({"id": 1, "v": "a"}), json!({"id": 3, "v": "c"})];
        assert_ne!(a, deterministic_batch_id("p", "market", "dividends", "run-1", &g2));
    }

    #[test]
    fn strip_removes_server_cols_keeps_raw_and_data() {
        let rec = json!({
            "source": "fmp", "source_endpoint": "stable/x",
            "source_run_id": "11111111-1111-1111-1111-111111111111",
            "ingest_ts": "2026-01-01T00:00:00Z", "id": 42,
            "raw": {"k": "v"}, "symbol": "AAPL", "val": 1.5
        });
        let out = strip_server_side_cols(&rec);
        let m = out.as_object().unwrap();
        // server-stamped cols rejected by the validator are stripped...
        for k in ["source", "source_endpoint", "source_run_id", "ingest_ts", "id"] {
            assert!(!m.contains_key(k), "{k} should be stripped");
        }
        // ...but `raw` (validator-accepted) and real data are kept.
        assert_eq!(m.get("raw"), Some(&json!({"k": "v"})), "raw must be preserved");
        assert_eq!(m.get("symbol"), Some(&json!("AAPL")));
        assert_eq!(m.get("val"), Some(&json!(1.5)));
    }

    #[test]
    fn strip_passes_through_non_objects() {
        assert_eq!(strip_server_side_cols(&json!("scalar")), json!("scalar"));
        assert_eq!(strip_server_side_cols(&json!([1, 2])), json!([1, 2]));
    }

    #[test]
    fn lineage_of_extracts_triplet() {
        let run = uuid::Uuid::new_v4();
        let rec = json!({
            "source": "fmp",
            "source_endpoint": "stable/dividends",
            "source_run_id": run.to_string(),
            "symbol": "AAPL"
        });
        let (s, e, r) = lineage_of(&rec).unwrap();
        assert_eq!(s, "fmp");
        assert_eq!(e, "stable/dividends");
        assert_eq!(r, run);
    }

    #[test]
    fn lineage_of_rejects_missing_run() {
        let rec = json!({"source": "fmp", "source_endpoint": "x"});
        assert!(lineage_of(&rec).is_err());
    }

    // --- Bug 1: retry classification (4xx fast-fails, 5xx retries) ---

    #[test]
    fn retry_only_on_5xx_and_429_not_other_4xx() {
        // Deterministic 4xx → never retry (would just burn backoff).
        assert!(!should_retry_status(400), "400 must fast-fail");
        assert!(!should_retry_status(401), "401 must fast-fail");
        assert!(!should_retry_status(404), "404 must fast-fail");
        assert!(!should_retry_status(422), "422 schema-mismatch must fast-fail");
        // 429 is the transient 4xx: an idempotent push should retry with backoff
        // rather than gap until the next scheduler interval.
        assert!(should_retry_status(429), "429 Too Many Requests is transient, retry");
        // Transient 5xx → retry.
        assert!(should_retry_status(500), "500 is transient, retry");
        assert!(should_retry_status(502), "502 is transient, retry");
        assert!(should_retry_status(503), "503 is transient, retry");
        // Boundaries: 3xx is not retryable; other 4xx (incl. 428/430) fast-fail;
        // 600 is out of the 5xx band.
        assert!(!should_retry_status(399));
        assert!(!should_retry_status(428), "only 429 retries among 4xx");
        assert!(!should_retry_status(430), "only 429 retries among 4xx");
        assert!(!should_retry_status(499));
        assert!(!should_retry_status(600));
    }

    #[test]
    fn retry_after_parses_delta_seconds_and_clamps() {
        // Delta-seconds form → ms.
        assert_eq!(retry_after_ms(Some("5"), 60_000), Some(5_000));
        assert_eq!(retry_after_ms(Some(" 2 "), 60_000), Some(2_000), "whitespace tolerated");
        assert_eq!(retry_after_ms(Some("0"), 60_000), Some(0));
        // Clamped to the max backoff so a hostile/huge value can't park forever.
        assert_eq!(retry_after_ms(Some("99999"), 30_000), Some(30_000));
        // HTTP-date form is not parsed → None (falls back to exp backoff).
        assert_eq!(retry_after_ms(Some("Wed, 21 Oct 2026 07:28:00 GMT"), 60_000), None);
        // Absent / garbage → None.
        assert_eq!(retry_after_ms(None, 60_000), None);
        assert_eq!(retry_after_ms(Some("soon"), 60_000), None);
        assert_eq!(retry_after_ms(Some("-3"), 60_000), None, "negative is not delta-seconds");
    }

    #[test]
    fn exp_backoff_doubles_and_caps() {
        // base * 2^(attempt-1), clamped at shift 16 (the historical schedule).
        assert_eq!(exp_backoff(500, 1, u64::MAX), 500);
        assert_eq!(exp_backoff(500, 2, u64::MAX), 1_000);
        assert_eq!(exp_backoff(500, 3, u64::MAX), 2_000);
        // Shift caps at 16 → base * 65536 regardless of higher attempt counts.
        assert_eq!(exp_backoff(1, 17, u64::MAX), 1 << 16);
        assert_eq!(exp_backoff(1, 99, u64::MAX), 1 << 16);
        // And the explicit max_ms clamp wins when lower.
        assert_eq!(exp_backoff(500, 10, 3_000), 3_000);
    }

    /// Mirrors `post_with_retry`'s attempt-loop control flow over a fixed
    /// sequence of response status codes, so we can assert exactly how many
    /// attempts a given outcome class costs WITHOUT a live HTTP server. Returns
    /// `(attempts_made, succeeded)`.
    fn simulate_attempts(statuses: &[u16], max: u32) -> (u32, bool) {
        let max = max.max(1);
        let mut attempt = 0u32;
        loop {
            let code = statuses.get((attempt) as usize).copied().unwrap_or(503);
            attempt += 1;
            if (200..300).contains(&code) {
                return (attempt, true);
            }
            if !should_retry_status(code) || attempt >= max {
                return (attempt, false);
            }
        }
    }

    #[test]
    fn deterministic_4xx_fast_fails_in_one_attempt() {
        // A 422 with plenty of retry budget still stops after the first attempt.
        let (attempts, ok) = simulate_attempts(&[422], 5);
        assert_eq!(attempts, 1, "4xx must not consume more than one attempt");
        assert!(!ok);
    }

    #[test]
    fn transient_5xx_retries_up_to_max() {
        // Persistent 5xx burns the whole retry budget, then fails.
        let (attempts, ok) = simulate_attempts(&[503, 503, 503, 503, 503], 4);
        assert_eq!(attempts, 4, "5xx retries up to max_attempts");
        assert!(!ok);
        // A 5xx that recovers on the 3rd attempt succeeds without exhausting.
        let (attempts, ok) = simulate_attempts(&[500, 502, 200], 5);
        assert_eq!(attempts, 3);
        assert!(ok);

        // 429 is now transient: a rate-limit that clears on the 2nd attempt
        // succeeds rather than fast-failing in one (the pre-#18-followup bug).
        let (attempts, ok) = simulate_attempts(&[429, 200], 5);
        assert_eq!(attempts, 2, "429 must retry, not fast-fail");
        assert!(ok);
        // Persistent 429 burns the budget like any transient class.
        let (attempts, ok) = simulate_attempts(&[429, 429, 429], 3);
        assert_eq!(attempts, 3);
        assert!(!ok);
    }

    // --- Bug 2: partial reject is a hard failure, clean push advances ---

    #[test]
    fn apply_ack_clean_response_has_no_rejects() {
        let ack = ApplyAck::from_value(&json!({"applied": 10, "failed": 0}));
        assert_eq!(ack.rejected(), 0, "clean push: status stays ok, cursor advances");
        assert!(ack.sample_reason.is_none());

        // Missing/absent fields default to a clean ACK (byte-equivalent to today).
        let ack = ApplyAck::from_value(&json!({"applied": 10}));
        assert_eq!(ack.rejected(), 0);
        let ack = ApplyAck::from_value(&Value::Null);
        assert_eq!(ack.rejected(), 0);
    }

    #[test]
    fn apply_ack_partial_reject_is_surfaced() {
        // Per-record reject → non-zero count drives the "partial" hard-failure
        // path in run_push (cursor NOT advanced, status != "ok").
        let ack = ApplyAck::from_value(&json!({
            "applied": 8,
            "failed": 2,
            "errors": [{"reason": "field 'symbol' is required"}]
        }));
        assert_eq!(ack.rejected(), 2);
        let detail = ack.reject_detail(Uuid::nil());
        assert!(detail.contains("rejected 2 record(s)"), "detail has count: {detail}");
        assert!(detail.contains("field 'symbol' is required"), "detail has reason: {detail}");

        // Legacy `rejected` field name is accepted too.
        let ack = ApplyAck::from_value(&json!({"rejected": 1}));
        assert_eq!(ack.rejected(), 1);

        // Reason can be a bare string in the array, or a top-level `error`.
        let ack = ApplyAck::from_value(&json!({"failed": 1, "rejects": ["bad row 5"]}));
        assert_eq!(ack.rejected(), 1);
        assert!(ack.reject_detail(Uuid::nil()).contains("bad row 5"));

        let ack = ApplyAck::from_value(&json!({"failed": 1, "error": "schema mismatch"}));
        assert!(ack.reject_detail(Uuid::nil()).contains("schema mismatch"));
    }
}
