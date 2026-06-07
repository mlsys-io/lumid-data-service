//! Inbox: apply a pushed batch idempotently, preserving original lineage.
//!
//! Flow (per request):
//! 1. `require_sync_peer` — gate to sync peers; derive the peer id.
//! 2. Dedup on `(peer, batch_id)` via `sync.inbox_ledger` → short-circuit a
//!    redelivery with the stored counts.
//! 3. Upsert the provenance preamble (`api_sources → endpoints → runs`) verbatim
//!    so the data rows' `source_run_id` FK is satisfied with the ORIGINAL run.
//! 4. Apply the records through `ingest_records`, adopting the original run
//!    (`run_id=Some`) + lineage triplet — target rows are lineage-identical.
//! 5. Invalidate the read cache; record the ledger row.
//!
//! Ordering note: the ledger is written AFTER a successful apply (never before),
//! so a crash between apply and ledger only causes an idempotent re-apply on
//! redelivery — never a "ledger says applied but data missing" loss.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde_json::Value;
use tokio_postgres::Client;

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::ingest::core::{ingest_records, IngestParams};
use crate::state::AppState;

use super::{require_sync_peer, SyncAck, SyncApplyBody};

pub async fn post_sync_apply(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    Json(body): Json<SyncApplyBody>,
) -> ApiResult<Json<SyncAck>> {
    let peer = require_sync_peer(&st.settings, &identity)?;

    // Target denylist: a peer must not write to the sync/provenance/system
    // schemas (replaces the `gate_target` ACL the normal /ingest path applies).
    if super::is_denied_schema(&schema) {
        return Err(ApiError::Forbidden(format!(
            "schema `{schema}` is not a permitted sync target"
        )));
    }

    if body.records.is_empty() {
        return Err(ApiError::BadRequest("`records` must be non-empty".into()));
    }

    let client = st.pool.get().await?;

    // 2. Dedup: already applied for this peer → return the stored counts.
    if let Some(row) = client
        .query_opt(
            "SELECT inserted, updated FROM sync.inbox_ledger WHERE peer=$1 AND batch_id=$2",
            &[&peer, &body.batch_id],
        )
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("ledger lookup: {e}")))?
    {
        return Ok(Json(SyncAck {
            batch_id: body.batch_id,
            inserted: row.get::<_, i64>("inserted"),
            updated: row.get::<_, i64>("updated"),
            failed: 0,
            duplicate: true,
        }));
    }

    // 3. Lineage preamble — upsert provided provenance rows verbatim, in FK order.
    upsert_prov(&client, "provenance.api_sources", "source", &body.provenance.api_sources).await?;
    upsert_prov(&client, "provenance.endpoints", "endpoint_id", &body.provenance.endpoints).await?;
    upsert_prov(&client, "provenance.runs", "run_id", &body.provenance.runs).await?;

    // 3b. The data write adopts `source_run_id` (ingest_records with run_id=Some
    //     does NOT create the run), so the run MUST exist now — either shipped in
    //     the preamble or already present. Guard with a clear 400 instead of
    //     letting the opaque FK violation surface from the merge.
    let run_exists = client
        .query_opt("SELECT 1 FROM provenance.runs WHERE run_id=$1", &[&body.source_run_id])
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("run lookup: {e}")))?
        .is_some();
    if !run_exists {
        return Err(ApiError::BadRequest(format!(
            "adopted run {} has no provenance.runs row; include it in `provenance.runs`",
            body.source_run_id
        )));
    }

    // 4. Apply through the normal ingest pipeline, adopting the original run +
    //    lineage triplet (so rows on the target trace to the source's run).
    let params = IngestParams {
        target_schema: &schema,
        target_table: &table,
        source: &body.source,
        source_endpoint: &body.source_endpoint,
        submitted_by: Some(&peer),
        run_id: Some(body.source_run_id), // adopt: ingest_records won't open/close a run
        declared_endpoint: None,
        mode: "sync",
        user_agent: None,
        validate: true,
        fire_lumilake: false,
    };
    let result = ingest_records(&st.backends, &params, &body.records)
        .await
        .map_err(ApiError::from)?;

    // 4b. A fully-rejected batch is NOT a successful apply — ingest_records
    //     returns Ok(status="failed") in that case (it doesn't error). Do NOT
    //     write the ledger and DO signal failure, so the producer retries /
    //     surfaces it instead of silently advancing past dropped rows.
    if result.status == "failed"
        && result.inserted == 0
        && result.updated == 0
        && result.failed == result.received
    {
        return Err(ApiError::Validation(result.to_json()));
    }

    // 5a. Read-your-writes: drop the target's cached reads of this table.
    if result.inserted + result.updated > 0 {
        st.read_cache.invalidate_table(&schema, &table).await;
    }

    // 5b. Record the ledger (idempotency for redelivery). DO NOTHING covers a
    //     concurrent duplicate that applied first.
    client
        .execute(
            "INSERT INTO sync.inbox_ledger \
               (peer, batch_id, source_run_id, target_schema, target_table, inserted, updated, n_rows) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (peer, batch_id) DO NOTHING",
            &[
                &peer,
                &body.batch_id,
                &body.source_run_id,
                &schema,
                &table,
                &result.inserted,
                &result.updated,
                &(result.received as i64),
            ],
        )
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("ledger insert: {e}")))?;

    Ok(Json(SyncAck {
        batch_id: body.batch_id,
        inserted: result.inserted,
        updated: result.updated,
        failed: result.failed as i64,
        duplicate: false,
    }))
}

/// Upsert verbatim provenance rows into `table` (a trusted `provenance.<t>`
/// identifier) keyed on `pk`. `jsonb_populate_record` maps each JSON object to
/// the table's row type server-side (matching by column name, correct types,
/// extra keys ignored) — no client-side type mapping. `ON CONFLICT DO NOTHING`
/// makes redelivery / pre-existing rows a no-op.
async fn upsert_prov(client: &Client, table: &str, pk: &str, rows: &[Value]) -> ApiResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "INSERT INTO {table} \
         SELECT (jsonb_populate_record(NULL::{table}, $1)).* \
         ON CONFLICT ({pk}) DO NOTHING"
    );
    for row in rows {
        client
            .execute(&sql, &[row])
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("preamble upsert {table}: {e}")))?;
    }
    Ok(())
}
