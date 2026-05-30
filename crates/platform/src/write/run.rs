//! Run lifecycle — port of `writeengine.open_run` / `close_run`.
//!
//! Each ingest writes one `provenance.runs` row (status running → ok/partial/
//! failed) carrying the caller identity, mode, and row counts. Stream mode
//! opens the run once and the route owns close.

use serde_json::Value;
use tokio_postgres::Client;
use uuid::Uuid;

/// Insert a `running` run row; returns its `run_id`.
pub async fn open_run(
    client: &Client,
    endpoint_id: &str,
    args: &Value,
    credential_label: Option<&str>,
) -> Result<Uuid, tokio_postgres::Error> {
    // Bind the JSON value directly (tokio-postgres `with-serde_json` maps
    // `Value` -> jsonb). Binding a String to the `$3::jsonb`-typed param fails
    // with "cannot convert String and jsonb".
    let row = client
        .query_one(
            "INSERT INTO provenance.runs (endpoint_id, credential_label, status, args) \
             VALUES ($1, $2, 'running', $3) \
             RETURNING run_id",
            &[&endpoint_id, &credential_label, &args],
        )
        .await?;
    Ok(row.get("run_id"))
}

/// Stamp `submitted_by` on a run row (separate column, not args).
pub async fn set_submitted_by(
    client: &Client,
    run_id: &Uuid,
    submitted_by: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "UPDATE provenance.runs SET submitted_by = $1 WHERE run_id = $2",
            &[&submitted_by, run_id],
        )
        .await?;
    Ok(())
}

/// Stamp ended_at / status / counts on a run row — port of `close_run`.
#[allow(clippy::too_many_arguments)]
pub async fn close_run(
    client: &Client,
    run_id: &Uuid,
    status: &str,
    rows_inserted: i64,
    rows_updated: i64,
    rows_failed: i64,
    error_text: Option<&str>,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "UPDATE provenance.runs \
                SET ended_at = now(), \
                    status = $1, \
                    rows_inserted = $2, \
                    rows_updated = $3, \
                    rows_failed = $4, \
                    error_text = $5 \
              WHERE run_id = $6",
            &[
                &status,
                &rows_inserted,
                &rows_updated,
                &rows_failed,
                &error_text,
                run_id,
            ],
        )
        .await?;
    Ok(())
}
