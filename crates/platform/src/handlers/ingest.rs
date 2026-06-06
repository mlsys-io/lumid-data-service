//! Ingress HTTP handlers — port of `injection/routes/ingest_*.py`.
//!
//! Modes: typed (POST /ingest/{schema}/{table}), stream (NDJSON), file
//! (multipart), blob (raw bytes), webhook (HMAC, ungated). Admin self-service
//! (ACL grant/revoke + cache refresh) and the schema.json catalog route.
//!
//! Net-new-target sandbox + adapter mode are OUT OF SCOPE for this build (see
//! report): an unknown table returns 404 instead of routing to the sandbox.

use axum::body::Bytes;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::ingest::core::{ingest_records, IngestErr, IngestParams, IngestResult};
use crate::ingest::{acl, blob, webhook};
use crate::ingest::lumilake::{self, LumilakeInfo};
use crate::parsers::{self, Kind};
use crate::state::AppState;
use crate::validation::{self, Rejected};
use crate::write::{introspect, run};

/// Source-endpoint provenance constant pattern lives in core; the header/body
/// inputs are validated there. These helpers build the per-mode src strings.
fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

// ---------------------------------------------------------------------------
// Split-gate: existing target → ACL; net-new → 404 (sandbox out of scope).
// ---------------------------------------------------------------------------
async fn gate_target(
    st: &AppState,
    identity: &Identity,
    schema: &str,
    table: &str,
) -> Result<bool, ApiError> {
    // Existence must be checked on the backend that OWNS the table — a
    // ClickHouse-backed table is invisible to Postgres's information_schema, so
    // a PG-only `table_exists` re-proposed every write to a CH table. `reg.get`
    // defaults an unknown table to Postgres, so a genuinely-new shape still
    // reports not-exists and enters the propose flow.
    let backend = st.backends.get(schema, table).await?;
    let exists = backend.table_meta(schema, table).await?.is_some();
    if exists {
        acl::check_can_write(&st.pool, &identity.role, schema, table).await?;
    } else if !acl::can_propose(&st.pool, &identity.role).await? {
        return Err(ApiError::Forbidden(format!(
            "role {:?} has no ingress allowlist entries; cannot write or propose new tables.",
            identity.role
        )));
    }
    Ok(exists)
}

/// Map an IngestErr to the route-level response. UnknownTable → 404
/// (the sandbox/proposal fallback is a Python-sidecar concern).
fn ingest_err_to_api(e: IngestErr) -> ApiError {
    e.into()
}

/// Drop cached reads of the written table so the just-ingressed rows are
/// immediately queryable (read-your-writes). Table-level; also publishes to the
/// `cache:invalidate` Redis channel for other replicas.
async fn invalidate_reads(st: &AppState, r: &IngestResult) {
    if r.inserted + r.updated > 0 {
        st.read_cache
            .invalidate_table(&r.target_schema, &r.target_table)
            .await;
    }
}

/// "all records rejected" → 422 with the result body (matches the Python
/// short-circuit at the end of post_typed / post_file).
fn all_failed(result: &IngestResult) -> bool {
    result.status == "failed"
        && result.inserted == 0
        && result.updated == 0
        && result.failed == result.received
}

// ---------------------------------------------------------------------------
// POST /ingest/{schema}/{table}  (typed)
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
pub struct TypedBody {
    #[serde(default)]
    pub source_endpoint: Option<String>,
    pub records: Vec<Value>,
}

pub async fn post_typed(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<TypedBody>,
) -> ApiResult<Json<Value>> {
    if body.records.is_empty() {
        return Err(ApiError::BadRequest("`records` must be non-empty".into()));
    }
    let exists = gate_target(&st, &identity, &schema, &table).await?;
    if !exists {
        // Unknown table + propose rights (gate_target enforced) → infer a schema
        // from the records and stage a proposal for admin approval.
        return Ok(Json(
            crate::ingest::proposals::create(
                &st.pool, &schema, &table, &identity.role, &identity.sub, &body.records,
            )
            .await?,
        ));
    }

    let src = format!("ingress:{}", identity.sub);
    let declared = body.source_endpoint.clone();
    let src_endpoint = declared.clone().unwrap_or_else(|| src.clone());
    let ua = header_str(&headers, "user-agent");

    let params = IngestParams {
        target_schema: &schema,
        target_table: &table,
        source: &src,
        source_endpoint: &src_endpoint,
        submitted_by: Some(&identity.sub),
        run_id: None,
        declared_endpoint: declared.as_deref(),
        mode: "typed",
        user_agent: ua,
        validate: true,
        fire_lumilake: true,
    };
    let result = ingest_records(&st.backends, &params, &body.records)
        .await
        .map_err(ingest_err_to_api)?;
    invalidate_reads(&st, &result).await;
    if all_failed(&result) {
        return Err(ApiError::Validation(result.to_json()));
    }
    Ok(Json(result.to_json()))
}

// ---------------------------------------------------------------------------
// POST /ingest/{schema}/{table}/stream  (NDJSON; gzip/zstd via Content-Encoding)
// ---------------------------------------------------------------------------
const STREAM_FLUSH: usize = 10_000;

pub async fn post_stream(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let exists = gate_target(&st, &identity, &schema, &table).await?;
    if !exists {
        // Net-new sandbox stream path is out of scope.
        return Err(ApiError::NotFound(format!("unknown table: {schema}.{table}")));
    }

    let src = format!("ingress:{}", identity.sub);
    let declared = header_str(&headers, "x-ingress-source-endpoint").map(|s| s.to_string());
    let src_endpoint = declared.clone().unwrap_or_else(|| src.clone());
    let ua = header_str(&headers, "user-agent").map(|s| s.to_string());
    let content_encoding = header_str(&headers, "content-encoding").map(|s| s.to_string());

    let decoded = parsers::decode(body.to_vec(), content_encoding.as_deref())?;
    let records = parsers::parse_ndjson(&decoded)?;
    let received = records.len();

    // One run row spans the whole stream.
    let client = st.pool.get().await?;
    let args = json!({
        "target_schema": schema, "target_table": table, "mode": "stream",
        "declared_endpoint": declared, "user_agent": ua, "submitted_by": identity.sub,
    });
    let run_id = run::open_run(&client, "ingress:generic", &args, None).await?;
    run::set_submitted_by(&client, &run_id, &identity.sub).await?;
    drop(client);

    let mut inserted = 0i64;
    let mut updated = 0i64;
    let mut failed = 0usize;
    let mut rejected: Vec<Rejected> = Vec::new();
    let mut status = "ok".to_string();
    let mut error_text: Option<String> = None;

    for chunk in records.chunks(STREAM_FLUSH) {
        let params = IngestParams {
            target_schema: &schema,
            target_table: &table,
            source: &src,
            source_endpoint: &src_endpoint,
            submitted_by: Some(&identity.sub),
            run_id: Some(run_id),
            declared_endpoint: declared.as_deref(),
            mode: "stream",
            user_agent: ua.as_deref(),
            validate: true,
            fire_lumilake: false,
        };
        match ingest_records(&st.backends, &params, chunk).await {
            Ok(r) => {
                inserted += r.inserted;
                updated += r.updated;
                failed += r.failed;
                rejected.extend(r.rejected);
            }
            Err(e) => {
                status = "failed".to_string();
                let msg = format!("{}", ApiError::from(e));
                error_text = Some(msg[msg.len().saturating_sub(4000)..].to_string());
                break;
            }
        }
    }

    let final_status = if !rejected.is_empty() && status == "ok" {
        "partial".to_string()
    } else {
        status.clone()
    };

    let client = st.pool.get().await?;
    let _ = run::close_run(
        &client,
        &run_id,
        &final_status,
        inserted,
        updated,
        failed as i64,
        error_text.as_deref(),
    )
    .await;
    drop(client);

    if inserted + updated > 0 {
        st.read_cache.invalidate_table(&schema, &table).await;
    }

    let mut rejected_capped = rejected;
    rejected_capped.truncate(50); // don't blow the response on huge streams

    let body_out = json!({
        "run_id": run_id.to_string(),
        "target_schema": schema,
        "target_table": table,
        "received": received,
        "inserted": inserted,
        "updated": updated,
        "failed": failed,
        "rejected": rejected_capped,
        "status": final_status,
    });

    if status == "failed" {
        return Err(ApiError::BadRequest(
            serde_json::to_string(&body_out).unwrap_or_default(),
        ));
    }
    if (inserted + updated) > 0 {
        lumilake::submit_after_ingest(
            &IngestResult {
                run_id: run_id.to_string(),
                target_schema: schema.clone(),
                target_table: table.clone(),
                received,
                inserted,
                updated,
                failed,
                rejected: Vec::new(),
                status: final_status,
            },
            LumilakeInfo {
                target_schema: schema,
                target_table: table,
                mode: "stream".into(),
                declared_endpoint: declared,
                submitted_by: Some(identity.sub),
            },
        );
    }
    Ok(Json(body_out))
}

// ---------------------------------------------------------------------------
// POST /ingest/{schema}/{table}/file  (multipart; field `file`)
// ---------------------------------------------------------------------------
pub async fn post_file(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> ApiResult<Json<Value>> {
    let exists = gate_target(&st, &identity, &schema, &table).await?;
    if !exists {
        return Err(ApiError::NotFound(format!("unknown table: {schema}.{table}")));
    }

    // Pull the `file` part (+ optional `source_endpoint`).
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut file_ct: Option<String> = None;
    let mut file_name: Option<String> = None;
    let mut form_source_endpoint: Option<String> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::BadRequest(format!("multipart error: {e}")))?
    {
        match field.name() {
            Some("file") => {
                file_ct = field.content_type().map(|s| s.to_string());
                file_name = field.file_name().map(|s| s.to_string());
                let data = field
                    .bytes()
                    .await
                    .map_err(|e| ApiError::BadRequest(format!("file read: {e}")))?;
                file_bytes = Some(data.to_vec());
            }
            Some("source_endpoint") => {
                form_source_endpoint = field.text().await.ok().filter(|s| !s.is_empty());
            }
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    let body = file_bytes
        .ok_or_else(|| ApiError::BadRequest("multipart body must include a 'file' part".into()))?;
    let content_encoding = header_str(&headers, "content-encoding");
    let body = parsers::decode(body, content_encoding)?;

    let kind = parsers::kind_for(file_ct.as_deref(), file_name.as_deref())?;
    if !kind.is_structured() {
        return Err(ApiError::Unavailable(format!(
            "file kind={kind:?} is binary; use POST /ingest/blob for images/PDFs/opaque bytes"
        )));
    }
    if !kind.is_native() {
        return Err(ApiError::Unavailable(format!(
            "{kind:?} files are unsupported on this build (Python sidecar only)"
        )));
    }
    let records = parsers::parse_to_records(&body, kind)?;
    if records.is_empty() {
        return Ok(Json(json!({
            "run_id": "", "target_schema": schema, "target_table": table,
            "received": 0, "inserted": 0, "updated": 0, "failed": 0,
            "rejected": [], "status": "ok",
            "_note": format!("parsed 0 records from {:?}", file_name),
        })));
    }

    let src = format!("ingress:{}", identity.sub);
    let src_endpoint = form_source_endpoint
        .clone()
        .unwrap_or_else(|| format!("ingress:file/{}", kind_label(kind)));
    let ua = header_str(&headers, "user-agent");
    let mode = format!("file:{}", kind_label(kind));

    let params = IngestParams {
        target_schema: &schema,
        target_table: &table,
        source: &src,
        source_endpoint: &src_endpoint,
        submitted_by: Some(&identity.sub),
        run_id: None,
        declared_endpoint: form_source_endpoint.as_deref(),
        mode: &mode,
        user_agent: ua,
        validate: true,
        fire_lumilake: true,
    };
    let result = ingest_records(&st.backends, &params, &records)
        .await
        .map_err(ingest_err_to_api)?;
    invalidate_reads(&st, &result).await;
    if all_failed(&result) {
        return Err(ApiError::Validation(result.to_json()));
    }
    Ok(Json(result.to_json()))
}

fn kind_label(k: Kind) -> &'static str {
    match k {
        Kind::Json => "json",
        Kind::Ndjson => "ndjson",
        Kind::Csv => "csv",
        Kind::Tsv => "tsv",
        Kind::Xml => "xml",
        Kind::Yaml => "yaml",
        Kind::Parquet => "parquet",
        Kind::Arrow => "arrow",
        Kind::Blob => "blob",
        Kind::TextBlob => "text",
    }
}

// ---------------------------------------------------------------------------
// POST /ingest/blob  (raw bytes; metadata via X-Ingress-* headers)
// ---------------------------------------------------------------------------
pub async fn post_blob(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    if !blob::is_configured(&st.settings) {
        return Err(ApiError::Unavailable(
            "blob storage is not configured on this deployment".into(),
        ));
    }
    if body.is_empty() {
        return Err(ApiError::BadRequest("empty body — no bytes to store".into()));
    }
    if (body.len() as u64) > st.settings.blob_max_bytes {
        return Err(ApiError::BadRequest(format!(
            "blob too large ({} > {} bytes)",
            body.len(),
            st.settings.blob_max_bytes
        )));
    }

    let content_type = header_str(&headers, "content-type").map(|s| s.to_string());
    let suggested_name = header_str(&headers, "x-ingress-filename").map(|s| s.to_string());
    let declared_endpoint =
        header_str(&headers, "x-ingress-source-endpoint").map(|s| s.to_string());
    let metadata = match header_str(&headers, "x-ingress-metadata") {
        Some(raw) if !raw.is_empty() => match serde_json::from_str::<Value>(raw) {
            Ok(v @ Value::Object(_)) => Some(v),
            Ok(_) => None,
            Err(_) => {
                return Err(ApiError::BadRequest(
                    "X-Ingress-Metadata is not valid JSON".into(),
                ))
            }
        },
        _ => None,
    };
    let ua = header_str(&headers, "user-agent").map(|s| s.to_string());
    let src = format!("ingress:{}", identity.sub);
    let src_endpoint = declared_endpoint.clone().unwrap_or_else(|| src.clone());

    let result = blob::ingest_blob(
        &st.pool,
        &st.settings,
        &st.blob_store,
        &body,
        content_type.as_deref(),
        suggested_name.as_deref(),
        metadata,
        &src,
        &src_endpoint,
        &identity.sub,
        declared_endpoint.as_deref(),
        ua.as_deref(),
    )
    .await?;
    Ok(Json(result.to_json()))
}

// ---------------------------------------------------------------------------
// POST /webhook/{webhook_id}  (HMAC — ungated; mounted outside `gate`)
// ---------------------------------------------------------------------------
pub async fn post_webhook(
    State(st): State<AppState>,
    Path(webhook_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    if body.is_empty() {
        return Err(ApiError::BadRequest("empty body".into()));
    }
    let sig = header_str(&headers, "x-webhook-signature").unwrap_or("");
    let wh = webhook::authenticate(&st.pool, &webhook_id, &body, sig).await?;

    if wh.adapter_id.is_some() {
        // Adapter mode is a Python-sidecar concern.
        return Err(ApiError::Unavailable(
            "adapter-bound webhooks are handled by the Python sidecar on this build".into(),
        ));
    }
    let target_schema = wh
        .target_schema
        .clone()
        .ok_or_else(|| ApiError::BadRequest("webhook has no typed target".into()))?;
    let target_table = wh
        .target_table
        .clone()
        .ok_or_else(|| ApiError::BadRequest("webhook has no typed target".into()))?;

    let envelope: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;
    let records = match envelope.get("records") {
        Some(Value::Array(a)) if !a.is_empty() => a.clone(),
        _ => {
            return Err(ApiError::BadRequest(
                "`records` must be a non-empty list".into(),
            ))
        }
    };

    // Webhooks are admin-issued → ACL as 'local' (seeded blanket-allow).
    acl::check_can_write(&st.pool, "local", &target_schema, &target_table).await?;

    let src = format!("ingress:webhook:{}", wh.webhook_id);
    let declared = wh.source_endpoint.clone().unwrap_or_else(|| src.clone());
    let ua = header_str(&headers, "user-agent");

    let params = IngestParams {
        target_schema: &target_schema,
        target_table: &target_table,
        source: &src,
        source_endpoint: &declared,
        submitted_by: Some(&wh.owner_sub),
        run_id: None,
        declared_endpoint: Some(&declared),
        mode: "webhook",
        user_agent: ua,
        validate: true,
        fire_lumilake: false,
    };
    let result = ingest_records(&st.backends, &params, &records)
        .await
        .map_err(ingest_err_to_api)?;
    invalidate_reads(&st, &result).await;

    webhook::stamp_used(st.pool.clone(), wh.webhook_id);
    Ok(Json(result.to_json()))
}

// ---------------------------------------------------------------------------
// GET /catalog/tables/{schema}/{table}/schema.json
// ---------------------------------------------------------------------------
pub async fn get_table_schema_json(
    State(st): State<AppState>,
    Extension(_identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let client = st.pool.get().await?;
    let meta = introspect::table_meta(&client, &schema, &table)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("unknown table: {schema}.{table}")))?;
    Ok(Json(validation::schema_json_for(&schema, &table, &meta)))
}

// ---------------------------------------------------------------------------
// Admin: ACL grant/revoke + cache refresh (super_admin / local only)
// ---------------------------------------------------------------------------
pub(crate) fn require_admin(identity: &Identity) -> ApiResult<()> {
    if identity.role == "super_admin" || identity.role == "local" {
        Ok(())
    } else {
        Err(ApiError::Forbidden("super_admin or local-key required".into()))
    }
}

// ---- Ingress proposals (net-new shapes: propose → review → approve) ----
pub async fn list_proposals(
    State(st): State<AppState>,
    Extension(_identity): Extension<Identity>,
    axum::extract::RawQuery(q): axum::extract::RawQuery,
) -> ApiResult<Json<Value>> {
    let status = q
        .as_deref()
        .and_then(|qs| qs.split('&').find_map(|kv| kv.strip_prefix("status=")))
        .map(|s| s.to_string());
    Ok(Json(crate::ingest::proposals::list(&st.pool, status.as_deref()).await?))
}

/// Optional approve body: `{ "backend": "postgres" | "clickhouse" }`. Overrides
/// the proposal's stored `suggested_backend`. Absent ⇒ use the suggestion.
#[derive(Deserialize, Default)]
pub struct ApproveBody {
    #[serde(default)]
    pub backend: Option<String>,
}

pub async fn approve_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
    body: Option<Json<ApproveBody>>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    // Parse the optional backend override. An explicitly-supplied unknown value
    // is rejected rather than silently defaulting to Postgres, so a typo can't
    // quietly route a table to the wrong engine.
    let override_kind = match body.and_then(|Json(b)| b.backend) {
        Some(s) => {
            let want = s.trim().to_ascii_lowercase();
            match want.as_str() {
                "postgres" => Some(crate::backend::BackendKind::Postgres),
                "clickhouse" => Some(crate::backend::BackendKind::ClickHouse),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "backend must be 'postgres' or 'clickhouse' (got {s:?})"
                    )))
                }
            }
        }
        None => None,
    };
    Ok(Json(
        crate::ingest::proposals::approve(&st.backends, &proposal_id, &identity.sub, override_kind)
            .await?,
    ))
}

pub async fn reject_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    Ok(Json(crate::ingest::proposals::reject(&st.pool, &proposal_id, &identity.sub, None).await?))
}

/// Allow if the caller is an admin/local key OR the original proposer — so a
/// builder can drive the negotiation on their own proposal.
async fn allow_proposer_or_admin(st: &AppState, identity: &Identity, id: &str) -> ApiResult<()> {
    if identity.role == "super_admin" || identity.role == "local" {
        return Ok(());
    }
    if crate::ingest::proposals::is_proposer(&st.pool, id, &identity.sub).await? {
        return Ok(());
    }
    Err(ApiError::Forbidden("not the proposer (or an admin) of this proposal".into()))
}

/// `GET /catalog/ingress/proposals/{id}` — current schema + round history.
pub async fn get_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
) -> ApiResult<Json<Value>> {
    allow_proposer_or_admin(&st, &identity, &proposal_id).await?;
    Ok(Json(crate::ingest::proposals::get_detail(&st.pool, &proposal_id).await?))
}

#[derive(Deserialize)]
pub struct CounterBody {
    /// Suggested columns as `{name: pgtype}`.
    pub columns: Value,
    #[serde(default)]
    pub key: Vec<String>,
    /// Optional sample records to ground the (optional) LLM refine.
    #[serde(default)]
    pub sample_records: Vec<Value>,
}

/// `POST /ingress/proposals/{id}/counter` — builder counter-proposes a schema;
/// platform validates (+ optional LLM refine) and loops back to pending.
pub async fn counter_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
    Json(body): Json<CounterBody>,
) -> ApiResult<Json<Value>> {
    allow_proposer_or_admin(&st, &identity, &proposal_id).await?;
    Ok(Json(crate::ingest::proposals::counter(
        &st.pool, &st.settings, &st.http, &proposal_id, &identity.sub,
        &body.columns, &body.key, &body.sample_records,
    ).await?))
}

/// `POST /ingress/proposals/{id}/approve` — builder (or admin) applies the
/// current negotiated schema (CREATE table + grant ACL).
pub async fn builder_approve_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
) -> ApiResult<Json<Value>> {
    allow_proposer_or_admin(&st, &identity, &proposal_id).await?;
    // Builder approve uses the proposal's stored suggested backend (no override).
    Ok(Json(crate::ingest::proposals::approve(&st.backends, &proposal_id, &identity.sub, None).await?))
}

/// `POST /ingress/proposals/{id}/reject` — builder (or admin) abandons it.
pub async fn builder_reject_proposal(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(proposal_id): Path<String>,
) -> ApiResult<Json<Value>> {
    allow_proposer_or_admin(&st, &identity, &proposal_id).await?;
    Ok(Json(crate::ingest::proposals::reject(&st.pool, &proposal_id, &identity.sub, None).await?))
}

#[derive(Deserialize)]
pub struct GrantAclBody {
    pub role: String,
    pub target_schema: String,
    pub target_table: String,
    #[serde(default = "default_true")]
    pub can_write: bool,
    #[serde(default)]
    pub notes: Option<String>,
}

fn default_true() -> bool {
    true
}

pub async fn grant_acl(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(body): Json<GrantAclBody>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    let client = st.pool.get().await?;
    client
        .execute(
            "INSERT INTO provenance.ingress_acl \
                 (role, target_schema, target_table, can_write, notes) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (role, target_schema, target_table) DO UPDATE \
                SET can_write = EXCLUDED.can_write, notes = EXCLUDED.notes",
            &[
                &body.role,
                &body.target_schema,
                &body.target_table,
                &body.can_write,
                &body.notes,
            ],
        )
        .await?;
    acl::invalidate();
    Ok(Json(json!({
        "role": body.role, "target_schema": body.target_schema,
        "target_table": body.target_table, "can_write": body.can_write,
        "notes": body.notes, "_status": "applied",
    })))
}

#[derive(Deserialize)]
pub struct RevokeAclQuery {
    pub role: String,
    pub target_schema: String,
    pub target_table: String,
}

pub async fn revoke_acl(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Query(q): Query<RevokeAclQuery>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    let client = st.pool.get().await?;
    let n = client
        .execute(
            "DELETE FROM provenance.ingress_acl \
              WHERE role=$1 AND target_schema=$2 AND target_table=$3",
            &[&q.role, &q.target_schema, &q.target_table],
        )
        .await?;
    acl::invalidate();
    if n == 0 {
        return Err(ApiError::NotFound("no ACL row matched".into()));
    }
    Ok(Json(json!({
        "role": q.role, "target_schema": q.target_schema,
        "target_table": q.target_table, "deleted": n,
    })))
}

pub async fn refresh_schemas(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    introspect::refresh_cache();
    // Also clear the backend-resolve cache so a re-approved/migrated table picks
    // up its (possibly new) backend on the next request.
    st.backends.refresh_cache().await;
    Ok(Json(json!({"status": "cleared"})))
}

pub async fn refresh_acl(Extension(identity): Extension<Identity>) -> ApiResult<Json<Value>> {
    require_admin(&identity)?;
    acl::invalidate();
    Ok(Json(json!({"status": "cleared"})))
}

// Silence unused import in builds without the webhook adapter path.
#[allow(unused)]
type _UuidKeep = Uuid;
