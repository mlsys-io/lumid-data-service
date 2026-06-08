//! Router assembly.
//!
//! `/health` + `/health/db` are public. All data routes go through the `gate`
//! middleware (require identity + tiered rate limit). Phase 1 wires the canary
//! read set (symbols, ohlc, fundamentals, news, freshness); later phases append
//! the remaining endpoints + the write plane + the proxy gateway.

use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(
    state: AppState,
    read_router: Router<AppState>,
    ext_router: Router<AppState>,
    openapi_router: Router<AppState>,
    public_ext_router: Router<AppState>,
    landing_router: Router<AppState>,
) -> Router {
    let public = Router::new()
        .route("/health", get(handlers::health::health))
        .route("/health/db", get(handlers::health::health_db))
        .route("/health/ready", get(handlers::health::health_ready))
        .merge(openapi_router) // GET /openapi.json (public)
        .merge(public_ext_router) // app-contributed public routes (e.g. /usage.md)
        // Public landing surfaces (no auth) — app-contributed (`ServeParts.landing`)
        // or the platform's generic fallback (`GET /`). The platform names no
        // domain, so the app-provided landing/reference/llm pages live in the app.
        .merge(landing_router)
        // Status board: browsable HTML, intentionally public (aggregate counts only).
        .route("/status", get(handlers::freshness::status))
        // Webhook ingress: HMAC-authenticated, mounted OUTSIDE the gate.
        .route("/webhook/:webhook_id", post(handlers::ingest::post_webhook));
        // Realtime WebSocket routes (self-authenticating, outside the gate) are
        // app-contributed via `ServeParts.public_routes` — the platform exposes
        // the generic `ws::{quotes,news}` transport handlers but names no path.

    let gated = Router::new()
        // Catalog read plane (provenance-exposing) — port of api/routes/catalog.py.
        .route("/catalog/schemas", get(handlers::catalog::get_schemas))
        .route("/catalog/schemas/:schema/tables", get(handlers::catalog::get_schema_tables))
        .route("/catalog/tables/:schema/:table", get(handlers::catalog::get_table_profile))
        .route("/catalog/ingress/writable", get(handlers::catalog::get_writable))
        .route("/catalog/lineage/run/:run_id", get(handlers::catalog::get_lineage_run))
        .route("/catalog/lineage/runs", get(handlers::catalog::get_lineage_runs))
        .route("/catalog/lineage/row", get(handlers::catalog::get_lineage_row))
        .route("/catalog/sources", get(handlers::catalog::get_sources))
        .route("/catalog/submitters", get(handlers::catalog::get_submitters))
        .route("/catalog/tables/:schema/:table/schema.json", get(handlers::ingest::get_table_schema_json))

        // Ingress write plane is merged below with a bounded body limit.

        // Usage dashboard (global board + per-caller view) — gated: principal IDs
        // and pipeline health counts shouldn't be unauthenticated (SEC-004).
        .route("/usage", get(handlers::usage::usage))
        .route("/usage/me", get(handlers::usage::usage_me))
        // Freshness JSON — gated: exposes endpoint SLA health details.
        .route("/freshness", get(handlers::freshness::freshness))
        // Ingress proposals: write to an unknown table → infer schema + stage a
        // proposal; admin lists/approves (creates the table + grants ACL).
        .route("/catalog/ingress/proposals", get(handlers::ingest::list_proposals))
        .route("/catalog/ingress/proposals/:id", get(handlers::ingest::get_proposal))
        // Builder-driven negotiation (proposer or admin): counter / approve / reject.
        .route("/ingress/proposals/:id/counter", post(handlers::ingest::counter_proposal))
        .route("/ingress/proposals/:id/approve", post(handlers::ingest::builder_approve_proposal))
        .route("/ingress/proposals/:id/reject", post(handlers::ingest::builder_reject_proposal))
        .route("/admin/ingress/proposals/:id/approve", post(handlers::ingest::approve_proposal))
        .route("/admin/ingress/proposals/:id/reject", post(handlers::ingest::reject_proposal))
        // Export: paginated NDJSON dump of any table, backend-agnostic.
        // Used for single-port cross-instance migration (see handlers/export.rs).
        .route("/admin/export/:schema/:table", get(handlers::export::get_export))
        // Ingress admin (super_admin / local key) — port of injection/routes/ingest_admin.py.
        .route("/admin/ingress/acl", post(handlers::ingest::grant_acl).delete(handlers::ingest::revoke_acl))
        .route("/admin/ingress/refresh-schemas", post(handlers::ingest::refresh_schemas))
        .route("/admin/ingress/refresh-acl", post(handlers::ingest::refresh_acl))

        // LLM reverse proxy is now an opt-in plugin — apps merge
        // `lumid_platform::llm::routes()` (src/llm.rs). Not mounted by the platform.

        // Direct SQL/storage retrieval — no LLM; same safety boundary as replay_retrieval_plan.
        .route("/retrieve", post(handlers::retrieve::post_retrieve))

        // EXPLAIN-based query cost estimation — feeds the HALO cost model in lumilake.
        // Same safety boundary as /retrieve: SELECT-only parser, READ ONLY txn,
        // statement timeout, optional db role. EXPLAIN is plain (no ANALYZE).
        .route("/profile", post(handlers::profile::post_profile))

        // Blob serving (read side). The generic `/blobs/*key` is platform-owned;
        // any domain-named compatibility alias (e.g. a legacy `/storage/...` URL)
        // is app-contributed via `ServeParts.ext_routes` → `blobs::legacy_storage_alias`.
        // Exact `/blobs` (list) here; wildcard `/blobs/*key` (fetch/write/delete) is
        // merged below with a body limit — coexists with this exact route in axum.
        .route("/blobs", get(handlers::blobs::list_blobs))

        // Realtime SSE routes (gated) are app-contributed via `ServeParts.ext_routes`
        // — the platform exposes the generic `sse_quotes::quotes_stream` handler
        // but names no path.

        // Ingress write plane — bounded body limit (batch NDJSON/file/blob need
        // more than axum's 2 MB default, but not unbounded → OOM/DoS guard).
        .merge(
            Router::new()
                .route("/ingest/:schema/:table", post(handlers::ingest::post_typed))
                .route("/ingest/:schema/:table/stream", post(handlers::ingest::post_stream))
                .route("/ingest/:schema/:table/file", post(handlers::ingest::post_file))
                .route("/ingest/blob", post(handlers::ingest::post_blob))
                .layer(axum::extract::DefaultBodyLimit::max(state.settings.ingest_max_bytes as usize)),
        )
        // Blob fetch/write/delete by key — bounded body limit (PUT bodies need
        // more than axum's 2 MB default; capped at blob_max_bytes). PUT/DELETE are
        // privileged (lumilake:write scope / local key); GET serves bytes (no body limit effect).
        .merge(
            Router::new()
                .route(
                    "/blobs/*key",
                    get(handlers::blobs::serve_blob)
                        .put(handlers::blobs::put_blob)
                        .delete(handlers::blobs::delete_blob),
                )
                .layer(axum::extract::DefaultBodyLimit::max(state.settings.blob_max_bytes as usize)),
        )
        .merge(read_router)
        .merge(ext_router)
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
