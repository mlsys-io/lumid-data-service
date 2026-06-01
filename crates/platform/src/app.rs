//! Router assembly.
//!
//! `/health` + `/health/db` are public. All data routes go through the `gate`
//! middleware (require identity + tiered rate limit). Phase 1 wires the canary
//! read set (symbols, ohlc, fundamentals, news, freshness); later phases append
//! the remaining endpoints + the write plane + the proxy gateway.

use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use axum::Router;
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
        .merge(openapi_router) // GET /openapi.json (public)
        .merge(public_ext_router) // app-contributed public routes (e.g. /usage.md)
        // Public landing surfaces (no auth) — app-contributed (`ServeParts.landing`)
        // or the platform's generic fallback (`GET /`). The platform names no
        // domain, so the financial landing/reference/llm pages live in the app.
        .merge(landing_router)
        // Status board + usage dashboard: browsable HTML, public.
        .route("/status", get(handlers::freshness::status))
        .route("/usage", get(handlers::usage::usage))
        .route("/freshness", get(handlers::freshness::freshness))
        // Webhook ingress: HMAC-authenticated, mounted OUTSIDE the gate.
        .route("/webhook/:webhook_id", post(handlers::ingest::post_webhook))
        // WebSocket realtime: self-authenticating (the WS upgrade can't carry
        // the gate's 401 body), so mounted OUTSIDE the gate.
        .route("/ws/quotes", get(handlers::ws::quotes))
        .route("/ws/news", get(handlers::ws::news))
        .route("/ws/prediction-markets", get(handlers::ws::prediction_markets));

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

        // Caller's own usage (authed; the public /usage is the global board).
        .route("/usage/me", get(handlers::usage::usage_me))
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
        // Ingress admin (super_admin / local key) — port of injection/routes/ingest_admin.py.
        .route("/admin/ingress/acl", post(handlers::ingest::grant_acl).delete(handlers::ingest::revoke_acl))
        .route("/admin/ingress/refresh-schemas", post(handlers::ingest::refresh_schemas))
        .route("/admin/ingress/refresh-acl", post(handlers::ingest::refresh_acl))

        // LLM reverse proxy is now an opt-in plugin — apps merge
        // `lumid_platform::llm::routes()` (src/llm.rs). Not mounted by the platform.

        // Government trades — port of api/routes/gov_trades.py.

        // Blob serving (read side) — port of api/routes/blobs.py.
        .route("/blobs/*key", get(handlers::blobs::serve_blob))
        .route("/storage/v1/object/findata/*path", get(handlers::blobs::legacy_storage_alias))

        // Realtime SSE (normal GET → gated through the auth middleware).
        .route("/quotes/stream", get(handlers::sse_quotes::quotes_stream))
        .route("/prediction-markets/stream", get(handlers::pm_stream::stream))

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
        .merge(read_router)
        .merge(ext_router)
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
