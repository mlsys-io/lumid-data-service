//! Router assembly.
//!
//! `/health` + `/health/db` are public. All data routes go through the `gate`
//! middleware (require identity + tiered rate limit). Phase 1 wires the canary
//! read set (symbols, ohlc, fundamentals, news, freshness); later phases append
//! the remaining endpoints + the write plane + the proxy gateway.

use axum::middleware::from_fn_with_state;
use axum::routing::{delete, get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(
    state: AppState,
    read_router: Router<AppState>,
    ext_router: Router<AppState>,
) -> Router {
    let public = Router::new()
        .route("/health", get(handlers::health::health))
        .route("/health/db", get(handlers::health::health_db))
        // Public landing surfaces (no auth) — ports of api/landing.py + llm_landing.py.
        .route("/", get(handlers::landing::landing))
        .route("/reference", get(handlers::landing::landing))
        .route("/llm", get(handlers::landing::llm_landing))
        // Status board + usage dashboard: browsable HTML linked from the public
        // landing, so public (matches Python: bare @app.get, no auth dep).
        .route("/status", get(handlers::freshness::status))
        .route("/usage", get(handlers::usage::usage))
        .route("/freshness", get(handlers::freshness::freshness))
        // Doc aliases linked from the landing. FastAPI's /docs + /redoc weren't
        // ported (no OpenAPI spec generator on the Rust side); /reference is the
        // canonical API reference, so redirect the legacy paths there.
        .route("/docs", get(|| async { axum::response::Redirect::permanent("/reference") }))
        .route("/redoc", get(|| async { axum::response::Redirect::permanent("/reference") }))
        // Webhook ingress: HMAC-authenticated, mounted OUTSIDE the gate.
        .route("/webhook/:webhook_id", post(handlers::ingest::post_webhook))
        // WebSocket realtime: self-authenticating (the WS upgrade can't carry
        // the gate's 401 body), so mounted OUTSIDE the gate.
        .route("/ws/quotes", get(handlers::ws::quotes))
        .route("/ws/news", get(handlers::ws::news));

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

        // Ingress write plane — port of injection/routes/ingest_*.py.
        .route("/ingest/:schema/:table", post(handlers::ingest::post_typed))
        .route("/ingest/:schema/:table/stream", post(handlers::ingest::post_stream))
        .route("/ingest/:schema/:table/file", post(handlers::ingest::post_file))
        .route("/ingest/blob", post(handlers::ingest::post_blob))

        // Ingress admin (super_admin / local key) — port of injection/routes/ingest_admin.py.
        .route("/admin/ingress/acl", post(handlers::ingest::grant_acl).delete(handlers::ingest::revoke_acl))
        .route("/admin/ingress/refresh-schemas", post(handlers::ingest::refresh_schemas))
        .route("/admin/ingress/refresh-acl", post(handlers::ingest::refresh_acl))

        // LLM reverse proxy (OpenAI + Anthropic compatible) — port of api/routes/llm.py.
        .route("/v1/models", get(handlers::llm::list_models))
        .route("/v1/chat/completions", post(handlers::llm::chat_completions))
        .route("/v1/completions", post(handlers::llm::completions))
        .route("/v1/embeddings", post(handlers::llm::embeddings))
        .route("/v1/messages", post(handlers::llm::messages))
        .route("/v1/messages/count_tokens", post(handlers::llm::count_tokens))

        // Government trades — port of api/routes/gov_trades.py.

        // Blob serving (read side) — port of api/routes/blobs.py.
        .route("/blobs/*key", get(handlers::blobs::serve_blob))
        .route("/storage/v1/object/findata/*path", get(handlers::blobs::legacy_storage_alias))

        // KOL media — port of api/routes/kol_media.py.
        .route("/kols/media", get(handlers::kol_media::info))
        .route("/kols/media/by-url", get(handlers::kol_media::by_url))
        .route("/kols/media/*rel", get(handlers::kol_media::serve))

        // KOL roster admin (super_admin / local) — port of api/routes/admin_kols.py.
        .route("/admin/kols", post(handlers::admin_kols::add_kol))
        .route("/admin/kols/:handle", delete(handlers::admin_kols::remove_kol))

        // Realtime SSE (normal GET → gated through the auth middleware).
        .route("/quotes/stream", get(handlers::sse_quotes::quotes_stream))
        .route("/prediction-markets/stream", get(handlers::pm_stream::stream))

        .merge(read_router)
        .merge(ext_router)
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
