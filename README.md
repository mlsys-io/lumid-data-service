# lumid-platform

A portable, domain-agnostic **data service platform** — read + write/ingest + realtime +
catalog/lineage + auth + MCP + an optional LLM reverse-proxy — packaged as one Rust library
crate (`lumid-platform`, in `crates/platform`). Applications embed it and add only their
domain: declarative read endpoints (config), any bespoke routes, and any realtime workers.

It is a **library, not a separate service**: an app statically links it into one binary/process.
(`mint` is a minimal reference app; a fuller app adds bespoke routes + realtime workers.)

## Build an app on it
The whole of `mint`'s `main.rs` (a config-only app — no domain Rust at all):
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Reads come from mint.toml; everything else is the platform's.
    lumid_platform::serve(lumid_platform::ServeParts::default()).await
}
```
`ServeParts::default()` is empty: no ext routes, no workers, no app landing → the platform
serves a **generic fallback landing** at `/`. A richer app fills in the fields it needs
(a fuller app's `main.rs`):
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lumid_platform::serve(lumid_platform::ServeParts {
        ext_routes:    my_ext::routes(),         // gated bespoke routes (merged into the auth'd group)
        public_routes: my_ext::public_routes(),  // un-gated routes (docs, self-auth WS upgrades, …)
        landing:       my_ext::landing_routes(), // overrides the generic `/`; adds /reference, /llm, /docs
        openapi_paths: my_ext::openapi_paths(),  // app routes to document in /openapi.json
        workers:       my_ext::workers(),         // realtime UpstreamWorkers
        enable_llm:    true,                           // mount the /v1 LLM proxy
    }).await
}
```
Every field has a sensible default (`Default::default()` for the routers/vec, `false` for
`enable_llm`, the generic platform landing for `landing`), so an app only sets what it adds.
`serve()` wires settings, DB pool, auth, Redis, the realtime hub + workers, the config-driven
read layer, the cache (+ cross-replica invalidation), auto-MCP, and the listener.

A new app therefore = **a config file + a thin `main` + a DB schema**:
1. **Database/schema** — set `LUMID_DB_*` (defaults to the shared warehouse); a new app
   typically uses its own Postgres **schema** (referenced by `schema.table` in its config + the
   tables it creates). No schema declaration in code — tables are introspected at write time.
2. **Reads** — a TOML of `[[read.endpoint]]` specs (`id`, `path`, `params`, `sql`, `ttl`,
   `shape`). Values bind as `:name` (always parameterized); `{{fragment}}` switches come only
   from author-controlled enum/presence maps. Pointed to via `LUMID_READ_CONFIG`.
3. **Bespoke** — anything SQL-in-config can't express: a compiled handler (gated via
   `ext_routes`, un-gated via `public_routes`), a realtime `UpstreamWorker` (via `workers`), a
   custom landing (`landing`), and OpenAPI entries for any of it (`openapi_paths`).

See the `mint` reference app for a complete platform-only example.

## What the platform provides (no app code)
- **Auth gate** — Lumid PAT + local-key bypass, tiered rate limit (emits
  `x-ratelimit-{limit,remaining,reset}` headers). Platform-owned public surfaces: `/health`,
  `/openapi.json` (generated OpenAPI 3.1), `/status`, `/usage`, `/freshness`, and a **generic
  fallback landing at `/`**. Apps add their own public routes via `ServeParts.public_routes`
  (e.g. `/usage.md`, `/skill.md`, and the self-authenticating WS upgrades) and replace
  the landing via `ServeParts.landing` (e.g. an app serving `/`, `/reference`, `/llm`, `/docs`,
  `/redoc`). The platform names no domain route — even `/quotes/stream` is the app's choice.
- **Write/ingest plane** — `POST /ingest/:schema/:table` (+ `/stream`, `/file`, `/blob`):
  table introspection, JSON-Schema validation, newest-wins upsert, full provenance, read-cache
  invalidation. Role-based ACL (`provenance.ingress_acl`, 3-tier wildcard).
- **Ingress proposals** — write to an unknown table (with propose rights) → schema inferred
  from the records → staged in `provenance.ingress_proposals`; admin `/catalog/ingress/proposals`
  + `/admin/ingress/proposals/:id/{approve,reject}` (approve CREATEs the table + grants ACL).
- **Catalog / lineage** — `/catalog/{schemas, tables/:s/:t, tables/:s/:t/schema.json, sources,
  submitters, ingress/writable, lineage/*}`.
- **Read cache** — L1 (moka, byte-weighted, single-flight) + L2 (Redis) + strong ETag/`304`,
  generation-based invalidation (inline on write + `cache:invalidate` pub/sub across replicas).
- **Realtime hub** — SSE/WS fan-out; `UpstreamWorker` trait (`start(hub, mux, settings, pool)`)
  is the IoC seam for domain feeds (the worker gets `pool`, so it can persist as well as publish).
  `Hub::warm(symbols)` (`LUMID_RT_WARM_SYMBOLS`) keeps a baseline subscribed so quote
  caches stay hot without an open client stream. The hub fans out app-declared **channel kinds**
  (`LUMID_RT_CHANNEL_KINDS`, e.g. `tick,news`) — it `PSUBSCRIBE`s `<kind>:*` and names no
  domain channel. The platform provides the generic SSE/WS *transport handlers*
  (`handlers::sse_quotes::quotes_stream`, `handlers::ws::{quotes,news}`) + `auth::extract_ws_token`;
  the app mounts them at its chosen paths (via `ext_routes`/`public_routes`) and adds any
  bespoke stream of its own.
- **Realtime health board** — workers report via `realtime::health::report` (WS link) /
  `report_feed` (data feed); `/status` renders feeds **measured by tick freshness + latency**
  (up / degraded / fail), and `/freshness` the per-endpoint SLA + per-source lag.
- **MCP** — `POST /mcp` (JSON-RPC 2.0); `mcp::registry_from_specs` auto-generates one tool per
  declarative read endpoint. `serverInfo.name` ← `LUMID_SERVICE_NAME` (default `lumid`).
- **LLM proxy (optional)** — enable via `ServeParts.enable_llm`; mounts the OpenAI/Anthropic-
  compatible `/v1/*` surface, **model-routed** across the primary `LUMID_LLM_BACKEND_URL` plus
  any extra backends in `LUMID_LLM_BACKENDS` (`model=url;model=url`). A request's `model` selects
  its backend (unknown / omitted → primary, with `LUMID_LLM_DEFAULT_MODEL` filled in);
  `GET /v1/models` aggregates the model list across all backends.

## Config (env, all `LUMID_*`)
DB: `LUMID_DB_{HOST,PORT,USER,PASSWORD,NAME}`, `LUMID_POOL_MAX`, `LUMID_STATEMENT_TIMEOUT_MS`.
Service: `LUMID_BIND_ADDR`, `LUMID_READ_CONFIG`, `LUMID_SERVICE_NAME`,
`LUMID_API_KEYS`, `LUMID_RATE_LIMIT_{ANON,AUTHED}`, `LUMID_REDIS_URL`,
`LUMID_AUTH_{ENABLED,URL}`, `LUMID_BLOB_ROOT`.
LLM proxy: `LUMID_LLM_BACKEND_URL` (primary) + `LUMID_LLM_DEFAULT_MODEL` + `LUMID_LLM_BACKENDS`
(`model=url;model=url` — extra model-routed backends).
Realtime: `LUMID_RT_CHANNEL_KINDS` (hub channel kinds, default `tick,news`),
`LUMID_RT_WARM_SYMBOLS`, `LUMID_RT_{HEARTBEAT_SEC,SSE_REQUEST_SYMS,SLOWCLIENT_QUEUE,…}`.
Storage backends (optional): `LUMID_CLICKHOUSE_*` registers a ClickHouse backend alongside
Postgres (per-table routing via `provenance.table_backend`); `LUMID_BLOB_BACKEND=s3` +
`LUMID_BLOB_S3_*` puts blobs in MinIO/S3.
Provider keys/caps + domain channel kinds for an app's workers live in the **app**
(e.g. `my_ext::cfg`), not here.

## Dev layout
Two repos, one binary. Clone as siblings:
```
/parent/lumid-data-service   (this repo — crates/platform → crate `lumid-platform`)
/parent/myapp                (your app — depends on ../lumid-data-service via path dep)
```
The platform stays generic and names no domain provider; all domain naming lives in the app, never here.
