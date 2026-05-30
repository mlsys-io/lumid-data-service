# lumid-platform

A portable, domain-agnostic **data service platform** — read + write/ingest + realtime +
catalog/lineage + auth + MCP + an optional LLM reverse-proxy — packaged as one Rust library
crate (`lumid-platform`, in `crates/platform`). Applications embed it and add only their
domain: declarative read endpoints (config), any bespoke routes, and any realtime workers.

It is a **library, not a separate service**: an app statically links it into one binary/process.
(`findata` is one such app; `mint` is a minimal reference app.)

## Build an app on it
A whole app's `main.rs`:
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lumid_platform::serve(lumid_platform::ServeParts {
        ext_routes: my_ext::routes(),     // or axum::Router::new()
        workers:    my_ext::workers(),    // or vec![]
        enable_llm: false,                // platform LLM proxy, opt-in per app
    }).await
}
```
`serve()` wires settings, DB pool, auth, Redis, the realtime hub + workers, the config-driven
read layer, the cache (+ cross-replica invalidation), auto-MCP, and the listener.

A new app therefore = **a config file + a ~6-line `main` + a DB schema**:
1. **Database/schema** — set `FINDATA_DB_*` (defaults to the shared warehouse); a new app
   typically uses its own Postgres **schema** (referenced by `schema.table` in its config + the
   tables it creates). No schema declaration in code — tables are introspected at write time.
2. **Reads** — a TOML of `[[read.endpoint]]` specs (`id`, `path`, `params`, `sql`, `ttl`,
   `shape`). Values bind as `:name` (always parameterized); `{{fragment}}` switches come only
   from author-controlled enum/presence maps. Pointed to via `FINDATA_FINANCIAL_CONFIG`.
3. **Bespoke** — anything SQL-in-config can't express: a compiled handler (merged via
   `ext_routes`) or a realtime `UpstreamWorker` (added via `workers`).

See the `mint` app (in the `findata` repo) for a complete platform-only example.

## What the platform provides (no app code)
- **Auth gate** — Lumid PAT + local-key bypass, tiered rate limit (emits
  `x-ratelimit-{limit,remaining,reset}` headers). Public surfaces: `/health`, `/`,
  `/reference`, `/openapi.json` (generated OpenAPI 3.1), `/llm`, `/status`, `/usage`,
  `/freshness`, `/docs`→`/reference`. Apps add their own public routes via
  `ServeParts.public_routes` (findata serves `/usage.md`, `/skill.md` this way).
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
  `Hub::warm(symbols)` (`FINDATA_RT_WARM_SYMBOLS`) keeps a baseline subscribed so quote
  caches stay hot without an open client stream.
- **Realtime health board** — workers report via `realtime::health::report` (WS link) /
  `report_feed` (data feed); `/status` renders feeds **measured by tick freshness + latency**
  (up / degraded / fail), and `/freshness` the per-endpoint SLA + per-source lag.
- **MCP** — `POST /mcp` (JSON-RPC 2.0); `mcp::registry_from_specs` auto-generates one tool per
  declarative read endpoint. `serverInfo.name` ← `FINDATA_SERVICE_NAME` (default `lumid`).
- **LLM proxy (optional)** — enable via `ServeParts.enable_llm`; mounts the OpenAI/Anthropic-
  compatible `/v1/*` surface proxying to `FINDATA_LLM_BACKEND_URL`.

## Config (env, all `FINDATA_*`)
DB: `FINDATA_DB_{HOST,PORT,USER,PASSWORD,NAME}`, `FINDATA_POOL_MAX`, `FINDATA_STATEMENT_TIMEOUT_MS`.
Service: `FINDATA_BIND_ADDR`, `FINDATA_FINANCIAL_CONFIG`, `FINDATA_SERVICE_NAME`,
`FINDATA_API_KEYS`, `FINDATA_RATE_LIMIT_{ANON,AUTHED}`, `FINDATA_REDIS_URL`,
`FINDATA_LUMID_{ENABLED,URL}`, `FINDATA_BLOB_ROOT`, `FINDATA_LLM_BACKEND_URL`.
Provider keys/caps for an app's workers live in the **app** (e.g. `findata-ext::cfg`), not here.

## Dev layout
Two repos, one binary. Clone as siblings:
```
/parent/lumid-data-service   (this repo — crates/platform → crate `lumid-platform`)
/parent/findata              (the financial app — depends on ../lumid-data-service via path dep)
```
The platform stays generic and names no domain provider; financial naming lives only in `findata`.
