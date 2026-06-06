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
        enable_agent:  true,                           // mount the /agent/v1 tool-use loop (needs enable_llm)
        enable_sync:   true,                           // mount the data-push plane (/sync/apply, /admin/sync/*)
    }).await
}
```
Every field has a sensible default (`Default::default()` for the routers/vec, `false` for
the `enable_*` flags, the generic platform landing for `landing`), so an app only sets what it adds.
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
- **Data-push / sync plane (optional)** — enable via `ServeParts.enable_sync`; mounts the
  **inbox** `POST /sync/apply/:schema/:table` and the optional push helper
  (`POST /admin/sync/push`, `GET /admin/sync/status`), and creates the `sync` bookkeeping tables
  at boot. Lets one instance ship rows to another (**fan-in: N producers → one inbox**),
  preserving the original provenance lineage and invalidating the target read cache. One-off /
  repeatable, not a live stream. See **Data migration / cross-instance sync** below.

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
Data-push / sync (optional, with `enable_sync`): `LUMID_SYNC_TARGET_URL` + `LUMID_SYNC_TARGET_TOKEN`
+ `LUMID_SYNC_PEER_ID` (push side), `LUMID_SYNC_PEER_LABELS` (inbox allowlist), and
`LUMID_SYNC_{BATCH_ROWS,MAX_ATTEMPTS,BACKOFF_MS}`.
Provider keys/caps + domain channel kinds for an app's workers live in the **app**
(e.g. `my_ext::cfg`), not here.

## Data migration / cross-instance sync
A generic push-based way to move rows from one instance (a **producer**) to another (a **target**),
preserving the original provenance lineage. Fan-in: many producers → one target inbox. It is
one-off / repeatable (re-run to push more) — there are no triggers and no always-on relay.

**Exactly-once-effective**: at-least-once delivery + per-`(peer, batch_id)` dedup in
`sync.inbox_ledger` + the idempotent upsert merge. Re-running a push, or redelivering a batch, is
safe. The push cursor advances only on a durable ACK, so a target outage just pauses progress.

### App side (config only — no app code)
Both roles are the *same binary* with `ServeParts.enable_sync = true`; the role is chosen by config.

- **Target (inbox).** Set `enable_sync`. Issue the producer a local key whose **label starts with
  `sync:`** so it authenticates as a sync peer:
  ```bash
  LUMID_API_KEYS=...,<token>:sync:findata   # peer id = "findata"
  # optional explicit allowlist (else any `sync:` label is accepted):
  LUMID_SYNC_PEER_LABELS=sync:findata,sync:worker7
  ```
  The inbox refuses writes to `sync.*`, `provenance.*`, and system schemas.
- **Producer (push helper).** Set `enable_sync` plus where to push:
  ```bash
  LUMID_SYNC_TARGET_URL=https://target.example   # the target instance
  LUMID_SYNC_TARGET_TOKEN=<token>                # the target's sync: local key
  LUMID_SYNC_PEER_ID=findata                     # this instance's id
  LUMID_SYNC_BATCH_ROWS=1000                     # optional
  ```
  A producer with no local table to drain (e.g. a stateless worker) doesn't need the push helper —
  it just POSTs batches to the target inbox directly (contract below).

At boot, `enable_sync` runs `sync::migrate` (idempotent) to create `sync.inbox_ledger` (target) and
`sync.push_cursor` (producer). No DDL to run by hand.

### Run a migration (one-off table push)
From the **producer**, drain a table to the target and watch progress:
```bash
# admin/local key required
curl -sX POST $PRODUCER/admin/sync/push \
  -H 'Authorization: Bearer <admin-or-local-key>' -H 'Content-Type: application/json' \
  -d '{"schema":"monitoring","table":"health_checks","watermark_col":"ts"}'
curl -s $PRODUCER/admin/sync/status -H 'Authorization: Bearer <admin-or-local-key>'
```
- `watermark_col` (default `ingest_ts`) must be a **timestamp** column; the helper pages by a keyset
  `(watermark, ctid)` so rows sharing a timestamp are never skipped, checkpoints to `sync.push_cursor`,
  and is resumable. Re-running continues from the cursor and re-pushes nothing already delivered.
- Each page is grouped by `source_run_id` so every shipped batch is lineage-homogeneous; the helper
  ships the run's provenance preamble (`api_sources → endpoints → runs`) ahead of the data.

### Inbox contract (for a direct/stateless producer)
```
POST {target}/sync/apply/{schema}/{table}
Authorization: Bearer <target sync: token>
X-Lumid-Sync-Peer: <peer id>
{
  "batch_id": "<uuid, stable per batch for dedup>",
  "source": "...", "source_endpoint": "...", "source_run_id": "<uuid>",
  "provenance": { "api_sources": [ {...} ], "endpoints": [ {...} ], "runs": [ {...} ] },
  "records": [ { ...row... }, ... ]
}
```
The batch is lineage-homogeneous (one triplet for all `records`). The `provenance.runs` row **must**
be present (shipped here or already on the target) — the inbox adopts `source_run_id` rather than
minting a new run, so the FK requires it; a missing run returns `400`. Provenance rows are upserted
verbatim (`ON CONFLICT DO NOTHING`). Response: `{batch_id, inserted, updated, failed, duplicate}`.
A fully-invalid batch returns `422` (nothing recorded); a partial one ACKs with `failed > 0`.

## Dev layout
Two repos, one binary. Clone as siblings:
```
/parent/lumid-data-service   (this repo — crates/platform → crate `lumid-platform`)
/parent/myapp                (your app — depends on ../lumid-data-service via path dep)
```
The platform stays generic and names no domain provider; all domain naming lives in the app, never here.
