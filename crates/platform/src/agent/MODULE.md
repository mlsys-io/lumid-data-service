# agent — tool-use loop module

## Purpose

Extends `enable_llm` with an agentic data-retrieval surface. The LLM does **not** receive data
rows in its context. Instead it plans a retrieval, the platform executes it deterministically,
and the result is **materialized to object storage** — the response carries a pointer
(`RetrievalResult`), never the payload.

## Module layout

```
src/agent/
  mod.rs          — re-exports + route builder (routes())
  agent_loop.rs   — run_loop() + axum handler (agent_chat)
  tools/mod.rs    — Tool trait, two tool impls, ToolRegistry, AgentConfig, dispatch()
  tools/schema_cards.rs — get_schema_cards
  tools/replay.rs       — replay_retrieval_plan
src/retrieve/     — the deterministic retrieval pipeline the tools call into
  schema_card.rs / card_builder.rs / card_store.rs — schema cards (+ TTL cache)
  plan.rs         — RetrievalPlan, RetrievalOp, AccessChainStep, RetrievalResult, SQL safety
  replayer.rs     — executes a plan (read-only txn, row cap), builds RetrievalResult
  materialize.rs  — writes csv/jsonl/raw to the object store, returns the /blobs/<key> URI
MODULE.md         — this file
```

## Direct retrieval endpoint

The same `retrieve::replayer::replay` engine is also exposed without the LLM at
`POST /retrieve` (gated, auth-required). Clients that already know the SQL or
storage key can skip the agent loop entirely and get a deterministic `RetrievalResult`.

## Query cost profiling endpoint

`POST /profile` (gated, auth-required) returns EXPLAIN-based cost estimates for a
SQL query across one or more planner-variant GUC sets. The query is **never executed**
(plain EXPLAIN, no ANALYZE). Response feeds the HALO cost model in lumilake.

```
POST /profile
{ "sql": "SELECT ...",
  "plans": [{"plan_id": "default", "settings": {}},
            {"plan_id": "prefer_index", "settings": {"enable_seqscan": "off"}}] }

→ { "variants": [{"plan_id": "default", "raw_cost": 1234.56,
                   "estimated_rows": 1000,
                   "footprints": {"public.t": 450, "idx_x": 60},
                   "explain_json": {...}}] }
```

Identical safety boundary as `/retrieve`:
- `retrieve::plan::is_safe_select` rejects non-SELECT SQL.
- `SET TRANSACTION READ ONLY` at the DB level.
- `SET LOCAL statement_timeout` and optional `SET LOCAL ROLE` from config.
- GUC keys restricted to `enable_*` planner toggles (explicit allowlist); values
  restricted to `"on"` / `"off"`. Anything else is rejected 400.

## Wire protocol

```
POST /agent/v1
Authorization: Bearer <pat>
Content-Type: application/json

{
  "messages":       [...],   // OpenAI chat-completions messages array
  "model":          "...",   // optional; falls back to LUMID_LLM_DEFAULT_MODEL
  "max_iterations": 10       // optional; hard cap is LUMID_AGENT_MAX_ITERATIONS
}
```

Response: `text/event-stream`, one `data: {...}` SSE frame per loop event:

| frame type        | when                                             |
|-------------------|--------------------------------------------------|
| `iteration`       | each loop iteration starts                       |
| `tool_call`       | LLM issued a tool call                           |
| `tool_result`     | tool returned successfully                       |
| `tool_error`      | tool returned an error (loop continues)          |
| `error`           | LLM call failed (stream terminates)              |
| `done`            | final assistant message; carries `result`        |

The `done` frame includes a top-level `result` field holding the `RetrievalResult` from the
last successful `replay_retrieval_plan` call (or `null` if the agent never materialized one).

## Tool surface (2 tools)

All tools are OpenAI function-call compatible (schema under `function.parameters`).

| tool                    | required args | backed by                                | boundary                                   |
|-------------------------|---------------|------------------------------------------|--------------------------------------------|
| `get_schema_cards`      | — (`scope?`)  | `retrieve::card_builder` (+ TTL cache)   | card visibility capped to `LUMID_USER_SCHEMAS` (does NOT constrain SQL) |
| `replay_retrieval_plan` | `plan`        | `retrieve::replayer::replay`             | SELECT-only parser + READ ONLY txn + row cap + key sanitization |

### get_schema_cards

Returns compact schema cards (table/column names, types, stats, samples, FK hints) for SQL
planning — never data rows. `LUMID_USER_SCHEMAS` is a **card-visibility cap** only — it
controls which schemas the agent is *shown*, not which schemas the SQL it generates may
touch (the DB role's grants are the data-access boundary). When non-empty, the effective
scope is `requested_scope ∩ user_schemas`; when the caller's `scope` arg is empty, the
full allowlist is used. When `LUMID_USER_SCHEMAS` is unset, all non-system schemas are
shown (system schemas — `pg_*`, `information_schema`, `_timescaledb*` — are always
excluded). Cards are cached in process with a TTL (`LUMID_RETRIEVAL_CARD_TTL_S`).

### replay_retrieval_plan

Accepts a `RetrievalPlan` (`{plan: [{op:"sql", query} | {op:"storage_get", key}]}`) and an
optional `output_format` (`csv`|`jsonl`|`raw`). Executes each op in order, streams results into
the materializer, writes the output as a new object under `<LUMID_RETRIEVAL_PREFIX>/<run_id>/`,
and returns a `RetrievalResult`:

```
{ run_id, materialized_uri, signed_url, output_format, access_chain[],
  rowcount, size_bytes, tokens_in, tokens_out, steps_taken, replay_latency_ms, transcript_url }
```

`materialized_uri` is the **app-relative fetch path** `/blobs/<key>` — the consumer does
`GET {base_url}{materialized_uri}` with the same bearer. The response exposes nothing about the
storage backend (no `s3://`, no bucket name); `access_chain` steps carry `key` but not `bucket`.

## SECURITY

### Read-only boundary on plan execution

`replay_retrieval_plan` enforces three controls before any SQL runs (`retrieve::replayer` +
`retrieve::plan`):

- **SELECT-only parser** (`plan::is_safe_select`): strips comments, requires a single statement
  starting with `select`, and rejects DML/DDL keywords (`insert/update/delete/merge/copy/
  create/drop/alter/truncate/grant/revoke/call/execute`). A leading `WITH` is rejected, so
  writable CTEs cannot slip through.
- **READ ONLY transaction**: each SELECT runs inside `BEGIN; SET TRANSACTION READ ONLY; SET
  LOCAL statement_timeout = …` so Postgres itself rejects any write (defense beyond the parser)
  and the statement timeout actually binds to the query's transaction.
- **Row cap**: the query is wrapped `SELECT * FROM (<q>) LIMIT <cap+1>`; exceeding
  `LUMID_RETRIEVAL_ROW_CAP` is rejected before materializing, bounding memory.

`storage_get` keys are sanitized (`handlers::blobs::sanitize_blob_key`) — `..`, absolute paths,
and prefix traversal are rejected — and reads go through the bucket-scoped object store.

### DB role hardening (deployment guidance)

The agent uses the platform's general connection pool. Running it under a minimally-privileged
role with `USAGE` on user schemas only (and no write grants) is recommended defense-in-depth;
the READ ONLY transaction is the control that makes execution safe, but a least-privilege role
is appropriate whenever a role touches production data. Note: `LUMID_USER_SCHEMAS` caps card
*visibility* only — the DB role is the authoritative data-access boundary for SQL execution.

## LLM integration

`run_loop` calls `POST {LUMID_LLM_BACKEND_URL}/v1/chat/completions` using `AppState.http` (the
same `reqwest::Client` the `enable_llm` proxy uses). When `LUMID_LLM_API_KEY` is set it injects
`Authorization: Bearer <key>` (required for hosted endpoints like `api.anthropic.com`). The
request uses `"stream": false` and the OpenAI chat-completions tool-call format.

## Iteration limit and error handling

The loop iterates up to `min(request.max_iterations, LUMID_AGENT_MAX_ITERATIONS)` times. Each
tool result is appended as a `tool` role message; a tool error is appended with `"error": true`
so the LLM can retry or explain — the loop never aborts on a single tool error. If
`max_iterations` is reached, a `done` frame is emitted with `"truncated": true`.

## Configuration knobs (env)

| env var                          | default       | description                                          |
|----------------------------------|---------------|------------------------------------------------------|
| `LUMID_AGENT_MAX_ITERATIONS`   | 10            | hard iteration cap for the loop                      |
| `LUMID_USER_SCHEMAS`           | (all non-sys) | comma-separated schema allowlist for card scope      |
| `LUMID_RETRIEVAL_CARD_TTL_S`   | 300           | schema-card cache TTL (seconds)                      |
| `LUMID_RETRIEVAL_STMT_TIMEOUT_MS` | 30000      | per-SELECT statement timeout                         |
| `LUMID_RETRIEVAL_ROW_CAP`      | 1000000       | max rows a SQL op may return                         |
| `LUMID_RETRIEVAL_PREFIX`       | retrievals    | object-store key prefix for materialized outputs     |
| `LUMID_RETRIEVAL_SAMPLE_ROWS`  | 5             | sample values per column in schema cards             |

## Auth

`/agent/v1` is merged into the gated router (same as `/v1/*`, `/mcp`, data routes) so it inherits
the platform's PAT + local-key auth and tiered rate limit with no extra wiring.

## Testing

Lib unit tests (`src/retrieve/*` `#[cfg(test)]`) + integration tests
(`crates/platform/tests/agent_tests.rs`) run in-memory (no Postgres, no LLM). Coverage:

- `tool_registry_*` — schemas are valid JSON-Schema objects; exactly `get_schema_cards` and
  `replay_retrieval_plan` are present; the old `list_tables`/`describe_table`/`read_blob` are absent
- `*_schema_requires_*` — each tool's schema lists the right required fields
- `sql_op_rejects_*` — SELECT-only parser rejects each DML/DDL keyword + multi-statement
- `storage_get_rejects_*` — key path-traversal guards (`..`, absolute paths)
- `write_to_store_uri_is_app_relative` — materialized_uri is `/blobs/<key>`, no `s3://`/bucket
- `catalog_queries_use_quote_ident_*` — card SQL binds text params, never `($1)::regclass`
- `user_schemas_*` — env-var allowlist parsing
- `serve_fails_when_enable_agent_without_enable_llm` — fast-fail guard
- `lineage_columns_are_stripped` / `sse_body_*` — result-row + SSE frame contracts
