# Standalone app — everything in one folder

Bring up a complete `lumid-platform` app from a single directory. The folder holds
the **app** + its **Postgres/TimescaleDB**, **MinIO** (object storage / blobs), and
**Redis** — all data and config under `./`. No shared host services, no external
dependencies (auth runs on local keys).

```
myapp/
├── docker-compose.yml   # db + redis + minio + app, all volumes under ./data
├── .env                 # secrets + config (copy from .env.example)
├── app.toml             # your declarative read endpoints
├── schema.sql           # your warehouse DDL (runs once on first DB boot)
└── data/                # created on first run — the entire dataset lives here
    ├── pgdata/          #   Postgres + TimescaleDB
    ├── minio/           #   object storage (blobs)
    └── redis/
```

## 1. Get an app image

**Config-only app** (declarative reads, no custom Rust — the common case): build a
thin generic platform binary once and tag it `lumid-app:latest`. This repo ships
only the platform library crate (`crates/platform`, crate `lumid-platform`), so
the config-only binary is a ~6-line `main` you provide in your own app crate (see
`../../DEVELOPING.md`); build it and package it:

```bash
# from your app crate that depends on lumid-platform via a path dep
docker run --rm -v "$PWD":/build -e CARGO_HOME=/build/.cargo-home \
  -w /build rust:1-bookworm cargo build --release --bin mint-app
# package the binary into a slim image
install -D target/release/mint-app /tmp/img/lumid-app
printf 'FROM debian:bookworm-slim\nRUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*\nWORKDIR /app\nCOPY lumid-app /usr/local/bin/lumid-app\nENV LUMID_BIND_ADDR=0.0.0.0:8088\nEXPOSE 8088\nCMD ["lumid-app"]\n' > /tmp/img/Dockerfile
docker build -t lumid-app:latest /tmp/img
```

**App with bespoke Rust** (custom handlers / realtime upstreams): build
your own image and set `APP_IMAGE=your-image:tag` in `.env`.

## 2. Configure + bring it up

```bash
cp .env.example .env && $EDITOR .env      # set DB_PASSWORD, MINIO_PASSWORD, API_KEYS
$EDITOR schema.sql                        # your tables (provenance cols + PK; hypertable optional)
$EDITOR app.toml                          # your read endpoints

docker compose up -d
curl localhost:5012/health
curl -H "Authorization: Bearer devkey" "localhost:5012/events/recent?limit=10"
```

The whole stack is now under this folder. `docker compose down` stops it; the data
persists in `./data`. Move the folder, you move the app.

## 3. Put data in

Reads serve whatever is in the DB. Load it however you like:
- **API ingest** (introspected + validated + provenance-stamped + upserted):
  ```bash
  curl -H "Authorization: Bearer devkey" -H 'Content-Type: application/json' \
    -X POST localhost:5012/ingest/app/events \
    -d '{"records":[{"id":1,"kind":"signup","payload":{"u":42}}]}'
  ```
  Writing to an **unknown** table stages a schema **proposal** (admin approves via
  `POST /admin/ingress/proposals/{id}/approve`, gated to a `super_admin`/local key).
- **Blobs** go to the in-folder MinIO: `POST /ingest/blob` → served at `/blobs/...`.
- **Bulk/batch**: run your own loaders against `localhost:5432` (the `db` service).

## What you get, for free, from the platform
- Declarative reads with a multi-tier cache (L1 + Redis L2 + ETag/304).
- Write/ingest plane + catalog/lineage + schema proposals.
- Realtime hub (SSE/WS) — register upstream workers in a bespoke app.
- `POST /mcp` (one tool per read endpoint) + optional OpenAI/Anthropic LLM proxy.
- A generic **landing at `/`**, plus `/openapi.json`, `/status` (health + measured realtime
  feeds), `/usage`, `/freshness`. (`/reference`, `/llm`, and any realtime SSE/WS routes are
  *app*-provided — a bespoke app overrides the landing via `ServeParts.landing` and mounts its
  own routes; a config-only app gets the generic `/`.)

## Notes
- **MinIO vs local FS**: this bundle stores blobs in MinIO (self-contained). To use a
  plain folder instead, set `LUMID_BLOB_BACKEND=localfs` and bind-mount a blobs dir.
- **Auth**: standalone uses local keys (`LUMID_API_KEYS`, `LUMID_AUTH_ENABLED=false`).
  Point at a real introspection service by flipping those.
- **Retrieval privileges**: `POST /retrieve` and the data agent run caller-shaped
  `SELECT`s. They share the app's pool, which has write/DDL (and, against a default
  Postgres, is a superuser). `reader_role.sql` creates a NOSUPERUSER `lumid_reader`
  role and `RETRIEVAL_DB_ROLE=lumid_reader` makes the retrieval path `SET LOCAL ROLE`
  into it per-transaction — so those SELECTs can't call `pg_read_file()` or read
  `pg_authid`. Edit `reader_role.sql` to scope its grants per-schema if you want
  `LUMID_USER_SCHEMAS` to be a hard read boundary. Blank `RETRIEVAL_DB_ROLE` to opt out.
- **Optional platform features** (set in the compose `environment:` block): a ClickHouse
  backend alongside Postgres (`LUMID_CLICKHOUSE_*`, per-table routing via
  `provenance.table_backend`); realtime channel kinds for a bespoke app's workers
  (`LUMID_RT_CHANNEL_KINDS`, default `tick,news`). See the platform README's Config section.
- **Data-push / sync**: to make this instance a sync **target**, issue the producer a
  `sync:<peer>` local key (`API_KEYS=...,synctoken:sync:findata`); to make it a **producer**,
  set `LUMID_SYNC_TARGET_URL`/`_TOKEN`/`LUMID_SYNC_PEER_ID` in the app `environment:`. Both need an
  app image built with `ServeParts.enable_sync` (the generic `mint` image ships it off). The `sync`
  bookkeeping tables are created automatically at boot — no DDL to add to `schema.sql`. Full recipe
  in the platform README's **Data migration / cross-instance sync** section.
- **First boot only**: `schema.sql` runs only when `./data/pgdata` is empty. To re-init,
  stop and delete `./data/pgdata`. Schema changes after that are manual DDL (`psql`).
