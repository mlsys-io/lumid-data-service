# Developing an app on lumid-platform

A new app = **a config file + a ~6-line `main` + a DB schema**. No platform code changes.

## 0. Prerequisites
- Rust (stable), Docker, and access to Postgres + Redis (use the shared `finai-tsdb-pg17`
  / `finai-redis`, or your own).
- Clone the platform and your app **as siblings**:
  ```
  /parent/lumid-data-service     # this repo (crate: lumid-platform)
  /parent/myapp                  # your app
  ```

## 1. Skeleton app
`myapp/Cargo.toml`
```toml
[package]
name = "myapp"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "myapp"
path = "src/main.rs"

[dependencies]
lumid-platform = { path = "../lumid-data-service/crates/platform" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal", "net"] }
anyhow = "1"
# add axum only if you ship bespoke ext routes
```

`myapp/src/main.rs`
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lumid_platform::serve(lumid_platform::ServeParts {
        ext_routes: axum::Router::new(),  // or your_ext::routes()
        workers:    vec![],               // or your_ext::workers()
        enable_llm: false,                // true → mount the /v1/* LLM proxy
    })
    .await
}
```

`myapp/myapp.toml` (read endpoints — pure config)
```toml
[[read.endpoint]]
id = "myapp.accounts"
method = "GET"
path = "/accounts"
tables = ["myapp.accounts"]
ttl = "30s"
shape = "rows"
strip_lineage = true
cache = true
sql = """
SELECT account, name, balance FROM myapp.accounts ORDER BY account
"""
```
Values bind as `:name` (always parameterized). Switchable table/filter fragments use
`{{name}}` driven by author-controlled `[read.endpoint.param.select.*]` maps — never raw input.

## 2. Database / schema
Pick a Postgres **schema** (your app's namespace). Tables follow the convention
(your columns + provenance columns + a natural key):
```sql
CREATE SCHEMA IF NOT EXISTS myapp;
CREATE TABLE myapp.accounts (
  account text NOT NULL, name text, balance numeric,
  source text NOT NULL, source_endpoint text NOT NULL,
  source_run_id uuid NOT NULL REFERENCES provenance.runs(run_id),
  ingest_ts timestamptz NOT NULL DEFAULT now(), raw jsonb,
  PRIMARY KEY (account, source));
```
(Or just `POST /ingest/myapp/accounts` records to an unknown table → the platform infers a
schema + stages a proposal; an admin approves to create it. No hand-DDL needed.)

## 3. Build
```bash
cargo build --release --bin myapp
# …or in-container (no toolchain/GitHub auth needed on the host):
docker run --rm -v /parent:/parent -w /parent/myapp \
  -e CARGO_HOME=/parent/.cargo-home rust:1-bookworm \
  cargo build --release --bin myapp
```

## 4. Run (local)
```bash
LUMID_DB_HOST=localhost LUMID_DB_PORT=5433 LUMID_DB_PASSWORD=… \
LUMID_DB_NAME=appdb \
LUMID_REDIS_URL=redis://127.0.0.1:6379 \
LUMID_API_KEYS='devkey:dev' \
LUMID_READ_CONFIG=$PWD/myapp.toml \
LUMID_SERVICE_NAME=myapp \
LUMID_BIND_ADDR=0.0.0.0:8090 \
./target/release/myapp
```
Smoke test:
```bash
H='Authorization: Bearer devkey'
curl -H "$H" -X POST localhost:8090/ingest/myapp/accounts \
  -d '{"records":[{"account":"a1","name":"Checking","balance":10}]}' -H 'Content-Type: application/json'
curl -H "$H" localhost:8090/accounts          # your config endpoint
curl -H "$H" -X POST localhost:8090/mcp -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' -H 'Content-Type: application/json'
```
You inherit auth, ingest+validation+provenance, config reads, cache/ETag, catalog/lineage,
realtime hub, and auto-MCP (one tool per read endpoint) for free.

## 5. Deploy
1. Bake a tiny runtime image — **never** build with the data tree as context:
   ```dockerfile
   FROM debian:bookworm-slim
   RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
   COPY myapp /usr/local/bin/myapp
   COPY myapp.toml /app/myapp.toml
   ENV LUMID_BIND_ADDR=0.0.0.0:8088 LUMID_READ_CONFIG=/app/myapp.toml
   EXPOSE 8088
   CMD ["myapp"]
   ```
   ```bash
   mkdir /tmp/myapp-img && cp target/release/myapp myapp.toml Dockerfile /tmp/myapp-img/
   docker build -t myapp:latest /tmp/myapp-img
   ```
2. Run on the shared network with real env:
   ```bash
   docker run -d --name myapp --network appnet -p 0.0.0.0:5030:8088 \
     --env-file secrets.env \
     -e LUMID_DB_HOST=finai-tsdb-pg17 -e LUMID_DB_PORT=5432 -e LUMID_DB_NAME=appdb \
     -e LUMID_REDIS_URL=redis://finai-redis:6379 -e LUMID_SERVICE_NAME=myapp \
     --restart unless-stopped myapp:latest
   ```
3. Public exposure: add an nginx `location / { proxy_pass http://172.17.0.1:5030; }` (+ TLS).
   Internal-only apps skip nginx.
4. Iterate: rebuild binary → rebuild tiny image → `docker rm -f myapp && docker run …`.

## Worked example
The **mint** app (`crates/mint-app-bin/` + `mint.toml`) is a complete
platform-only app — copy it.

## Enabling the LLM proxy
Set `enable_llm: true` in `ServeParts` and `LUMID_LLM_BACKEND_URL` in the env → the
OpenAI/Anthropic-compatible `/v1/*` surface is served, proxying to that backend.
