# findata Injection

The **write plane** of findata — a portable, self-contained service for pushing
data into the warehouse with full provenance. It is deliberately decoupled from
the read API: it runs as its own process/container, shares only the Postgres
database, and carries no read, realtime, or MCP surface.

## What it does

Accept data in any of six shapes and land it in a target table, stamping every
row with its run, submitter, and source endpoint. A per-target role ACL governs
who can write where.

| Mode | Endpoint | Body |
|---|---|---|
| typed | `POST /ingest/{schema}/{table}` | JSON records in target-column shape |
| adapter | `POST /ingest/adapter/{adapter_id}` | upstream-shape records, flattened server-side *(optional — see below)* |
| stream | `POST /ingest/{schema}/{table}/stream` | chunked NDJSON (gzip/zstd ok) |
| file | `POST /ingest/{schema}/{table}/file` | multipart upload (JSON/CSV/TSV/XML/YAML/Parquet/Arrow) |
| blob | `POST /ingest/blob` | raw binary (images / PDFs / octet-stream) |
| webhook | `POST /webhook/{webhook_id}` | HMAC-signed body (no PAT) |

Discovery: `GET /catalog/ingress` (overview), `GET /catalog/tables/{s}/{t}/schema.json`
(JSON Schema for typed writes), `GET /catalog/ingress/adapters`,
`GET /catalog/ingress/proposals`. Admin self-service (webhooks, ACL grants,
schema/ACL cache refresh) lives under `/admin/ingress/*`.

## Portability

The write engine — the COPY-staging + idempotent merge — is **vendored** in
`injection/writeengine.py`. So typed / stream / file / blob / webhook run
standalone against any Postgres with the findata schema, with no external
dependencies beyond the Python packages in `pyproject.toml`.

**Adapter mode is the one optional feature.** It flattens upstream-shaped JSON
using per-table normalizers from a separate `loaders/` tree. Mount that tree at
`/app/loaders` (plus a `CLAUDE.md` marker, or set `FINAI_ROOT`) and adapter mode
activates; omit it and adapter mode returns `503` while every other mode keeps
working. `GET /catalog/ingress/adapters` returns `[]` when it's off.

## Auth

Every route except `GET /health` requires a Lumid PAT
(`Authorization: Bearer <token>` or `X-API-Key`). The webhook route is the
exception — it authenticates by HMAC signature. A local-key bypass
(`FINDATA_API_KEYS=key:label,...`) is available for internal callers. The
service runs its **own** auth + ACL on every request — it never trusts an
upstream caller's say-so.

## Run

```bash
cp .env.example .env          # fill in FINDATA_DB_PASSWORD (+ keys as needed)
docker compose -f docker-compose.injection.yml up -d --build
curl -s localhost:5011/health
# Scalar API reference at  http://localhost:5011/
```

It joins the external `findata-net` network and talks to the same Postgres and
blob volume as the read service. Provenance (`provenance.runs`, the ingress ACL,
sandbox proposals, blob registry) is shared in that one database, so lineage
stays unified no matter which service wrote a row.

## Layout

```
injection/
  server.py            FastAPI app (ingest + ingress-catalog + blobs only)
  writeengine.py       vendored COPY-staging + merge (the portability core)
  config.py auth.py lumid.py
  ingest/              core write fn, validation, ACL, pool, adapters bridge,
                       sandbox, proposals, blob, webhook, parsers, decompress, …
  routes/              ingest_{typed,adapter,stream,file,blob,webhook,admin},
                       ingress_catalog, blobs
```
