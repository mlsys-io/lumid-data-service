"""findata Injection service — the portable write plane.

A standalone FastAPI app that owns data injection only: the typed / adapter /
stream / file / blob / webhook ingress modes, their admin self-service, the
ingress-discovery catalog endpoints, and blob serving. It shares one Postgres
with the read service but has no asyncpg pool, no realtime, no MCP, no LLM —
just the sync psycopg2 write pool (ingest/pool.py) and the vendored write
engine (writeengine.py).

Portability: the write engine is vendored, so typed / stream / file / blob /
webhook run with no dependency on any external `loaders/` tree. Adapter mode is
the one optional feature — it activates only when a `loaders/` tree is mounted
under $FINAI_ROOT, and 503s otherwise.

Auth: every data route requires a Lumid PAT (or a local-key bypass via
FINDATA_API_KEYS). The webhook route authenticates by HMAC instead. `/health`
is the one public route.
"""
from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from scalar_fastapi import get_scalar_api_reference
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from .auth import limiter, require_identity
from .lumid import lumid
from .routes import (
    ingest_typed as r_ingest_typed,
    ingest_adapter as r_ingest_adapter,
    ingest_stream as r_ingest_stream,
    ingest_file as r_ingest_file,
    ingest_blob as r_ingest_blob,
    ingest_webhook as r_ingest_webhook,
    ingest_admin as r_ingest_admin,
    ingress_catalog as r_ingress_catalog,
    blobs as r_blobs,
)

# Force findata.* loggers to surface at INFO regardless of how uvicorn
# configures the root logger.
_log_fmt = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
_log_h = logging.StreamHandler(sys.stderr)
_log_h.setFormatter(_log_fmt)
_findata_root = logging.getLogger("findata")
if not _findata_root.handlers:
    _findata_root.addHandler(_log_h)
_findata_root.setLevel(logging.INFO)
_findata_root.propagate = False

log = logging.getLogger("findata")


API_DESCRIPTION = """
The **findata Injection** service — the write plane of the findata dataset.

Push data in any of several shapes and it lands in the warehouse with full
provenance (every row traces back to a run, a submitter, and a source
endpoint). A per-target role ACL governs who can write where.

## Modes

| Mode | Endpoint | Body |
|---|---|---|
| **typed** | `POST /ingest/{schema}/{table}` | JSON records already in target-column shape |
| **adapter** | `POST /ingest/adapter/{adapter_id}` | upstream-shape records, flattened server-side |
| **stream** | `POST /ingest/{schema}/{table}/stream` | chunked NDJSON |
| **file** | `POST /ingest/{schema}/{table}/file` | multipart upload (JSON/CSV/TSV/XML/YAML/Parquet/Arrow) |
| **blob** | `POST /ingest/blob` | raw binary (images / PDFs / octet-stream) |
| **webhook** | `POST /webhook/{webhook_id}` | HMAC-signed body (no PAT) |

## Discovery

* `GET /catalog/tables/{schema}/{table}/schema.json` — JSON Schema for typed writes
* `GET /catalog/ingress` — one-call overview (modes, formats, what you can write)
* `GET /catalog/ingress/adapters` — registered adapters (empty when adapter mode is off)
* `GET /catalog/ingress/proposals` — your sandbox proposals for net-new shapes

## Auth

Every route except `GET /health` requires a Lumid PAT as
`Authorization: Bearer <token>` (or `X-API-Key`). The webhook route is the
exception — it authenticates by HMAC signature. Writes are additionally gated
by a per-target role ACL.
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    # auth.require_identity uses the module-level lumid client; without
    # startup() every PAT introspection rejects.
    await lumid.startup()
    log.info("lumid client started — injection service ready")
    try:
        yield
    finally:
        await lumid.shutdown()
        try:
            from .ingest import pool as ingest_pool
            ingest_pool.close_pool()
        except Exception as e:
            log.warning("ingest pool close failed: %s", e)


_OPENAPI_TAGS = [
    {"name": "Ingress", "description": (
        "Write surface — push data into findata. Typed / adapter / stream / "
        "file / blob / webhook modes. Per-target role ACL governs writes; "
        "every row is stamped with full provenance."
    )},
    {"name": "Catalog", "description": (
        "Ingress discovery — JSON Schema per writable target, one-call overview, "
        "adapters, and your sandbox proposals."
    )},
]


app = FastAPI(
    title="findata Injection API",
    description=API_DESCRIPTION,
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=_OPENAPI_TAGS,
)


# ----- middleware -----
# Writes need POST (and OPTIONS for CORS preflight). Read service only allows GET.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)


def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Default slowapi body + a standard Retry-After header."""
    response = _rate_limit_exceeded_handler(request, exc)
    window = 60
    try:
        parts = str(exc.detail).split()
        n, unit = int(parts[2]), parts[3].rstrip("s").lower()
        window = n * {"second": 1, "minute": 60, "hour": 3600, "day": 86400}.get(unit, 60)
    except Exception:
        pass
    response.headers["Retry-After"] = str(window)
    response.headers["X-RateLimit-Limit"] = str(exc.detail)
    return response


app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)


# ----- routers -----
_auth_dep = [Depends(require_identity)]


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "service": "findata-injection"}


@app.get("/", include_in_schema=False)
async def scalar_reference():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title="findata Injection API",
    )


# Ingress write plane. Order matters: specific paths (/ingest/adapter/{id},
# /ingest/blob, /ingest/{s}/{t}/stream, .../file) MUST precede the catch-all
# /ingest/{schema}/{table} or the wildcard shadows them.
app.include_router(r_ingest_blob.router,    dependencies=_auth_dep)
app.include_router(r_ingest_adapter.router, dependencies=_auth_dep)
app.include_router(r_ingest_stream.router,  dependencies=_auth_dep)
app.include_router(r_ingest_file.router,    dependencies=_auth_dep)
app.include_router(r_ingest_typed.router,   dependencies=_auth_dep)
# Webhook ingress — HMAC-authenticated, no PAT (auth happens inside the route).
app.include_router(r_ingest_webhook.router)
# Admin self-service (super_admin / local only — role check inside each route).
app.include_router(r_ingest_admin.router,   dependencies=_auth_dep)
# Ingress-discovery catalog (the 4 endpoints that need ingest introspection).
app.include_router(r_ingress_catalog.router, dependencies=_auth_dep)
# Blob serve — /blobs/{key} + legacy /storage/v1/... 302 alias.
app.include_router(r_blobs.router,          dependencies=_auth_dep)
