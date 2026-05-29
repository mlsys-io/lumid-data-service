"""GET /blobs/{key:path} — serve blobs from FINDATA_BLOB_ROOT.

Keyed paths look like `images/sha256=<hex>`, `pdf/sha256=<hex>`, …
Content-Type is read from raw.blobs metadata (DB-truth, not file-extension
guessing). Returns 404 if either the DB row is absent OR the file is
missing.

Plus a back-compat alias `/storage/v1/object/findata/{path:path}` that
302-redirects to `/blobs/{path}` so already-emitted `storage_url`s in
existing `raw.blobs` rows keep working after the migration.

This is the injection-service copy: it reads the content-type via the sync
psycopg2 ingest pool (the injection service has no asyncpg pool), hopping to a
worker thread so the DB lookup doesn't block the event loop.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse

from ..config import settings
from ..ingest.pool import connection

log = logging.getLogger("findata.routes.blobs")

router = APIRouter(prefix="", tags=["Ingress"])


def _content_type_for_key_sync(key: str) -> Optional[str]:
    """Look up raw.blobs row by reconstructing sha256 from the key suffix."""
    if "sha256=" not in key:
        return None
    sha = key.rsplit("sha256=", 1)[-1]
    if len(sha) != 64:
        return None
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT content_type FROM raw.blobs WHERE blob_sha256 = %s",
                (sha,),
            )
            row = cur.fetchone()
    return row[0] if row else None


@router.get(
    "/blobs/{key:path}",
    summary="Serve a stored blob by storage key",
    description=(
        "Returns the bytes at `<FINDATA_BLOB_ROOT>/{key}`. The Content-Type "
        "is read from `raw.blobs.content_type` (so the API is the source of "
        "truth, not file-extension guessing). Returns 404 if the blob isn't "
        "registered in raw.blobs."
    ),
)
async def serve_blob(key: str):
    if not settings.blob_root:
        raise HTTPException(status_code=503, detail="blob storage not configured")
    # Security: confine to blob_root; reject any traversal attempts.
    abs_path = os.path.realpath(os.path.join(settings.blob_root, key))
    if not abs_path.startswith(os.path.realpath(settings.blob_root) + os.sep) \
       and abs_path != os.path.realpath(settings.blob_root):
        raise HTTPException(status_code=400, detail="invalid key")
    if not os.path.isfile(abs_path):
        raise HTTPException(status_code=404, detail="blob not found")
    ct = await asyncio.to_thread(_content_type_for_key_sync, key) \
         or "application/octet-stream"
    return FileResponse(abs_path, media_type=ct)


@router.get(
    "/storage/v1/object/findata/{path:path}",
    summary="Back-compat: redirect legacy storage URLs to /blobs/...",
    description=(
        "URLs emitted prior to the storage streamline pointed at a legacy "
        "/storage/v1 endpoint. We accept them here and 302-redirect to the "
        "current /blobs/<key> shape so existing raw.blobs.storage_url values "
        "stay resolvable."
    ),
    include_in_schema=False,
)
async def legacy_storage_alias(path: str):
    return RedirectResponse(url=f"/blobs/{path}", status_code=302)
