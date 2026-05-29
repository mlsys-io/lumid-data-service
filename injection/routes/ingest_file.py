"""POST /ingest/{schema}/{table}/file — multipart structured-file ingress (v1).

Accepts a single `file` part. Server sniffs Content-Type or filename suffix
and dispatches to the matching parser (JSON / NDJSON / CSV / TSV / XML /
YAML / Parquet / Arrow). For binary types (images / PDF / HTML / opaque)
this route ALSO accepts the blob via a separate `/ingest/blob` route.

For structured files, behaves like a synchronous typed-mode write — the
whole file is parsed into records, then handed to core.ingest_records.
Gzip/zstd compression is honoured via Content-Encoding header.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

import asyncio as _asyncio

from ..auth import require_identity
from ..ingest import acl as ingress_acl
from ..ingest import lumilake as lumilake_hook
from ..ingest import sandbox as ingress_sandbox
from ..ingest.core import ingest_records
from ..ingest.decompress import decode
from ..ingest.errors import ACLError, IngestError, SchemaIntrospectionError
from ..ingest.parsers import kind_for, parse_to_records
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_file")

router = APIRouter(prefix="", tags=["Ingress"])

_STRUCTURED_KINDS = {"json", "ndjson", "csv", "tsv", "xml", "yaml", "parquet", "arrow"}


@router.post(
    "/ingest/{schema}/{table}/file",
    summary="Upload a structured file into a target table",
    description=(
        "Multipart upload. Server picks the parser by Content-Type or "
        "filename suffix: .json, .ndjson/.jsonl, .csv, .tsv, .xml, "
        ".yaml/.yml, .parquet/.pq, .arrow. Gzip/zstd compression is "
        "transparent via Content-Encoding. For images/PDFs/binary blobs, "
        "use POST /ingest/blob instead."
    ),
)
async def post_file(
    request: Request,
    schema: str,
    table: str,
    file: UploadFile = File(..., description="Structured file (JSON/NDJSON/CSV/TSV/XML/YAML/Parquet/Arrow)."),
    source_endpoint: Optional[str] = Form(default=None),
    identity: Identity = Depends(require_identity),
):
    # Probe target existence before reading the file body — split-gate
    # ACL (existing tables) vs propose (sandbox fallback for net-new).
    def _target_exists() -> bool:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema=%s AND table_name=%s",
                    (schema, table),
                )
                return cur.fetchone() is not None
    exists = await _asyncio.to_thread(_target_exists)
    if exists:
        try:
            ingress_acl.check_can_write(identity.role, schema, table)
        except ACLError as e:
            raise HTTPException(status_code=403, detail=str(e))
    else:
        if not ingress_acl.can_propose(identity.role):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"role {identity.role!r} has no ingress allowlist entries; "
                    "cannot write or propose new tables."
                ),
            )

    body = await file.read()
    content_encoding = request.headers.get("content-encoding")
    try:
        body = decode(body, content_encoding)
    except IngestError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        kind = kind_for(file.content_type, file.filename)
    except IngestError as e:
        raise HTTPException(status_code=415, detail=str(e))

    if kind not in _STRUCTURED_KINDS:
        raise HTTPException(
            status_code=415,
            detail=(
                f"file content_type={file.content_type!r} resolves to kind={kind!r}; "
                "this route only accepts structured files. Use POST /ingest/blob "
                "for images / PDFs / opaque binary."
            ),
        )

    try:
        records = await asyncio.to_thread(parse_to_records, body, kind)
    except IngestError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not records:
        return {
            "run_id": "",
            "target_schema": schema, "target_table": table,
            "received": 0, "inserted": 0, "updated": 0, "failed": 0,
            "rejected": [], "status": "ok",
            "_note": f"parsed 0 records from {file.filename!r}",
        }

    src = f"ingress:{identity.sub}"
    src_endpoint = source_endpoint or f"ingress:file/{kind}"
    ua = request.headers.get("user-agent")

    # Net-new target → sandbox-fallback (matches the typed-route behavior).
    if not exists:
        def _sandbox() -> Dict[str, Any]:
            with connection() as conn:
                sb = ingress_sandbox.land_in_sandbox(
                    conn,
                    declared_schema=schema, declared_table=table,
                    records=records,
                    source=src, source_endpoint=src_endpoint,
                    submitted_by=identity.sub,
                    proposer_role=identity.role,
                    declared_endpoint=source_endpoint,
                    user_agent=ua,
                )
                return sb.to_dict()
        try:
            sb_result = await _asyncio.to_thread(_sandbox)
        except IngestError as ie:
            raise HTTPException(
                status_code=getattr(ie, "http_status", 400), detail=str(ie),
            )
        return JSONResponse(status_code=202, content={
            **sb_result,
            "_next_steps": (
                f"Net-new target — parsed {len(records)} records from "
                f"{file.filename!r} and persisted them in raw.ingress_drops. "
                "Admin review at GET /admin/ingress/proposals."
            ),
        })

    def _run() -> Dict[str, Any]:
        with connection() as conn:
            r = ingest_records(
                conn,
                target_schema=schema, target_table=table,
                records=records,
                source=src, source_endpoint=src_endpoint,
                submitted_by=identity.sub,
                declared_endpoint=source_endpoint,
                mode=f"file:{kind}", user_agent=ua,
                on_finalize=lumilake_hook.submit_after_ingest,
            )
            return r.to_dict()

    try:
        result = await asyncio.to_thread(_run)
    except SchemaIntrospectionError as e:
        # Race: table existed at probe-time, deleted by an admin between
        # probe and write. Fall back to sandbox.
        log.warning("post-probe SchemaIntrospectionError for %s.%s — routing "
                    "to sandbox", schema, table)
        def _sandbox2() -> Dict[str, Any]:
            with connection() as conn:
                sb = ingress_sandbox.land_in_sandbox(
                    conn,
                    declared_schema=schema, declared_table=table,
                    records=records,
                    source=src, source_endpoint=src_endpoint,
                    submitted_by=identity.sub,
                    proposer_role=identity.role,
                    declared_endpoint=source_endpoint, user_agent=ua,
                )
                return sb.to_dict()
        sb_result = await _asyncio.to_thread(_sandbox2)
        return JSONResponse(status_code=202, content=sb_result)
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400), detail=str(e))

    if (result.get("status") == "failed"
            and result.get("inserted", 0) == 0
            and result.get("updated", 0) == 0
            and result.get("failed", 0) == result.get("received", 0)):
        return JSONResponse(status_code=422, content=result)
    return result
