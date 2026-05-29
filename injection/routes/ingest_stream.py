"""POST /ingest/{schema}/{table}/stream — NDJSON streaming ingress (v1).

Chunked transfer-encoding with one JSON record per line. One provenance.runs
row spans the whole stream; rows are flushed in 10k-record batches to keep
memory bounded.

Content-Type: application/x-ndjson  (or application/jsonl)
Content-Encoding: gzip|zstd|identity (transparent decode)
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from ..auth import require_identity
from ..ingest import acl as ingress_acl
from ..ingest import lumilake as lumilake_hook
from ..ingest import sandbox as ingress_sandbox
from ..writeengine import engine as loaders_lib
from ..ingest.core import IngestResult, ingest_records
from ..ingest.decompress import aiter_decoded
from ..ingest.errors import ACLError, IngestError, SchemaIntrospectionError
from ..ingest.parsers import aiter_ndjson
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_stream")

router = APIRouter(prefix="", tags=["Ingress"])

# Flush every N records during a stream.
_STREAM_FLUSH = 10_000


def _sandbox_chunk(
    schema: str, table: str, records: List[Dict[str, Any]],
    src: str, src_endpoint: str, submitted_by: str, proposer_role: str,
    declared: Optional[str], ua: Optional[str],
) -> Dict[str, Any]:
    """Helper invoked via asyncio.to_thread from the stream-sandbox path.
    Each chunk opens its own provenance.runs row (sandbox mode); the
    proposal row in provenance.ingress_proposals is upserted (drop_count
    accumulates) so a multi-chunk stream still yields ONE proposal."""
    with connection() as conn:
        return ingress_sandbox.land_in_sandbox(
            conn,
            declared_schema=schema, declared_table=table,
            records=records,
            source=src, source_endpoint=src_endpoint,
            submitted_by=submitted_by,
            proposer_role=proposer_role,
            declared_endpoint=declared, user_agent=ua,
        ).to_dict()


@router.post(
    "/ingest/{schema}/{table}/stream",
    summary="Stream NDJSON records into a target table",
    description=(
        "Streaming variant of POST /ingest/{schema}/{table}. Body is "
        "line-delimited JSON; supply `Content-Type: application/x-ndjson` "
        "(or .../jsonl). Gzip/zstd accepted via `Content-Encoding`. One "
        "provenance.runs row tracks the whole stream; flushes every "
        f"{_STREAM_FLUSH} records to bound memory."
    ),
)
async def post_stream(
    request: Request,
    schema: str,
    table: str,
    identity: Identity = Depends(require_identity),
):
    # Probe target existence — split-gate ACL vs propose for net-new.
    def _target_exists() -> bool:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema=%s AND table_name=%s",
                    (schema, table),
                )
                return cur.fetchone() is not None
    exists = await asyncio.to_thread(_target_exists)
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
                    "cannot stream into a non-existent target."
                ),
            )

    src = f"ingress:{identity.sub}"
    declared = request.headers.get("x-ingress-source-endpoint")
    src_endpoint = declared or f"ingress:{identity.sub}"
    ua = request.headers.get("user-agent")
    content_encoding = request.headers.get("content-encoding")

    # ---- Net-new target → sandbox-stream path ----
    # Drain the NDJSON stream into 10k-record chunks, hand each to
    # land_in_sandbox (which opens its own provenance.runs row per chunk).
    # We return a 202 with a single proposal summary; subsequent typed
    # writes after admin approval go through the existing code path.
    if not exists:
        chunk: List[Dict[str, Any]] = []
        total_received = 0
        total_drops = 0
        proposal_id = ""
        try:
            async for rec in aiter_ndjson(
                aiter_decoded(request.stream(), content_encoding)
            ):
                total_received += 1
                chunk.append(rec)
                if len(chunk) >= _STREAM_FLUSH:
                    sb = await asyncio.to_thread(
                        _sandbox_chunk, schema, table, chunk, src, src_endpoint,
                        identity.sub, identity.role, declared, ua,
                    )
                    proposal_id = sb.get("proposal_id", proposal_id)
                    total_drops += sb.get("drops_inserted", 0)
                    chunk = []
            if chunk:
                sb = await asyncio.to_thread(
                    _sandbox_chunk, schema, table, chunk, src, src_endpoint,
                    identity.sub, identity.role, declared, ua,
                )
                proposal_id = sb.get("proposal_id", proposal_id)
                total_drops += sb.get("drops_inserted", 0)
        except IngestError as e:
            return JSONResponse(status_code=getattr(e, "http_status", 400),
                                content={"error": str(e),
                                         "received": total_received,
                                         "drops_inserted": total_drops})
        return JSONResponse(status_code=202, content={
            "proposal_id": proposal_id,
            "declared_schema": schema, "declared_table": table,
            "received": total_received, "drops_inserted": total_drops,
            "status": "sandboxed",
            "_next_steps": (
                f"Net-new target — {total_drops} records persisted in "
                "raw.ingress_drops. Admin review at "
                "GET /admin/ingress/proposals."
            ),
        })

    # 1) Open the run row once.
    def _open_run() -> uuid.UUID:
        with connection() as conn:
            run_args: Dict[str, Any] = {
                "target_schema": schema,
                "target_table": table,
                "mode": "stream",
                "declared_endpoint": declared,
                "user_agent": ua,
                "submitted_by": identity.sub,
            }
            rid = loaders_lib.open_run(
                conn, endpoint_id="ingress:generic", args=run_args,
            )
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE provenance.runs SET submitted_by = %s WHERE run_id = %s",
                    (identity.sub, rid),
                )
            conn.commit()
            return rid

    try:
        run_id = await asyncio.to_thread(_open_run)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to open run: {e}")

    inserted = 0
    updated = 0
    failed = 0
    rejected: List[Dict[str, Any]] = []
    received = 0
    chunk: List[Dict[str, Any]] = []
    status = "ok"
    error_text: Optional[str] = None

    def _flush(records: List[Dict[str, Any]]) -> None:
        nonlocal inserted, updated, failed, rejected
        if not records:
            return
        # Reuse core.ingest_records with run_id=<our run> so it appends to the
        # existing run row instead of opening a new one.
        with connection() as conn:
            r = ingest_records(
                conn,
                target_schema=schema, target_table=table,
                records=records,
                source=src, source_endpoint=src_endpoint,
                submitted_by=identity.sub, run_id=run_id,
                declared_endpoint=declared, mode="stream",
                user_agent=ua,
            )
            inserted += r.inserted
            updated  += r.updated
            failed   += r.failed
            rejected.extend(r.rejected)

    try:
        async for rec in aiter_ndjson(
            aiter_decoded(request.stream(), content_encoding)
        ):
            received += 1
            chunk.append(rec)
            if len(chunk) >= _STREAM_FLUSH:
                await asyncio.to_thread(_flush, chunk)
                chunk = []
        if chunk:
            await asyncio.to_thread(_flush, chunk)
    except IngestError as e:
        status = "failed"
        error_text = str(e)[-4000:]
        log.exception("stream ingest %s.%s failed", schema, table)
    except Exception as e:  # pragma: no cover — defensive
        status = "failed"
        error_text = repr(e)[-4000:]
        log.exception("stream ingest %s.%s unexpected error", schema, table)

    # 2) Close the run row.
    def _close():
        with connection() as conn:
            loaders_lib.close_run(
                conn, run_id,
                "partial" if (rejected and status == "ok") else status,
                rows_inserted=inserted, rows_updated=updated, rows_failed=failed,
                error_text=error_text,
            )
    try:
        await asyncio.to_thread(_close)
    except Exception:
        log.exception("failed to close stream run %s", run_id)

    final_status = "partial" if (rejected and status == "ok") else status
    body = {
        "run_id": str(run_id),
        "target_schema": schema,
        "target_table": table,
        "received": received,
        "inserted": inserted,
        "updated": updated,
        "failed": failed,
        "rejected": rejected[:50],  # cap so an N-million-line stream doesn't blow the response
        "status": final_status,
    }
    if status == "failed":
        return JSONResponse(status_code=400, content=body)
    # Fire Lumilake handoff once for the whole stream (matches the
    # "one run row spans the stream" model).
    if (inserted + updated) > 0:
        lumilake_hook.submit_after_ingest(
            IngestResult(
                run_id=str(run_id), target_schema=schema, target_table=table,
                received=received, inserted=inserted, updated=updated,
                failed=failed, rejected=[], status=final_status,
            ),
            {
                "target_schema": schema, "target_table": table, "mode": "stream",
                "declared_endpoint": declared, "submitted_by": identity.sub,
            },
        )
    return body
