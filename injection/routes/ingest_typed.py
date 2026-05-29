"""POST /ingest/{schema}/{table} — typed-row ingress (v0 MVP).

Body shape:
  {
    "source_endpoint": "ingress:partner-foo/...",      # optional; defaults to ingress:<sub>
    "records": [ {col: val, ...}, ... ]
  }

Authentication: every route inherits `require_identity` from server.py's
`_auth_dep` — anonymous returns 401. Local-key bypass (FINDATA_API_KEYS)
gives role='local' which is seeded for blanket allow in provenance.ingress_acl.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from fastapi.responses import JSONResponse

from ..auth import require_identity
from ..ingest import core
from ..ingest import acl as ingress_acl
from ..ingest import lumilake as lumilake_hook
from ..ingest import sandbox as ingress_sandbox
from ..ingest.errors import ACLError, IngestError, SchemaIntrospectionError, ValidationError
from ..ingest.models import IngestResultModel
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_typed")

router = APIRouter(prefix="", tags=["Ingress"])


class IngestTypedBody(BaseModel):
    source_endpoint: Optional[str] = Field(
        default=None,
        max_length=200,
        description=(
            "Caller-declared upstream endpoint identifier. Stamped onto every "
            "ingested row as source_endpoint. If omitted, defaults to "
            "'ingress:<your-sub>'. Must match [A-Za-z0-9_:/?=&.\\-]{1,200}."
        ),
    )
    records: List[Dict[str, Any]] = Field(
        ...,
        min_length=1,
        max_length=50_000,
        description=(
            "List of records in TARGET-COLUMN shape. Required columns and "
            "their types are listed at GET /catalog/tables/{schema}/{table}/schema.json. "
            "Unknown keys are rejected (422). Provenance columns "
            "(source, source_endpoint, source_run_id, ingest_ts, raw) are set "
            "server-side and must NOT be included."
        ),
    )


@router.post(
    "/ingest/{schema}/{table}",
    summary="Push typed rows (ingress)",
    description=(
        "Append/upsert rows into `<schema>.<table>` with full provenance. "
        "Idempotent: re-posting the same rows returns inserted=0,updated=0 "
        "thanks to the DISTINCT-FROM merge. Each request creates one "
        "provenance.runs row (visible via GET /catalog/lineage/run/{run_id}) "
        "stamped with the caller's identity.sub as `submitted_by`."
    ),
    response_model=IngestResultModel,
    responses={
        200: {"description": "Rows accepted; status='ok' or 'partial'."},
        401: {"description": "Anonymous or invalid PAT."},
        403: {"description": "Role not allowed to write target."},
        404: {"description": "Unknown schema or table."},
        422: {"description": "All records failed Pydantic validation."},
    },
)
async def post_typed(
    request: Request,
    schema: str,
    table: str,
    body: IngestTypedBody = Body(...),
    identity: Identity = Depends(require_identity),
):
    # Probe whether the target exists so we can pick the right gate.
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
                    "cannot write or propose new tables."
                ),
            )

    src = f"ingress:{identity.sub}"
    declared = body.source_endpoint
    src_endpoint = declared or f"ingress:{identity.sub}"
    ua = request.headers.get("user-agent")

    def _run() -> Dict[str, Any]:
        with connection() as conn:
            result = core.ingest_records(
                conn,
                target_schema=schema,
                target_table=table,
                records=body.records,
                source=src,
                source_endpoint=src_endpoint,
                submitted_by=identity.sub,
                declared_endpoint=declared,
                mode="typed",
                user_agent=ua,
                on_finalize=lumilake_hook.submit_after_ingest,
            )
            return result.to_dict()

    try:
        result = await asyncio.to_thread(_run)
    except SchemaIntrospectionError:
        # Net-new target — fall back to the sandbox + proposal queue.
        # Returns 202 with a proposal id so the partner knows their bytes
        # are persisted and awaiting admin review.
        def _sandbox() -> Dict[str, Any]:
            with connection() as conn:
                sb = ingress_sandbox.land_in_sandbox(
                    conn,
                    declared_schema=schema, declared_table=table,
                    records=body.records,
                    source=src, source_endpoint=src_endpoint,
                    submitted_by=identity.sub,
                    proposer_role=identity.role,
                    declared_endpoint=declared,
                    user_agent=ua,
                )
                return sb.to_dict()
        try:
            sb_result = await asyncio.to_thread(_sandbox)
        except IngestError as ie:
            raise HTTPException(
                status_code=getattr(ie, "http_status", 400), detail=str(ie),
            )
        return JSONResponse(status_code=202, content={
            **sb_result,
            "_next_steps": (
                "Net-new target — your records have been persisted in "
                "raw.ingress_drops and a proposal opened. An admin will "
                "review the inferred schema and approve it; subsequent "
                "pushes to this target will then land in the typed table "
                "(your sandboxed rows will be auto-backfilled on approval). "
                "Track via GET /catalog/ingress/proposals/{proposal_id}."
            ),
        })
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400), detail=str(e))
    # If every record failed validation (no run row created, no rows landed),
    # surface as 422 so callers don't have to inspect status in a 200 body.
    if (result.get("status") == "failed"
            and result.get("inserted", 0) == 0
            and result.get("updated", 0) == 0
            and result.get("failed", 0) == result.get("received", 0)):
        raise HTTPException(status_code=422, detail=result)
    return result
