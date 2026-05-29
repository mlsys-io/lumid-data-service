"""POST /ingest/adapter/{adapter_id} — adapter-mode ingress (v1).

Body shape:
  {
    "source_endpoint": "ingress:partner-foo/...",   # optional
    "scope_key": "AAPL",                            # passed as `scope_key` to adapter.normalize()
    "records": [ <upstream-shape>, ... ]
  }

The server resolves `adapter_id` to a registered module under
`loaders.adapters.<adapter_id>`, runs `normalize(record, meta, scope_key)`
on each record to flatten upstream JSON into target columns, then writes
via core.ingest_records.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..auth import require_identity
from ..ingest import adapter_dispatch, acl as ingress_acl
from ..ingest.adapter_registry import _split_adapter_id
from ..ingest.errors import ACLError, IngestError, SchemaIntrospectionError
from ..ingest.models import IngestResultModel
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_adapter")

router = APIRouter(prefix="", tags=["Ingress"])


class IngestAdapterBody(BaseModel):
    source_endpoint: Optional[str] = Field(default=None, max_length=200)
    scope_key: Optional[str] = Field(
        default=None,
        max_length=200,
        description=(
            "Adapter-specific scope key (e.g. the symbol the records pertain "
            "to). Passed as the third argument to `adapter.normalize`. "
            "Required by some adapters; safe to omit for others."
        ),
    )
    records: List[Dict[str, Any]] = Field(
        ..., min_length=1, max_length=50_000,
        description="List of upstream-shaped records (not yet flattened).",
    )


@router.post(
    "/ingest/adapter/{adapter_id}",
    summary="Push upstream-shaped records through a registered adapter",
    description=(
        "Looks up the per-target-table adapter (e.g. `news_articles`, "
        "`fundamentals_income_statement`), runs `normalize()` on each "
        "record, then writes the resulting target-column rows via the "
        "same ingest_records core function the typed-row route uses. "
        "Use this when your data is in the original upstream JSON shape "
        "instead of pre-flattened column-shape. The full adapter list is "
        "at `GET /catalog/ingress/adapters`."
    ),
    response_model=IngestResultModel,
)
async def post_adapter(
    request: Request,
    adapter_id: str,
    body: IngestAdapterBody = Body(...),
    identity: Identity = Depends(require_identity),
):
    # Resolve adapter to (schema, table) so we can ACL-check.
    schema, table = _split_adapter_id(adapter_id)
    try:
        ingress_acl.check_can_write(identity.role, schema, table)
    except ACLError as e:
        raise HTTPException(status_code=403, detail=str(e))

    src = f"ingress:{identity.sub}"
    declared = body.source_endpoint
    src_endpoint = declared or f"ingress:adapter:{adapter_id}"
    ua = request.headers.get("user-agent")

    def _run() -> Dict[str, Any]:
        with connection() as conn:
            result = adapter_dispatch.dispatch(
                conn,
                adapter_id=adapter_id,
                records=body.records,
                scope_key=body.scope_key or "",
                source=src,
                source_endpoint=src_endpoint,
                submitted_by=identity.sub,
                declared_endpoint=declared,
                user_agent=ua,
            )
            return result.to_dict()

    try:
        return await asyncio.to_thread(_run)
    except SchemaIntrospectionError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400), detail=str(e))
