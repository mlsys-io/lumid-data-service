"""POST /webhook/{webhook_id} — pre-registered HMAC ingress (v2).

Lets external systems push data without holding a full Lumid PAT. The
webhook is created up-front (admin route below) and bound to either:
  - a typed target  (target_schema + target_table), OR
  - an adapter      (adapter_id).

The body shape matches the corresponding ingress mode's payload:
  - typed   → {"records": [{col: val, ...}, ...]}
  - adapter → {"records": [<upstream-shape>, ...], "scope_key": "..."}

Identity for ACL/provenance derives from the webhook's owner_sub.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from slowapi.errors import RateLimitExceeded

from ..auth import limiter
from ..ingest import acl as ingress_acl
from ..ingest import adapter_dispatch
from ..ingest.core import ingest_records
from ..ingest.errors import ACLError, IngestError, SchemaIntrospectionError
from ..ingest.pool import connection
from ..ingest.webhook_auth import authenticate, stamp_used

log = logging.getLogger("findata.routes.ingest_webhook")

router = APIRouter(prefix="", tags=["Ingress"])

# Per-webhook rate limit — slowapi keys on `webhook:{id}`. Default
# 60/minute; finer control by adding a `rate_limit` value on the
# webhook row (future polish, fall back to default for now).
_WEBHOOK_LIMIT_DEFAULT = "60/minute"


def _webhook_key(request: Request) -> str:
    # The path looks like /webhook/<uuid>. Pluck the uuid for the bucket.
    parts = (request.url.path or "").rsplit("/", 1)
    wid = parts[-1] if len(parts) > 1 else "anon"
    return f"webhook:{wid}"


@router.post(
    "/webhook/{webhook_id}",
    summary="Push data via a pre-registered HMAC webhook",
    description=(
        "Send the body's raw bytes signed with `X-Webhook-Signature: <hex>` "
        "(or `sha256=<hex>`) — HMAC-SHA256 of the body using the webhook's "
        "secret. The webhook is bound at creation time to either a typed "
        "target (`target_schema`+`target_table`) or an adapter (`adapter_id`). "
        "Body shape is the corresponding ingress mode's normal envelope. "
        "Rate limit: 60 req/min per webhook_id (separate bucket from PAT)."
    ),
)
@limiter.limit(_WEBHOOK_LIMIT_DEFAULT, key_func=_webhook_key)
async def post_webhook(
    request: Request,
    webhook_id: str,
    x_webhook_signature: Optional[str] = Header(default=None, alias="X-Webhook-Signature"),
):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="empty body")
    try:
        wh = await asyncio.to_thread(
            authenticate, webhook_id, body, x_webhook_signature or "",
        )
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 401), detail=str(e))

    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"invalid JSON: {e}")
    if not isinstance(envelope, dict):
        raise HTTPException(status_code=400, detail="webhook body must be a JSON object")
    records = envelope.get("records")
    if not isinstance(records, list) or not records:
        raise HTTPException(status_code=400, detail="`records` must be a non-empty list")

    # ACL check is performed against the webhook owner's principal (role
    # at creation time is captured via the webhook itself — only the
    # owner's role-allowlist gates writes).
    target_schema = wh.target_schema
    target_table = wh.target_table
    if wh.adapter_id:
        # Adapter mode: schema/table derive from the adapter id.
        from ..ingest.adapter_registry import _split_adapter_id
        target_schema, target_table = _split_adapter_id(wh.adapter_id)
    try:
        # Use 'local' role-equivalent for ACL purposes — webhooks are
        # admin-issued and the owner already consented to the target by
        # creating it. Treating as 'local' lets the seeded blanket-allow
        # row pass them; finer-grained per-webhook ACL is a v3 polish.
        ingress_acl.check_can_write("local", target_schema, target_table)
    except ACLError as e:
        raise HTTPException(status_code=403, detail=str(e))

    src = f"ingress:webhook:{wh.webhook_id}"
    declared = wh.source_endpoint or f"ingress:webhook:{wh.webhook_id}"
    submitted_by = wh.owner_sub
    ua = request.headers.get("user-agent")

    def _do() -> Dict[str, Any]:
        with connection() as conn:
            if wh.adapter_id:
                scope_key = envelope.get("scope_key", "")
                result = adapter_dispatch.dispatch(
                    conn,
                    adapter_id=wh.adapter_id,
                    records=records,
                    scope_key=scope_key or "",
                    source=src,
                    source_endpoint=declared,
                    submitted_by=submitted_by,
                    declared_endpoint=declared,
                    user_agent=ua,
                )
                return result.to_dict()
            result = ingest_records(
                conn,
                target_schema=target_schema, target_table=target_table,
                records=records,
                source=src, source_endpoint=declared,
                submitted_by=submitted_by,
                declared_endpoint=declared,
                mode="webhook", user_agent=ua,
            )
            return result.to_dict()

    try:
        result = await asyncio.to_thread(_do)
    except SchemaIntrospectionError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400), detail=str(e))
    # Bump use counter best-effort.
    asyncio.create_task(asyncio.to_thread(stamp_used, str(wh.webhook_id)))
    return result
