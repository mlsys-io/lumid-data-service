"""Admin self-service for the ingress plane.

Routes (all super_admin or local-key only):
  POST   /admin/ingress/webhooks            — create a webhook (returns plaintext secret ONCE)
  GET    /admin/ingress/webhooks            — list webhooks (owner_sub-scoped for non-admin)
  DELETE /admin/ingress/webhooks/{id}       — revoke (set active=false)
  POST   /admin/ingress/acl                 — grant a (role, schema, table) row
  DELETE /admin/ingress/acl                 — revoke
  POST   /admin/ingress/refresh-schemas     — invalidate per-table Pydantic cache
  POST   /admin/ingress/refresh-acl         — invalidate the ACL cache

All routes require role in {'super_admin', 'local'} — the same set that
gets blanket-allow ACL by seed. Non-admin partners can't manage webhooks
in v2; that's a v3 polish.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
from hashlib import sha256
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field

from ..auth import require_identity
from ..ingest import acl as ingress_acl
from ..ingest import proposals as ingress_proposals
from ..ingest import validation as ingress_validation
from ..ingest.adapter_registry import _split_adapter_id
from ..ingest.errors import IngestError
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_admin")

router = APIRouter(prefix="/admin/ingress", tags=["Ingress admin"])


def _require_admin(identity: Identity) -> None:
    if identity.role not in ("super_admin", "local"):
        raise HTTPException(status_code=403, detail="super_admin or local-key required")


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

class CreateWebhookBody(BaseModel):
    label: Optional[str] = Field(default=None, max_length=120)
    target_schema: Optional[str] = Field(default=None, max_length=63)
    target_table: Optional[str] = Field(default=None, max_length=63)
    adapter_id: Optional[str] = Field(default=None, max_length=120)
    source_endpoint: Optional[str] = Field(default=None, max_length=200)
    owner_sub: Optional[str] = Field(
        default=None, max_length=200,
        description=(
            "Optional override for the webhook's owner identity. Defaults "
            "to the caller's `sub`. Useful when a super_admin provisions a "
            "webhook on behalf of a third party."
        ),
    )
    notes: Optional[str] = Field(default=None)


def _validate_target(body: CreateWebhookBody) -> None:
    """Enforce the typed XOR adapter constraint at the API layer too —
    matches the CHECK on provenance.webhooks."""
    has_typed = bool(body.target_schema and body.target_table)
    has_adapter = bool(body.adapter_id)
    if has_typed and has_adapter:
        raise HTTPException(
            status_code=400,
            detail="provide either {target_schema, target_table} OR adapter_id, not both",
        )
    if not (has_typed or has_adapter):
        raise HTTPException(
            status_code=400,
            detail="provide either {target_schema, target_table} OR adapter_id",
        )
    if has_adapter:
        # Split sanity-check; module existence verified lazily at call time.
        sch, tbl = _split_adapter_id(body.adapter_id or "")
        if not sch or not tbl:
            raise HTTPException(status_code=400, detail=f"unparseable adapter_id {body.adapter_id!r}")


@router.post(
    "/webhooks",
    summary="Create a webhook (returns plaintext secret ONCE)",
)
async def create_webhook(
    body: CreateWebhookBody = Body(...),
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)
    _validate_target(body)
    owner = body.owner_sub or identity.sub
    secret = secrets.token_urlsafe(32)
    secret_hash = sha256(secret.encode("utf-8")).hexdigest()

    def _insert() -> str:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO provenance.webhooks (
                        label, owner_sub, secret_hash, secret_plain,
                        target_schema, target_table, adapter_id,
                        source_endpoint, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING webhook_id::text
                    """,
                    (
                        body.label, owner, secret_hash, secret,
                        body.target_schema, body.target_table, body.adapter_id,
                        body.source_endpoint, body.notes,
                    ),
                )
                wid = cur.fetchone()[0]
            conn.commit()
            return wid

    wid = await asyncio.to_thread(_insert)
    log.info("created webhook %s for %s by %s", wid, owner, identity.sub)
    return {
        "webhook_id": wid,
        "secret": secret,
        "_warning": (
            "Store this secret now — it will not be retrievable later. "
            "Use it to compute X-Webhook-Signature: hex(hmac_sha256(secret, body))."
        ),
        "owner_sub": owner,
        "target_schema": body.target_schema,
        "target_table": body.target_table,
        "adapter_id": body.adapter_id,
        "label": body.label,
    }


@router.get(
    "/webhooks",
    summary="List webhooks (super_admin sees all; others see their own)",
)
async def list_webhooks(identity: Identity = Depends(require_identity)):
    _require_admin(identity)

    def _select() -> List[Dict[str, Any]]:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT webhook_id::text, owner_sub, label,
                           target_schema, target_table, adapter_id,
                           source_endpoint, active, created_at,
                           last_used_at, use_count
                      FROM provenance.webhooks
                     ORDER BY created_at DESC
                    """
                )
                rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "webhook_id": r[0],
                "owner_sub": r[1],
                "label": r[2],
                "target_schema": r[3],
                "target_table": r[4],
                "adapter_id": r[5],
                "source_endpoint": r[6],
                "active": r[7],
                "created_at": r[8].isoformat() if r[8] else None,
                "last_used_at": r[9].isoformat() if r[9] else None,
                "use_count": r[10],
            })
        return out

    rows = await asyncio.to_thread(_select)
    return {"count": len(rows), "webhooks": rows}


@router.delete(
    "/webhooks/{webhook_id}",
    summary="Revoke a webhook (active=false)",
)
async def revoke_webhook(
    webhook_id: str, identity: Identity = Depends(require_identity),
):
    _require_admin(identity)

    def _update() -> int:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE provenance.webhooks SET active = false WHERE webhook_id::text = %s",
                    (webhook_id,),
                )
                affected = cur.rowcount
            conn.commit()
            return affected

    n = await asyncio.to_thread(_update)
    if n == 0:
        raise HTTPException(status_code=404, detail=f"webhook {webhook_id!r} not found")
    return {"webhook_id": webhook_id, "active": False}


# ---------------------------------------------------------------------------
# ACL grants
# ---------------------------------------------------------------------------

class GrantACLBody(BaseModel):
    role: str = Field(..., max_length=120)
    target_schema: str = Field(..., max_length=63)
    target_table: str = Field(..., max_length=63)
    can_write: bool = True
    notes: Optional[str] = Field(default=None)


@router.post(
    "/acl",
    summary="Grant a role write access to a target (or upsert can_write)",
)
async def grant_acl(
    body: GrantACLBody = Body(...),
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)

    def _upsert() -> None:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO provenance.ingress_acl
                        (role, target_schema, target_table, can_write, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (role, target_schema, target_table) DO UPDATE
                    SET can_write = EXCLUDED.can_write,
                        notes     = EXCLUDED.notes
                    """,
                    (body.role, body.target_schema, body.target_table,
                     body.can_write, body.notes),
                )
            conn.commit()
        ingress_acl.invalidate()

    await asyncio.to_thread(_upsert)
    return {**body.model_dump(), "_status": "applied"}


@router.delete(
    "/acl",
    summary="Revoke an ACL row by (role, schema, table)",
)
async def revoke_acl(
    role: str, target_schema: str, target_table: str,
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)

    def _delete() -> int:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM provenance.ingress_acl "
                    "WHERE role=%s AND target_schema=%s AND target_table=%s",
                    (role, target_schema, target_table),
                )
                n = cur.rowcount
            conn.commit()
        ingress_acl.invalidate()
        return n

    n = await asyncio.to_thread(_delete)
    if n == 0:
        raise HTTPException(status_code=404, detail="no ACL row matched")
    return {"role": role, "target_schema": target_schema, "target_table": target_table, "deleted": n}


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

@router.post(
    "/refresh-schemas",
    summary="Invalidate the per-table Pydantic schema cache",
    description=(
        "Call after DDL changes so the next POST /ingest/... uses the new "
        "column set. No-op otherwise."
    ),
)
async def refresh_schemas(identity: Identity = Depends(require_identity)):
    _require_admin(identity)
    ingress_validation.refresh()
    return {"status": "cleared"}


@router.post(
    "/refresh-acl",
    summary="Invalidate the in-process ACL cache",
)
async def refresh_acl(identity: Identity = Depends(require_identity)):
    _require_admin(identity)
    ingress_acl.invalidate()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# Proposals (net-new-shape review queue)
# ---------------------------------------------------------------------------

class ApproveProposalBody(BaseModel):
    target_schema: Optional[str] = Field(default=None, max_length=63,
        description="Override schema. Defaults to declared_schema.")
    target_table: Optional[str] = Field(default=None, max_length=63,
        description="Override table. Defaults to declared_table.")
    natural_key: Optional[List[str]] = Field(default=None,
        description="Override the inferred UNIQUE-key cols.")
    column_overrides: Optional[Dict[str, Dict[str, Any]]] = Field(default=None,
        description=("Per-inferred-column overrides: "
                     "{<inferred_col>: {name?, type?, nullable?}}."))
    allowed_schemas: Optional[List[str]] = Field(default=None,
        description=("Schemas this proposal may target. Defaults to ['raw']; "
                     "pass ['*'] for unrestricted."))
    review_notes: Optional[str] = None


class RejectProposalBody(BaseModel):
    review_notes: Optional[str] = None


@router.get(
    "/proposals",
    summary="List ingress proposals (super_admin sees all)",
)
async def list_proposals(
    status: Optional[str] = None, proposer_sub: Optional[str] = None,
    limit: int = 50,
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)
    def _run():
        with connection() as conn:
            return ingress_proposals.list_proposals(
                conn, status=status, proposer_sub=proposer_sub, limit=limit,
            )
    rows = await asyncio.to_thread(_run)
    return {"count": len(rows), "proposals": rows}


@router.get(
    "/proposals/{proposal_id}",
    summary="Fetch one proposal (inferred schema + sample + drop_count)",
)
async def get_proposal(
    proposal_id: str, identity: Identity = Depends(require_identity),
):
    _require_admin(identity)
    def _run():
        with connection() as conn:
            return ingress_proposals.fetch_proposal(conn, proposal_id)
    row = await asyncio.to_thread(_run)
    if row is None:
        raise HTTPException(status_code=404,
                            detail=f"unknown proposal {proposal_id!r}")
    return row


@router.post(
    "/proposals/{proposal_id}/approve",
    summary="Apply DDL + auto-backfill sandbox drops in one transaction",
    description=(
        "Creates the target table with the proposal's inferred schema "
        "(admin can override columns and natural key), inserts the staged "
        "rows from raw.ingress_drops into the new table, grants ACL, and "
        "marks the proposal `applied`. All in ONE transaction — any "
        "failure rolls back."
    ),
)
async def approve_proposal(
    proposal_id: str,
    body: ApproveProposalBody = Body(default_factory=ApproveProposalBody),
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)
    def _run():
        with connection() as conn:
            try:
                res = ingress_proposals.approve(
                    conn,
                    proposal_id=proposal_id,
                    reviewer_sub=identity.sub,
                    target_schema=body.target_schema,
                    target_table=body.target_table,
                    natural_key=body.natural_key,
                    column_overrides=body.column_overrides,
                    allowed_schemas=body.allowed_schemas,
                    review_notes=body.review_notes,
                )
            except IngestError as e:
                conn.rollback()
                raise
        return {
            "proposal_id":     res.proposal_id,
            "applied_table":   res.applied_table,
            "columns":         res.columns,
            "natural_key":     res.natural_key,
            "backfilled_rows": res.backfilled_rows,
            "acl_granted":     res.acl_granted,
            "status":          "applied",
        }
    try:
        return await asyncio.to_thread(_run)
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400),
                            detail=str(e))


@router.post(
    "/proposals/{proposal_id}/reject",
    summary="Reject a pending proposal (drops stay in raw.ingress_drops)",
)
async def reject_proposal(
    proposal_id: str,
    body: RejectProposalBody = Body(default_factory=RejectProposalBody),
    identity: Identity = Depends(require_identity),
):
    _require_admin(identity)
    def _run():
        with connection() as conn:
            return ingress_proposals.reject(
                conn, proposal_id=proposal_id,
                reviewer_sub=identity.sub,
                review_notes=body.review_notes,
            )
    try:
        return await asyncio.to_thread(_run)
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400),
                            detail=str(e))
