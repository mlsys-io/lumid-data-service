"""Ingress-discovery catalog — the read endpoints that need ingest introspection.

These four endpoints live in the injection service (not the read service)
because each reaches into ingest internals — the per-table Pydantic schema
factory, the adapter registry, the blob-storage config, and the sandbox
proposal store. The read service proxies them here over HTTP.

  GET /catalog/tables/{schema}/{table}/schema.json — JSON Schema for typed writes
  GET /catalog/ingress                              — one-call ingress overview
  GET /catalog/ingress/adapters                     — registered adapters (or [])
  GET /catalog/ingress/proposals                    — caller's sandbox proposals

Everything here uses the sync psycopg2 ingest pool (the injection service has
no asyncpg pool), hopping to a worker thread for the DB-bound calls.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_identity
from ..ingest import storage as ingest_storage
from ..ingest import validation as ingest_validation
from ..ingest.adapter_registry import list_adapters
from ..ingest.pool import connection
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingress_catalog")

router = APIRouter(prefix="/catalog", tags=["Catalog"])

# Mirrors api.catalog.core.USER_SCHEMAS — the schemas wildcard ACL rows expand over.
USER_SCHEMAS = (
    "reference", "market", "fundamentals", "estimates", "ownership",
    "events", "news", "regulatory", "macro", "prediction_markets",
    "raw", "provenance",
)


def _list_writable_for_role_sync(role: str) -> List[Dict[str, Any]]:
    """Sync re-implementation of catalog.core.list_writable_for_role.

    Resolves provenance.ingress_acl for `role`, expanding ('*','*') to every
    base table in USER_SCHEMAS and (schema,'*') to every table in that schema.
    Returns the same row shape the read service's /catalog/ingress/writable does.
    """
    out: List[Dict[str, Any]] = []
    seen: set = set()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT target_schema, target_table, notes
                  FROM provenance.ingress_acl
                 WHERE role = %s AND can_write = true
                 ORDER BY target_schema, target_table
                """,
                (role,),
            )
            rules = cur.fetchall()

            for sch, tbl, notes in rules:
                if sch == "*" and tbl == "*":
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                          FROM information_schema.tables
                         WHERE table_schema = ANY(%s)
                           AND table_type = 'BASE TABLE'
                         ORDER BY table_schema, table_name
                        """,
                        (list(USER_SCHEMAS),),
                    )
                    for r_sch, r_tbl in cur.fetchall():
                        key = (r_sch, r_tbl)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({
                            "schema": r_sch, "table": r_tbl,
                            "rule_source": "wildcard",
                            "schema_url": f"/catalog/tables/{r_sch}/{r_tbl}/schema.json",
                        })
                elif tbl == "*":
                    cur.execute(
                        """
                        SELECT table_name
                          FROM information_schema.tables
                         WHERE table_schema = %s
                           AND table_type = 'BASE TABLE'
                         ORDER BY table_name
                        """,
                        (sch,),
                    )
                    for (r_tbl,) in cur.fetchall():
                        key = (sch, r_tbl)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({
                            "schema": sch, "table": r_tbl,
                            "rule_source": "wildcard",
                            "schema_url": f"/catalog/tables/{sch}/{r_tbl}/schema.json",
                        })
                else:
                    key = (sch, tbl)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append({
                        "schema": sch, "table": tbl,
                        "rule_source": "explicit", "notes": notes,
                        "schema_url": f"/catalog/tables/{sch}/{tbl}/schema.json",
                    })
    return out


@router.get(
    "/tables/{schema}/{table}/schema.json",
    summary="JSON Schema for ingress writes",
    description=(
        "Returns the JSON Schema of the model used to validate "
        "POST /ingest/{schema}/{table} bodies. Required fields are NOT NULL "
        "non-provenance columns without defaults; optional ones are "
        "nullable / defaulted. Use this to validate client-side before "
        "pushing data."
    ),
)
async def get_table_schema_json(
    schema: str, table: str, _: Identity = Depends(require_identity),
) -> Dict[str, Any]:
    def _build():
        return ingest_validation.schema_json_for(schema, table)
    try:
        return await asyncio.to_thread(_build)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get(
    "/ingress",
    summary="One-call ingress discovery (for AI agents)",
    description=(
        "Returns everything a client needs to start writing data: supported "
        "wire formats, available ingress modes, the (schema, table) tables "
        "the calling identity can write to, the adapter count, and an "
        "example payload for each common mode."
    ),
)
async def get_ingress_overview(identity: Identity = Depends(require_identity)):
    writable = await asyncio.to_thread(_list_writable_for_role_sync, identity.role)
    adapters = list_adapters()
    return {
        "role": identity.role,
        "sub": identity.sub,
        "formats": [
            "json", "ndjson", "csv", "tsv", "xml",
            "yaml", "parquet", "arrow", "blob",
        ],
        "modes": ["typed", "adapter", "stream", "file", "webhook", "blob"],
        "blob_plane_enabled": ingest_storage.is_configured(),
        "adapters_count": len(adapters),
        "writable_count": len(writable),
        "writable_tables": writable[:50],
        "endpoints": {
            "typed":   "POST /ingest/{schema}/{table}",
            "adapter": "POST /ingest/adapter/{adapter_id}",
            "stream":  "POST /ingest/{schema}/{table}/stream  (Content-Type: application/x-ndjson)",
            "file":    "POST /ingest/{schema}/{table}/file    (multipart/form-data; field: file)",
            "blob":    "POST /ingest/blob                     (Content-Type: image/* | application/pdf | text/* | application/octet-stream)",
            "webhook": "POST /webhook/{webhook_id}            (X-Webhook-Signature: hex(hmac_sha256(secret, body)))",
        },
        "example_payloads": {
            "typed": {
                "source_endpoint": "ingress:example/typed",
                "records": [
                    {"url": "https://example.com/a",
                     "published_at": "2026-01-01T00:00:00Z",
                     "symbol": "AAPL", "headline": "example"},
                ],
            },
            "adapter": {
                "source_endpoint": "ingress:example/adapter",
                "scope_key": "AAPL",
                "records": [
                    {"symbol": "AAPL", "publishedDate": "2026-01-01",
                     "title": "example", "url": "https://example.com/a"},
                ],
            },
            "stream": "{<json record>}\\n{<json record>}\\n... (one record per line)",
            "webhook": {
                "headers": {"X-Webhook-Signature": "<hex(hmac_sha256(secret, body))>"},
                "body": {"records": [{"...": "...same shape as typed/adapter..."}]},
            },
        },
        "discovery": {
            "writable_full":   "GET /catalog/ingress/writable",
            "adapters_full":   "GET /catalog/ingress/adapters",
            "schema_for_table": "GET /catalog/tables/{schema}/{table}/schema.json",
        },
    }


@router.get(
    "/ingress/adapters",
    summary="List adapters available for /ingest/adapter/{id}",
    description=(
        "Every adapter id resolvable by POST /ingest/adapter/{id}, with its "
        "target schema + table. Empty when adapter mode is disabled on this "
        "deployment (no loaders/ tree mounted)."
    ),
)
async def get_adapters(_: Identity = Depends(require_identity)):
    adapters = list_adapters()
    return {"count": len(adapters), "adapters": adapters}


@router.get(
    "/ingress/proposals",
    summary="Sandbox proposals for the calling identity",
    description=(
        "Lists ingress proposals (net-new shapes the caller has pushed to "
        "but that don't have a typed target table yet). Super_admin / local "
        "see ALL proposals; others see only their own."
    ),
)
async def get_my_proposals(
    status: Optional[str] = None,
    identity: Identity = Depends(require_identity),
):
    from ..ingest import proposals as ingress_proposals
    only_self = None if identity.role in ("super_admin", "local") else identity.sub

    def _run():
        with connection() as conn:
            return ingress_proposals.list_proposals(
                conn, status=status, proposer_sub=only_self, limit=100,
            )

    rows = await asyncio.to_thread(_run)
    return {"count": len(rows), "proposals": rows}
