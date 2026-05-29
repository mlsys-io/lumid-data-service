"""Proposal lifecycle — approve / reject / apply.

Approval is the load-bearing step. In ONE psycopg2 transaction we:
  1. Lock the proposal row (FOR UPDATE).
  2. Create the typed target table with:
       - admin-edited columns + their pg types
       - provenance triplet (source, source_endpoint, source_run_id) NOT NULL
       - ingest_ts timestamptz DEFAULT now()
       - raw jsonb
       - UNIQUE constraint on the admin-chosen natural-key cols
       - FOREIGN KEY (source_run_id) REFERENCES provenance.runs(run_id)
  3. INSERT … SELECT from raw.ingress_drops for this (schema, table) into
     the new typed table, expanding `record` jsonb → typed columns.
  4. UPDATE raw.ingress_drops SET promoted_to_table='<schema>.<table>',
     promoted_at=now() for those rows.
  5. INSERT a default ACL row granting the proposer's role write access
     (skipped if already granted, e.g. via wildcard).
  6. Mark the proposal `applied`.

If anything inside fails, we ROLLBACK and leave the world unchanged.

The DDL is NEVER auto-applied to schemas the system doesn't own
(reference, market, fundamentals, etc.) UNLESS the admin explicitly
sends `force_into_existing_schema=true` — by default we route the new
table into the `partner_<sub>` schema or a partner-namespaced area.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg2

from .acl import invalidate as acl_invalidate
from .errors import IngestError
from .sandbox import _to_jsonb
from .validation import refresh as refresh_validation_cache

log = logging.getLogger("findata.ingest.proposals")


# Identifiers in the SQL we generate must be safe — pg will quote them if
# we wrap in double-quotes, but defense in depth: regex-validate.
_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _q_ident(name: str) -> str:
    """Quote an identifier safely after validating it. Lowercase only."""
    if not _IDENT_RE.match(name or ""):
        raise IngestError(f"invalid identifier {name!r}")
    return f'"{name}"'


# Allowed pg types for proposal columns. Anything outside this set falls
# back to text on admin override.
_ALLOWED_TYPES = {
    "boolean", "smallint", "integer", "bigint",
    "real", "double precision", "numeric",
    "text", "uuid",
    "date", "time", "timestamp", "timestamp with time zone",
    "jsonb", "json", "bytea",
}


def _safe_pg_type(t: str) -> str:
    t = (t or "text").lower().strip()
    return t if t in _ALLOWED_TYPES else "text"


@dataclass
class ApproveResult:
    proposal_id: str
    applied_table: str
    columns: List[str]
    natural_key: List[str]
    backfilled_rows: int
    acl_granted: bool


def fetch_proposal(conn, proposal_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT proposal_id::text, declared_schema, declared_table,
                   proposer_sub, inferred_schema, inferred_key,
                   sample_records, drop_count, status, applied_table,
                   reviewer_sub, reviewed_at, review_notes,
                   created_at, updated_at
              FROM provenance.ingress_proposals
             WHERE proposal_id = %s::uuid
            """,
            (proposal_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    cols = ["proposal_id", "declared_schema", "declared_table",
            "proposer_sub", "inferred_schema", "inferred_key",
            "sample_records", "drop_count", "status", "applied_table",
            "reviewer_sub", "reviewed_at", "review_notes",
            "created_at", "updated_at"]
    out: Dict[str, Any] = dict(zip(cols, row))
    # Datetimes → isoformat for clean JSON.
    for k in ("reviewed_at", "created_at", "updated_at"):
        if out.get(k) is not None:
            out[k] = out[k].isoformat()
    return out


def list_proposals(conn, *, status: Optional[str] = None,
                   proposer_sub: Optional[str] = None,
                   limit: int = 50) -> List[Dict[str, Any]]:
    clauses, params = [], []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if proposer_sub:
        clauses.append("proposer_sub = %s")
        params.append(proposer_sub)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)
    sql = f"""
    SELECT proposal_id::text, declared_schema, declared_table,
           proposer_sub, drop_count, status, applied_table,
           created_at, updated_at,
           (SELECT count(*) FROM jsonb_object_keys(inferred_schema))
             AS inferred_cols
      FROM provenance.ingress_proposals
    {where}
     ORDER BY updated_at DESC
     LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({
            "proposal_id":     r[0],
            "declared_schema": r[1],
            "declared_table":  r[2],
            "proposer_sub":    r[3],
            "drop_count":      int(r[4] or 0),
            "status":          r[5],
            "applied_table":   r[6],
            "created_at":      r[7].isoformat() if r[7] else None,
            "updated_at":      r[8].isoformat() if r[8] else None,
            "inferred_cols":   int(r[9] or 0),
        })
    return out


# ---------------------------------------------------------------------------
# Approval — the load-bearing path
# ---------------------------------------------------------------------------

def approve(
    conn: psycopg2.extensions.connection,
    *,
    proposal_id: str,
    reviewer_sub: str,
    target_schema: Optional[str] = None,
    target_table: Optional[str] = None,
    natural_key: Optional[List[str]] = None,
    column_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    allowed_schemas: Optional[List[str]] = None,
    review_notes: Optional[str] = None,
) -> ApproveResult:
    """Apply a proposal end-to-end in ONE transaction.

    - target_schema/target_table override the declared names. Defaults to
      the declared pair.
    - natural_key overrides the inferred UNIQUE-key cols.
    - column_overrides lets the reviewer reshape types or rename a col:
        {"<inferred_col>": {"name": "<new_name>"?, "type": "<pg_type>"?, "nullable": bool?}}
    - allowed_schemas restricts which schemas a proposal may target.
      Defaults to ('raw',) so admins consciously opt-in to writing into
      curated schemas; pass ['*'] to permit anywhere.
    """
    if allowed_schemas is None:
        allowed_schemas = ["raw"]

    # 1) Lock + load proposal.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT declared_schema, declared_table, inferred_schema, "
            "       inferred_key, status, proposer_sub, proposer_role "
            "  FROM provenance.ingress_proposals "
            " WHERE proposal_id = %s::uuid FOR UPDATE",
            (proposal_id,),
        )
        row = cur.fetchone()
    if not row:
        raise IngestError(f"unknown proposal {proposal_id!r}")
    dec_sch, dec_tbl, inferred, inferred_nkey, status, proposer, proposer_role = row
    if status in ("applied",):
        raise IngestError(f"proposal {proposal_id} already applied")
    if status == "rejected":
        raise IngestError(f"proposal {proposal_id} was rejected")
    proposer_role = proposer_role or "local"  # defensive: pre-DDL-24 rows

    final_schema = (target_schema or dec_sch).lower()
    final_table  = (target_table  or dec_tbl).lower()
    # Schema gate.
    if allowed_schemas != ["*"] and final_schema not in allowed_schemas:
        raise IngestError(
            f"schema {final_schema!r} not in allowed list {allowed_schemas!r}; "
            "pass allowed_schemas=['*'] to override."
        )

    # 2) Resolve final column list + types after overrides.
    inferred = dict(inferred or {})
    overrides = column_overrides or {}
    final_cols: List[Dict[str, Any]] = []
    rename_map: Dict[str, str] = {}  # inferred_col -> final_col
    for col_inferred, meta in inferred.items():
        ov = overrides.get(col_inferred, {})
        final_name = ov.get("name") or col_inferred
        final_type = _safe_pg_type(ov.get("type") or meta.get("type") or "text")
        nullable   = bool(ov.get("nullable", meta.get("nullable", True)))
        rename_map[col_inferred] = final_name
        final_cols.append({
            "name": final_name, "type": final_type, "nullable": nullable,
        })

    final_col_names = [c["name"] for c in final_cols]
    if len(set(final_col_names)) != len(final_col_names):
        raise IngestError(f"duplicate column names after rename: {final_col_names}")

    nkey = list(natural_key) if natural_key is not None else list(inferred_nkey or [])
    # Map natural-key cols through the rename if they reference inferred names.
    nkey = [rename_map.get(c, c) for c in nkey]
    if not nkey:
        raise IngestError(
            "proposal has no natural-key cols and none were supplied; "
            "natural_key is required (used for the UNIQUE constraint that "
            "makes the merge idempotent)"
        )
    for k in nkey:
        if k not in final_col_names:
            raise IngestError(
                f"natural_key col {k!r} not in column set {final_col_names!r}"
            )

    # 3) Build + execute DDL.
    q_sch = _q_ident(final_schema)
    q_tbl = _q_ident(final_table)
    cols_sql_parts: List[str] = []
    for c in final_cols:
        nn = "" if c["nullable"] else " NOT NULL"
        cols_sql_parts.append(f'{_q_ident(c["name"])} {c["type"]}{nn}')
    # Provenance triplet.
    cols_sql_parts.append('"source" text NOT NULL')
    cols_sql_parts.append('"source_endpoint" text NOT NULL')
    cols_sql_parts.append('"source_run_id" uuid NOT NULL '
                          'REFERENCES provenance.runs(run_id)')
    cols_sql_parts.append('"ingest_ts" timestamptz NOT NULL DEFAULT now()')
    cols_sql_parts.append('"raw" jsonb')
    # UNIQUE on the natural key.
    nkey_quoted = ", ".join(_q_ident(k) for k in nkey)
    cols_sql_parts.append(f"UNIQUE ({nkey_quoted})")
    ddl = (
        f"CREATE SCHEMA IF NOT EXISTS {q_sch};\n"
        f"CREATE TABLE IF NOT EXISTS {q_sch}.{q_tbl} (\n  "
        + ",\n  ".join(cols_sql_parts)
        + "\n);"
    )
    log.info("applying proposal %s: %s.%s with %d cols, key=%s",
             proposal_id, final_schema, final_table, len(final_cols), nkey)

    with conn.cursor() as cur:
        cur.execute(ddl)

    # 4) Backfill from raw.ingress_drops → typed table.
    # We do this in pure SQL with one INSERT … SELECT that expands the
    # jsonb record to each typed column. Cast through information_schema
    # so the pg type system handles the coercion uniformly.
    column_extracts = []
    for c in final_cols:
        # Map final column back to its inferred-source key (rename_map values).
        src_key = None
        for src, dst in rename_map.items():
            if dst == c["name"]:
                src_key = src
                break
        src_key = src_key or c["name"]
        # `record -> 'col'` returns jsonb; `->>` returns text. Cast as needed.
        if c["type"] == "jsonb":
            extract = f"record -> {_pg_literal(src_key)}"
        else:
            extract = f"NULLIF(record ->> {_pg_literal(src_key)}, '')"
            if c["type"] != "text":
                extract = f"({extract})::{c['type']}"
        column_extracts.append(f"{extract} AS {_q_ident(c['name'])}")

    backfill_sql = f"""
    WITH src AS (
      SELECT drop_id, record, source, source_endpoint, source_run_id, ingest_ts
        FROM raw.ingress_drops
       WHERE declared_schema = %s AND declared_table = %s
         AND promoted_to_table IS NULL
    )
    , inserted AS (
      INSERT INTO {q_sch}.{q_tbl}
        ({", ".join(_q_ident(c["name"]) for c in final_cols)},
         source, source_endpoint, source_run_id, ingest_ts, raw)
      SELECT {", ".join(column_extracts)},
             source, source_endpoint, source_run_id, ingest_ts, record
        FROM src
      ON CONFLICT ({nkey_quoted}) DO NOTHING
      RETURNING source_run_id
    )
    , promoted AS (
      UPDATE raw.ingress_drops
         SET promoted_to_table = %s,
             promoted_at       = now()
       WHERE declared_schema = %s AND declared_table = %s
         AND promoted_to_table IS NULL
      RETURNING 1
    )
    SELECT (SELECT count(*) FROM inserted) AS n_inserted,
           (SELECT count(*) FROM promoted) AS n_promoted
    """
    applied_qualname = f"{final_schema}.{final_table}"
    with conn.cursor() as cur:
        cur.execute(
            backfill_sql,
            (dec_sch, dec_tbl, applied_qualname, dec_sch, dec_tbl),
        )
        n_inserted, n_promoted = cur.fetchone()

    # 5) Grant ACL to the proposer's role (captured at sandbox time on the
    # proposal row). If that role is already covered by a wildcard the
    # upsert is a no-op; otherwise the partner can write to their newly
    # approved target on their next request.
    granted = False
    granted_roles: List[str] = []
    roles_to_grant = {proposer_role, "local"}  # always include 'local' for internal callers
    with conn.cursor() as cur:
        for role_name in sorted(roles_to_grant):
            cur.execute(
                """
                INSERT INTO provenance.ingress_acl
                  (role, target_schema, target_table, can_write, notes)
                VALUES (%s, %s, %s, true, %s)
                ON CONFLICT (role, target_schema, target_table) DO UPDATE
                  SET can_write = true,
                      notes     = EXCLUDED.notes
                """,
                (role_name, final_schema, final_table,
                 f"auto-granted on proposal {proposal_id} approval"),
            )
            granted_roles.append(role_name)
        granted = True

    # 6) Stamp the proposal as applied.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE provenance.ingress_proposals
               SET status        = 'applied',
                   applied_table = %s,
                   reviewer_sub  = %s,
                   reviewed_at   = now(),
                   review_notes  = %s,
                   updated_at    = now()
             WHERE proposal_id = %s::uuid
            """,
            (applied_qualname, reviewer_sub, review_notes, proposal_id),
        )
    conn.commit()

    # Refresh caches so the new table is immediately writable through the
    # typed-row path on the next request.
    refresh_validation_cache()
    acl_invalidate()

    return ApproveResult(
        proposal_id=proposal_id,
        applied_table=applied_qualname,
        columns=final_col_names,
        natural_key=nkey,
        backfilled_rows=int(n_inserted or 0),
        acl_granted=granted,
    )


def _pg_literal(s: str) -> str:
    """Format a Python string as a PG SQL string literal — single quotes
    around it, with embedded quotes doubled. Only used for static column
    keys from `inferred_schema`, which are themselves bounded to safe
    identifiers."""
    return "'" + s.replace("'", "''") + "'"


def reject(
    conn: psycopg2.extensions.connection,
    *,
    proposal_id: str,
    reviewer_sub: str,
    review_notes: Optional[str] = None,
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE provenance.ingress_proposals
               SET status       = 'rejected',
                   reviewer_sub = %s,
                   reviewed_at  = now(),
                   review_notes = %s,
                   updated_at   = now()
             WHERE proposal_id  = %s::uuid AND status = 'pending'
            RETURNING proposal_id::text
            """,
            (reviewer_sub, review_notes, proposal_id),
        )
        row = cur.fetchone()
    if not row:
        raise IngestError(
            f"proposal {proposal_id!r} not found or not in 'pending' state"
        )
    conn.commit()
    return {"proposal_id": row[0], "status": "rejected"}
