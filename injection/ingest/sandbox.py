"""Sandbox + proposal queue for net-new data shapes.

When `core.ingest_records` would raise `SchemaIntrospectionError` (target
table doesn't exist yet), the route layer falls back here. We:

  1. Insert each record verbatim into `raw.ingress_drops` (jsonb) under
     ONE provenance.runs row stamped mode='sandbox'.
  2. Infer a per-column schema (type, nullable, one sample value) from
     the batch + any historical drops for the same target.
  3. Upsert one row in `provenance.ingress_proposals` per
     (declared_schema, declared_table) with the merged inference,
     bumping `drop_count` and refreshing `sample_records`.

An admin route (in routes/ingest_admin.py) reviews + approves: applies
the DDL, replays drops into the new typed table, updates ACL, marks the
proposal `applied`.

Inference rules:
  - bool > int > float > str (most-specific wins per column)
  - any null observed → nullable=true
  - jsonb-shaped value (dict / list) → 'jsonb'
  - timestamp-shaped string (parseable as ISO) → 'timestamptz'
  - date-shaped string (YYYY-MM-DD) → 'date'
  - everything else string → 'text'

Natural-key candidates: scalar columns whose values are unique across
the observed sample (capped at 200 distinct rows for the heuristic).
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2

from ..writeengine import engine as loaders_lib
from .errors import IngestError

log = logging.getLogger("findata.ingest.sandbox")

_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$"
)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")

# Inferred column names must round-trip as Postgres identifiers when
# admin approves the proposal. Reject anything that can't (with a
# camel-to-snake auto-conversion attempt first).
_SAFE_COL_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_CAMEL_RE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_RE_2 = re.compile(r"([a-z0-9])([A-Z])")

# Per-submitter pending-proposal cap — prevents one principal from
# spamming raw.ingress_drops with unreviewable proposals.
PENDING_PROPOSAL_CAP_PER_SUBMITTER = 10
# Per-proposal drop_count ceiling. After this many records accumulate
# on one proposal, refuse further sandboxing until admin reviews.
DROP_COUNT_CAP_PER_PROPOSAL = 100_000


def _to_snake(name: str) -> str:
    s = (name or "").replace("-", "_").replace(" ", "_")
    s = _CAMEL_RE_1.sub(r"\1_\2", s)
    s = _CAMEL_RE_2.sub(r"\1_\2", s)
    return s.lower()


def _safe_col(name: str) -> Optional[str]:
    """Return a postgres-safe identifier for `name`, or None if not coercible."""
    if not isinstance(name, str) or not name:
        return None
    if _SAFE_COL_RE.match(name):
        return name
    candidate = _to_snake(name)
    if _SAFE_COL_RE.match(candidate):
        return candidate
    return None


@dataclass
class SandboxResult:
    run_id: str
    proposal_id: str
    declared_schema: str
    declared_table: str
    received: int
    drops_inserted: int
    drop_count_total: int
    inferred_columns: List[str]
    inferred_key: List[str]
    proposal_status: str
    status: str = "sandboxed"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

def _infer_scalar_type(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "bigint"
    if isinstance(v, float):
        return "double precision"
    if isinstance(v, (dict, list)):
        return "jsonb"
    if isinstance(v, (datetime,)):
        return "timestamp with time zone"
    if isinstance(v, date):
        return "date"
    if isinstance(v, str):
        if _TIMESTAMP_RE.match(v):
            return "timestamp with time zone"
        if _DATE_RE.match(v):
            return "date"
        if _INT_RE.match(v):
            return "bigint"
        if _FLOAT_RE.match(v):
            return "double precision"
        return "text"
    return "text"


# Promotion order — more specific types are preferred when the same
# column has been observed with multiple shapes. (bigint, double precision)
# both fold into 'double precision'; (timestamp, text) promotes to 'text'.
_TYPE_RANK = {
    "null":                       -1,
    "boolean":                     0,
    "bigint":                      1,
    "double precision":            2,
    "date":                        3,
    "timestamp with time zone":    4,
    "jsonb":                       5,
    "text":                        6,
}


def _merge_types(a: str, b: str) -> str:
    if a == b:
        return a
    if a == "null":
        return b
    if b == "null":
        return a
    # bigint + double → double
    if {a, b} == {"bigint", "double precision"}:
        return "double precision"
    # date + timestamp → timestamp
    if {a, b} == {"date", "timestamp with time zone"}:
        return "timestamp with time zone"
    # any disagreement we can't reconcile → text (safest superset)
    return "text"


def infer_schema(records: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Walk N records, return {col: {type, nullable, sample}}.

    Column names are coerced to safe Postgres identifiers via _safe_col
    (camel-to-snake first, reject anything still unsafe). Unsafe keys
    are silently dropped from the inferred schema — they'll still appear
    verbatim in raw.ingress_drops.record (the jsonb landing zone), so
    no data is lost; the admin just doesn't see them as proposed columns.
    """
    cols: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        for k, v in rec.items():
            safe_name = _safe_col(k)
            if safe_name is None:
                continue
            t = _infer_scalar_type(v)
            entry = cols.get(safe_name)
            if entry is None:
                cols[safe_name] = {
                    "type": t if t != "null" else "text",
                    "nullable": (v is None),
                    "sample": v if v is not None else None,
                    "source_key": k,  # the original (unsafe-or-not) JSON key
                }
            else:
                entry["type"] = _merge_types(entry["type"], t)
                if v is None:
                    entry["nullable"] = True
                elif entry["sample"] is None:
                    entry["sample"] = v
    return cols


def suggest_natural_key(
    records: Sequence[Dict[str, Any]], cols: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Heuristic: pick the smallest set of columns whose tuples are unique
    across the sample. Prefers (timestamp-like + id-like) pairs; falls back
    to the first column whose values are all-distinct."""
    if not records:
        return []
    n = len(records)

    # First try every single column.
    for col, meta in cols.items():
        if meta["type"] in ("jsonb",):
            continue
        vals = [r.get(col) for r in records if isinstance(r, dict)]
        if all(v is not None for v in vals) and len(set(map(repr, vals))) == n:
            return [col]

    # Two-column tuple: prefer a timestamp + a string-y col.
    ts_cols = [c for c, m in cols.items()
               if m["type"] in ("timestamp with time zone", "date")]
    other_cols = [c for c, m in cols.items()
                  if c not in ts_cols and m["type"] not in ("jsonb",)]
    for ts in ts_cols:
        for oc in other_cols:
            vals = [(r.get(ts), r.get(oc)) for r in records if isinstance(r, dict)]
            if all(v[0] is not None and v[1] is not None for v in vals) \
               and len(set(map(repr, vals))) == n:
                return [oc, ts]  # natural shape: (id, ts)

    return []


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _to_jsonb(v: Any) -> str:
    def _default(x):
        if isinstance(x, (datetime, date)):
            return x.isoformat()
        return str(x)
    return json.dumps(v, default=_default)


def write_drops(
    conn: psycopg2.extensions.connection,
    *,
    declared_schema: str,
    declared_table: str,
    records: Sequence[Dict[str, Any]],
    source: str,
    source_endpoint: str,
    source_run_id: uuid.UUID,
    submitted_by: str,
    natural_key_hint: Optional[str] = None,
) -> int:
    """Insert every record into raw.ingress_drops under one run. Returns count."""
    if not records:
        return 0
    rows = []
    for rec in records:
        rows.append((
            declared_schema, declared_table,
            _to_jsonb(rec),
            natural_key_hint,
            source, source_endpoint, str(source_run_id), submitted_by,
        ))
    sql = (
        "INSERT INTO raw.ingress_drops "
        "(declared_schema, declared_table, record, natural_key_hint, "
        " source, source_endpoint, source_run_id, submitted_by) "
        "VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::uuid, %s)"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_proposal(
    conn: psycopg2.extensions.connection,
    *,
    declared_schema: str,
    declared_table: str,
    proposer_sub: str,
    proposer_role: str,
    new_records: Sequence[Dict[str, Any]],
    drops_added: int,
) -> Tuple[str, Dict[str, Any], List[str], str]:
    """Merge inference from new_records with any existing proposal.
    Returns (proposal_id, inferred_schema, inferred_key, status)."""

    # Fetch existing proposal if present.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT proposal_id::text, inferred_schema, inferred_key, "
            "       sample_records, drop_count, status "
            "  FROM provenance.ingress_proposals "
            " WHERE declared_schema=%s AND declared_table=%s",
            (declared_schema, declared_table),
        )
        existing = cur.fetchone()

    new_inferred = infer_schema(new_records)
    if existing:
        proposal_id, prev_inferred, prev_key, prev_samples, prev_count, status = existing
        # Merge column inferences — keep prev columns, merge types per column
        # for the ones we saw again.
        merged: Dict[str, Dict[str, Any]] = dict(prev_inferred or {})
        for col, meta in new_inferred.items():
            prev = merged.get(col)
            if prev is None:
                merged[col] = dict(meta)
            else:
                merged[col] = {
                    "type": _merge_types(prev.get("type", "text"), meta["type"]),
                    "nullable": prev.get("nullable", False) or meta["nullable"],
                    "sample": prev.get("sample") if prev.get("sample") is not None else meta["sample"],
                }
        # If any column we expected before is missing this batch → it
        # may still be nullable; mark it so.
        for col in merged:
            if col not in new_inferred:
                merged[col]["nullable"] = True
        # Refresh sample_records (cap 5) — bias toward newest seen.
        cap = 5
        samples = list(new_records[:cap])
        if len(samples) < cap and isinstance(prev_samples, list):
            samples.extend(prev_samples[: cap - len(samples)])
        nkey = suggest_natural_key(list(new_records), merged) or list(prev_key or [])
    else:
        merged = new_inferred
        samples = list(new_records[:5])
        nkey = suggest_natural_key(list(new_records), merged)
        status = "pending"
        proposal_id = None

    with conn.cursor() as cur:
        if proposal_id is None:
            cur.execute(
                """
                INSERT INTO provenance.ingress_proposals
                  (declared_schema, declared_table, proposer_sub,
                   proposer_role, inferred_schema, inferred_key,
                   sample_records, drop_count, status)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s)
                RETURNING proposal_id::text, status
                """,
                (
                    declared_schema, declared_table, proposer_sub,
                    proposer_role, json.dumps(merged), nkey, _to_jsonb(samples),
                    drops_added, "pending",
                ),
            )
            proposal_id, status = cur.fetchone()
        else:
            cur.execute(
                """
                UPDATE provenance.ingress_proposals
                   SET inferred_schema = %s::jsonb,
                       inferred_key    = %s,
                       sample_records  = %s::jsonb,
                       drop_count      = drop_count + %s,
                       updated_at      = now()
                 WHERE proposal_id = %s::uuid
                """,
                (json.dumps(merged), nkey, _to_jsonb(samples),
                 drops_added, proposal_id),
            )
    conn.commit()
    return proposal_id, merged, nkey, status


# ---------------------------------------------------------------------------
# Top-level helper used by the typed-route fallback
# ---------------------------------------------------------------------------

def land_in_sandbox(
    conn: psycopg2.extensions.connection,
    *,
    declared_schema: str,
    declared_table: str,
    records: Sequence[Dict[str, Any]],
    source: str,
    source_endpoint: str,
    submitted_by: str,
    proposer_role: str = "local",
    declared_endpoint: Optional[str] = None,
    user_agent: Optional[str] = None,
    natural_key_hint: Optional[str] = None,
) -> SandboxResult:
    """Sandbox-mode write: open a run, write drops, upsert proposal.

    Enforces two server-side caps to bound the cost of an unreviewed
    sandbox queue:
      - PENDING_PROPOSAL_CAP_PER_SUBMITTER pending proposals per principal
      - DROP_COUNT_CAP_PER_PROPOSAL records on one target before refusing
    """
    # ---- Cap check: too many pending proposals from this principal? ----
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM provenance.ingress_proposals "
            "WHERE proposer_sub = %s AND status = 'pending'",
            (submitted_by,),
        )
        n_pending = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT 1 FROM provenance.ingress_proposals "
            " WHERE declared_schema=%s AND declared_table=%s "
            "   AND proposer_sub=%s AND status='pending'",
            (declared_schema, declared_table, submitted_by),
        )
        already_pending_this_target = cur.fetchone() is not None
    if (n_pending >= PENDING_PROPOSAL_CAP_PER_SUBMITTER
            and not already_pending_this_target):
        e = IngestError(
            f"too many pending proposals ({n_pending}/"
            f"{PENDING_PROPOSAL_CAP_PER_SUBMITTER}); ask admin to review or "
            "reject existing ones at GET /admin/ingress/proposals?status=pending"
        )
        e.http_status = 429
        raise e

    # ---- Cap check: too many drops already on this target? ----
    with conn.cursor() as cur:
        cur.execute(
            "SELECT drop_count FROM provenance.ingress_proposals "
            " WHERE declared_schema=%s AND declared_table=%s",
            (declared_schema, declared_table),
        )
        row = cur.fetchone()
        existing_drops = int(row[0]) if row else 0
    if existing_drops >= DROP_COUNT_CAP_PER_PROPOSAL:
        e = IngestError(
            f"drop_count for {declared_schema}.{declared_table} has reached "
            f"the cap ({existing_drops}/{DROP_COUNT_CAP_PER_PROPOSAL}); "
            "ask admin to approve or reject the proposal"
        )
        e.http_status = 429
        raise e

    run_args: Dict[str, Any] = {
        "target_schema":      declared_schema,
        "target_table":       declared_table,
        "mode":               "sandbox",
        "submitted_by":       submitted_by,
        "proposer_role":      proposer_role,
        "n_records_received": len(records),
    }
    if declared_endpoint:
        run_args["declared_endpoint"] = declared_endpoint
    if user_agent:
        run_args["user_agent"] = user_agent
    run_id = loaders_lib.open_run(
        conn, endpoint_id="ingress:generic", args=run_args,
    )
    # Stamp submitted_by on the run row.
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE provenance.runs SET submitted_by = %s WHERE run_id = %s",
            (submitted_by, run_id),
        )
    conn.commit()

    try:
        drops = write_drops(
            conn, declared_schema=declared_schema, declared_table=declared_table,
            records=records,
            source=source, source_endpoint=source_endpoint,
            source_run_id=run_id, submitted_by=submitted_by,
            natural_key_hint=natural_key_hint,
        )
        proposal_id, inferred, nkey, status = upsert_proposal(
            conn,
            declared_schema=declared_schema, declared_table=declared_table,
            proposer_sub=submitted_by, proposer_role=proposer_role,
            new_records=records, drops_added=drops,
        )
        # Final drop_count visible to the response.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT drop_count FROM provenance.ingress_proposals "
                "WHERE proposal_id = %s::uuid",
                (proposal_id,),
            )
            total = cur.fetchone()[0]
        loaders_lib.close_run(
            conn, run_id, "ok",
            rows_inserted=drops, rows_updated=0, rows_failed=0,
        )
    except Exception as e:
        try:
            loaders_lib.close_run(
                conn, run_id, "failed",
                rows_inserted=0, rows_updated=0, rows_failed=len(records),
                error_text=str(e)[-4000:],
            )
        except Exception as close_err:
            log.warning("failed to close failed sandbox run %s: %s",
                        run_id, close_err)
        raise IngestError(f"sandbox landing failed: {e}") from e

    return SandboxResult(
        run_id=str(run_id),
        proposal_id=proposal_id,
        declared_schema=declared_schema,
        declared_table=declared_table,
        received=len(records),
        drops_inserted=drops,
        drop_count_total=int(total),
        inferred_columns=sorted(inferred.keys()),
        inferred_key=nkey,
        proposal_status=status,
    )
