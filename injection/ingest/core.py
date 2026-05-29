"""The single findata write function.

Both HTTP routes (via asyncio.to_thread) and phase4 scrapers (post-migration,
in-process) call this. It opens (or reuses) a provenance.runs row, runs
loaders.lib.copy_into_staging + merge_staging_into_target, and stamps the
run with status + counts on the way out.

This deliberately wraps `loaders/lib.py` rather than duplicating the COPY +
DISTINCT-FROM merge. The merge behaviour (re-runs produce 0/0; non-key
column changes UPDATE; provenance columns always refresh) is load-bearing
for 90 adapters' idempotency — we reuse it untouched.
"""
from __future__ import annotations

import json
import logging
import re
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import psycopg2

from ..writeengine import engine as loaders_lib
from .errors import IngestError, SchemaIntrospectionError

log = logging.getLogger("findata.ingest.core")


# Regex for partner-declared source_endpoint strings. Permissive but bounded —
# rejects newlines, control chars, absurd lengths.
_SOURCE_ENDPOINT_RE = re.compile(r"^[A-Za-z0-9_:/?=&.\-]{1,200}$")


@dataclass
class IngestResult:
    run_id: str
    target_schema: str
    target_table: str
    received: int
    inserted: int
    updated: int
    failed: int
    rejected: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "ok"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # run_id always string for JSON.
        d["run_id"] = str(self.run_id)
        return d


def _coerce_records(model_cls, records: Sequence[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validate each record against the per-table Pydantic model.

    Returns (parsed, rejected). Each `parsed` entry is the model_dump dict
    (exclude_unset=True so defaults aren't materialized into NULL slots).
    """
    parsed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for idx, rec in enumerate(records):
        try:
            m = model_cls.model_validate(rec)
        except Exception as e:
            rejected.append({"index": idx, "error": str(e), "record": rec})
            continue
        parsed.append(m.model_dump(exclude_unset=True))
    return parsed, rejected


def ingest_records(
    conn: psycopg2.extensions.connection,
    *,
    target_schema: str,
    target_table: str,
    records: Sequence[Dict[str, Any]],
    source: str,
    source_endpoint: str,
    submitted_by: Optional[str] = None,
    run_id: Optional[uuid.UUID] = None,
    credential_label: Optional[str] = None,
    declared_endpoint: Optional[str] = None,
    mode: str = "typed",
    user_agent: Optional[str] = None,
    validate: bool = True,
    on_finalize: Optional[Callable[["IngestResult", Dict[str, Any]], None]] = None,
) -> IngestResult:
    """Write `records` to `target_schema.target_table` with full provenance.

    Behaviour:
      - If `run_id` is None, opens a new provenance.runs row (status='running',
        submitted_by=…, args=…) before any COPY and closes it on the way out
        (status='ok' / 'partial' / 'failed').
      - If `run_id` is provided, the caller owns the lifecycle — this function
        WILL NOT call close_run. Use this for stream mode and scraper migration.
      - `records` is expected to be already in target-column shape (post-adapter
        or POST'd directly by a typed-mode caller).
      - `validate=True` (default) runs each record through the per-table Pydantic
        model. Set False ONLY when the caller has already validated (e.g.
        adapter mode after normalize()) — saves a redundant Pydantic pass.

    Provenance:
      - `source` is stamped verbatim onto every row. HTTP callers should pass
        f"ingress:{identity.sub}"; scraper callers pass 'fmp', 'finnhub', etc.
      - `source_endpoint` is stamped verbatim. For HTTP, the route layer
        validates against _SOURCE_ENDPOINT_RE before getting here.
      - `submitted_by` lands on provenance.runs (only the run row, not per
        fact row — that'd be redundant).

    Returns IngestResult. Raises only on infrastructure failures (DB down,
    target table missing); Pydantic rejections come back as the `rejected`
    list with status='partial' or 'ok' depending on whether any rows landed.
    """
    if not target_schema or not target_table:
        raise IngestError("target_schema and target_table are required")
    if not source:
        raise IngestError("source is required (use 'ingress:<sub>' or 'fmp'/'finnhub'/...)")
    if not source_endpoint or not _SOURCE_ENDPOINT_RE.match(source_endpoint):
        raise IngestError(
            f"source_endpoint must match {_SOURCE_ENDPOINT_RE.pattern} "
            "(got: " + repr(source_endpoint) + ")"
        )

    received = len(records)
    # Validate (if requested) BEFORE opening a run row, so a pure-validation
    # failure doesn't leave behind a 0/0 'failed' run row.
    rejected: List[Dict[str, Any]] = []
    if validate:
        from .validation import model_for
        try:
            Model = model_for(target_schema, target_table)
        except SchemaIntrospectionError:
            raise
        parsed, rejected = _coerce_records(Model, records)
    else:
        parsed = [dict(r) for r in records]

    # If every single record was rejected and no run row exists yet, short-
    # circuit: nothing to do, return 422-shape from the route layer.
    if validate and not parsed and rejected:
        return IngestResult(
            run_id="",
            target_schema=target_schema,
            target_table=target_table,
            received=received,
            inserted=0,
            updated=0,
            failed=len(rejected),
            rejected=rejected,
            status="failed",
        )

    # Open or adopt a provenance.runs row.
    owned_run = run_id is None
    if owned_run:
        run_args: Dict[str, Any] = {
            "target_schema": target_schema,
            "target_table": target_table,
            "mode": mode,
            "n_records_received": received,
        }
        if declared_endpoint:
            run_args["declared_endpoint"] = declared_endpoint
        if user_agent:
            run_args["user_agent"] = user_agent
        if submitted_by:
            run_args["submitted_by"] = submitted_by
        run_id = loaders_lib.open_run(
            conn,
            endpoint_id="ingress:generic",
            args=run_args,
            credential_label=credential_label,
        )
        # Stamp submitted_by on the runs row itself (it's a column, not args).
        if submitted_by:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE provenance.runs SET submitted_by = %s WHERE run_id = %s",
                    (submitted_by, run_id),
                )
            conn.commit()

    inserted = updated = 0
    status = "ok"
    error_text: Optional[str] = None
    try:
        if parsed:
            # Discover the writable column set and the natural key.
            target_cols_info = loaders_lib.get_target_columns(conn, target_schema, target_table)
            writable_cols = [c for c, _, _ in target_cols_info]
            # We pass each record's column subset (Pydantic model_dump with
            # exclude_unset gives us only the fields the caller actually
            # supplied). Build column union across all records, intersected
            # with writable columns (drop anything that's not in the table —
            # validation should have caught this already, but defensive).
            col_set = set()
            for r in parsed:
                col_set.update(r.keys())
            col_set &= set(writable_cols)
            # Drop server-stamped columns even if a caller smuggled them past
            # validation (we set those ourselves). NB: `raw` is NOT in this
            # set — partners/adapters legitimately supply it.
            from .validation import SERVER_STAMPED_COLS
            col_set -= SERVER_STAMPED_COLS
            cols = [c for c in writable_cols if c in col_set]
            if not cols:
                raise IngestError(
                    f"no usable columns after intersection with {target_schema}.{target_table}"
                )

            # Build (column-aligned) row tuples.
            rows_iter = ((r.get(c) for c in cols) for r in parsed)

            tmp_table, copied = loaders_lib.copy_into_staging(
                conn,
                columns=cols,
                rows_iter=rows_iter,
                schema=target_schema,
                table=target_table,
                source=source,
                source_endpoint=source_endpoint,
                source_run_id=run_id,
            )
            conflict_cols = loaders_lib.get_unique_columns(conn, target_schema, target_table)
            if not conflict_cols:
                raise IngestError(
                    f"{target_schema}.{target_table} has no UNIQUE/PRIMARY KEY — refusing to upsert"
                )
            inserted, updated = loaders_lib.merge_staging_into_target(
                conn, tmp_table, target_schema, target_table, conflict_cols
            )

        if rejected:
            status = "partial"
    except Exception as e:
        status = "failed"
        error_text = traceback.format_exc()[-4000:]
        log.exception("ingest_records to %s.%s failed", target_schema, target_table)
        if owned_run:
            try:
                loaders_lib.close_run(
                    conn, run_id, status,
                    rows_inserted=inserted, rows_updated=updated,
                    rows_failed=len(rejected),
                    error_text=error_text,
                )
            except Exception as close_err:
                log.warning("failed to close failed run %s: %s", run_id, close_err)
        # Surface to caller as IngestError so route layer maps to 4xx/5xx.
        raise IngestError(f"ingest failed: {e}") from e

    if owned_run:
        loaders_lib.close_run(
            conn, run_id, status,
            rows_inserted=inserted, rows_updated=updated,
            rows_failed=len(rejected),
        )

    result_obj = IngestResult(
        run_id=str(run_id) if run_id is not None else "",
        target_schema=target_schema,
        target_table=target_table,
        received=received,
        inserted=inserted,
        updated=updated,
        failed=len(rejected),
        rejected=rejected,
        status=status,
    )
    # Forward-compat seam — when a Lumilake handoff is wired (v3), the
    # route layer passes a callback here that POSTs job submission events
    # to Lumilake with the just-closed run summary. v2 callers always pass
    # None, so this is a no-op for now.
    if on_finalize is not None:
        try:
            on_finalize(result_obj, {
                "target_schema": target_schema, "target_table": target_table,
                "mode": mode, "declared_endpoint": declared_endpoint,
                "submitted_by": submitted_by,
            })
        except Exception:
            log.exception("on_finalize hook raised — swallowed (ingest succeeded)")
    return result_obj
