"""Adapter-mode dispatch.

For each upstream-shaped record, calls
`loaders.adapters.<id>.normalize(record, meta, scope_key)`, collects the
target-column rows, then forwards them to core.ingest_records with
validate=False (the adapter already produced target-shaped dicts).

The meta argument we pass to normalize() mirrors what loaders/run.py builds
when running the legacy disk-file scrape path. The adapter doesn't need
the same shape that the scraper sees — `target_schema`, `target_table`,
`endpoint_id`, `field_map` are the load-bearing keys.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

import psycopg2

from .adapter_registry import _split_adapter_id, get_adapter
from .core import IngestResult, ingest_records
from .errors import IngestError

log = logging.getLogger("findata.ingest.adapter_dispatch")


def _maybe_dict(value) -> Dict[str, Any]:
    """Adapters sometimes return None for a record (e.g. records they
    decided to skip). Coerce None → {} so downstream loops don't NPE."""
    return value if isinstance(value, dict) else {}


def normalize_one(adapter_module, record: Dict[str, Any], meta: Dict[str, Any],
                  scope_key: str) -> Optional[Dict[str, Any]]:
    """Call adapter.normalize(record, meta, scope_key) with the right signature.

    A few legacy adapters use the 2-arg form (record, meta); detect via
    signature inspection.
    """
    import inspect
    fn = getattr(adapter_module, "normalize", None)
    if fn is None:
        raise IngestError(
            f"adapter {adapter_module.__name__} has no normalize() function"
        )
    try:
        sig = inspect.signature(fn)
        nargs = len([p for p in sig.parameters.values()
                     if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)])
    except (TypeError, ValueError):
        nargs = 3
    if nargs >= 3:
        result = fn(record, meta, scope_key)
    else:
        result = fn(record, meta)
    if result is None:
        return None
    if isinstance(result, list):
        # Some adapters fan out one upstream record into multiple rows
        # (e.g. raw.finnhub_financials_reported per-filing). The caller
        # below flattens with type-check.
        return result  # type: ignore[return-value]
    return _maybe_dict(result)


def dispatch(
    conn: psycopg2.extensions.connection,
    *,
    adapter_id: str,
    records: Sequence[Dict[str, Any]],
    scope_key: str,
    source: str,
    source_endpoint: str,
    submitted_by: Optional[str] = None,
    declared_endpoint: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> IngestResult:
    """Run upstream-shaped records through the registered adapter, then
    persist via core.ingest_records.

    Returns the same IngestResult as core.ingest_records. If the adapter
    isn't registered, raises IngestError (caller maps to 404)."""
    adapter, schema, table = get_adapter(adapter_id)
    if adapter is None:
        raise IngestError(
            f"no adapter registered for {adapter_id!r}; available adapters "
            f"are listed at GET /catalog/ingress/adapters (v2)"
        )

    # `meta` mirrors what loaders/run.py builds for the legacy disk path.
    # Only the four keys an adapter actually reads are populated; others
    # default to None or empty.
    meta: Dict[str, Any] = {
        "target_schema": schema,
        "target_table": table,
        "endpoint_id": f"ingress:adapter:{adapter_id}",
        "field_map": None,
    }

    target_rows: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for idx, rec in enumerate(records):
        try:
            out = normalize_one(adapter, rec, meta, scope_key)
        except Exception as e:
            rejected.append({"index": idx, "error": f"adapter error: {e}", "record": rec})
            continue
        if out is None:
            # Adapter intentionally skipped this record.
            continue
        if isinstance(out, list):
            # Fan-out: each list entry is one target row.
            for sub in out:
                if isinstance(sub, dict) and sub:
                    target_rows.append(sub)
        elif isinstance(out, dict) and out:
            target_rows.append(out)

    # Reuse core.ingest_records — validate=False because the adapter
    # produced target-shaped rows. core still strips SERVER_STAMPED_COLS.
    from . import lumilake as lumilake_hook
    result = ingest_records(
        conn,
        target_schema=schema,
        target_table=table,
        records=target_rows,
        source=source,
        source_endpoint=source_endpoint,
        submitted_by=submitted_by,
        declared_endpoint=declared_endpoint,
        mode="adapter",
        user_agent=user_agent,
        validate=False,
        on_finalize=lumilake_hook.submit_after_ingest,
    )
    # Splice any adapter-level rejections in alongside core's (none in
    # validate=False mode, but stay defensive).
    if rejected:
        result.rejected.extend(rejected)
        result.failed += len(rejected)
        if result.status == "ok":
            result.status = "partial"
    return result
