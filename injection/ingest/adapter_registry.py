"""Optional bridge into the `loaders/adapters` normalize modules.

Adapter mode (`POST /ingest/adapter/{adapter_id}`) flattens upstream-shaped
JSON into target columns by calling a per-table `normalize()` from the
`loaders/adapters` tree. That tree is provider-coupled and lives OUTSIDE this
repo — it is mounted (read-only) under $FINAI_ROOT only on deployments that
want adapter mode.

This bridge is therefore **optional and best-effort**:
  * If the `loaders/` tree is found above this file (or via $FINAI_ROOT), it
    is added to sys.path and `loaders.adapters` is imported — adapter mode works.
  * If it is absent, we degrade gracefully: `loaders_adapters` stays None,
    `list_adapters()` returns [], and `get_adapter()` raises an IngestError
    that the route layer maps to HTTP 503.

The write engine itself (COPY-staging + merge) is vendored locally in
`injection/writeengine.py`; it does NOT depend on this bridge. So
typed / stream / file / blob / webhook / sandbox modes are fully standalone
and work with `loaders/` unmounted — only adapter mode needs this.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from .errors import IngestError

log = logging.getLogger("findata.ingest.adapter_registry")


def _resolve_findata_root() -> str | None:
    """Find the project root (the directory containing CLAUDE.md + loaders/).

    Returns None (never raises) when no such tree is present — adapter mode is
    optional, and the rest of the service must still import cleanly.
    """
    env = os.environ.get("FINAI_ROOT")
    if env and Path(env).is_dir() and (Path(env) / "loaders").is_dir():
        return env
    # Walk up from this file looking for a CLAUDE.md + loaders/ pair.
    here = Path(__file__).resolve()
    for parent in (here.parent, *here.parents):
        if (parent / "CLAUDE.md").exists() and (parent / "loaders").is_dir():
            return str(parent)
    return None


FINAI_ROOT = _resolve_findata_root()

loaders_adapters = None  # type: ignore[assignment]
if FINAI_ROOT:
    if FINAI_ROOT not in sys.path:
        sys.path.insert(0, FINAI_ROOT)
        log.info("inserted %s onto sys.path for loaders.adapters imports", FINAI_ROOT)
    try:
        from loaders import adapters as loaders_adapters  # noqa: E402
    except Exception as e:  # pragma: no cover — loaders/adapters is optional
        log.warning("loaders.adapters not importable: %s — adapter mode will 503", e)
        loaders_adapters = None  # type: ignore[assignment]
else:
    log.info("no loaders/ tree found — adapter mode disabled (typed/stream/file/blob still work)")


def _split_adapter_id(adapter_id: str) -> tuple[str, str]:
    """Split an adapter id like 'fundamentals_income_statement' or
    'fundamentals.income_statement' into (schema, table)."""
    if "." in adapter_id:
        schema, _, table = adapter_id.partition(".")
        return schema, table
    schema, _, table = adapter_id.partition("_")
    return schema, table


def _adapter_unavailable() -> IngestError:
    err = IngestError(
        "adapter mode unavailable on this deployment: the loaders/ tree is not "
        "mounted. Use typed mode (POST /ingest/{schema}/{table}) instead."
    )
    err.http_status = 503  # route layer reads getattr(e, "http_status", 400)
    return err


def get_adapter(adapter_id: str):
    """Resolve a loaders.adapters module by id (e.g. 'fundamentals_income_statement').

    Returns (module_or_None, schema, table). Raises a 503 IngestError when the
    loaders/ tree is not present (adapter mode disabled on this deployment).
    """
    if loaders_adapters is None:
        raise _adapter_unavailable()
    schema, table = _split_adapter_id(adapter_id)
    return loaders_adapters.get(schema, table), schema, table


def list_adapters() -> list[dict]:
    """Enumerate the registered (schema, table) → adapter pairs.
    Returns [] when adapter mode is disabled. Used by GET /catalog/ingress/adapters."""
    if loaders_adapters is None:
        return []
    reg = getattr(loaders_adapters, "_REGISTRY", {})
    out = []
    for (schema, table), modname in sorted(reg.items()):
        out.append({
            "adapter_id": f"{schema}_{table}",
            "schema": schema,
            "table": table,
            "module": modname,
        })
    return out


__all__ = [
    "FINAI_ROOT",
    "loaders_adapters",
    "get_adapter",
    "list_adapters",
    "_split_adapter_id",
]
