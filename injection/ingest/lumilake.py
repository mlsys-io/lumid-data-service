"""Lumilake handoff (v3).

After every successful HTTP-ingress write, optionally POST a small
notification to the Lumilake job server so downstream pipelines can react
(e.g. trigger a backfill, kick off an iceberg prefetch). The call is
fire-and-forget — it never blocks the ingest response, never raises into
the caller, and is a no-op when `FINDATA_LUMILAKE_BASE_URL` is unset.

Wiring:
  - Ingest routes assemble `submit_after_ingest(result, info)` and pass it
    to core.ingest_records as `on_finalize=...`.
  - The function is sync (called inside the sync ingest path) but spawns
    the actual HTTP POST as an asyncio task on the caller's loop. If no
    loop is running (e.g. called from a CLI scraper), it falls back to
    a one-shot thread.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Callable, Dict, Optional

import httpx

from ..config import settings

log = logging.getLogger("findata.ingest.lumilake")


def is_enabled() -> bool:
    return bool(settings.lumilake_base_url)


async def _post_job(payload: Dict[str, Any]) -> None:
    """Fire one POST /api/v1/jobs — log on failure but don't raise."""
    url = settings.lumilake_base_url.rstrip("/") + "/api/v1/jobs"
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if settings.lumilake_token:
        headers["Authorization"] = f"Bearer {settings.lumilake_token}"
    try:
        async with httpx.AsyncClient(timeout=settings.lumilake_timeout_s) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if 200 <= resp.status_code < 300:
                log.debug("lumilake handoff ok (%d)", resp.status_code)
            else:
                log.warning("lumilake handoff %d: %s",
                            resp.status_code, resp.text[:200])
    except Exception as e:
        log.warning("lumilake handoff failed: %s", e)


def _fire_and_forget(payload: Dict[str, Any]) -> None:
    """Schedule _post_job on the running loop, else fall back to a thread."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        asyncio.ensure_future(_post_job(payload), loop=loop)
        return
    # Sync caller (CLI scraper, etc.) — run in a one-shot thread.
    def _runner():
        try:
            asyncio.run(_post_job(payload))
        except Exception as e:  # pragma: no cover
            log.warning("lumilake handoff thread failed: %s", e)
    threading.Thread(target=_runner, daemon=True, name="lumilake-handoff").start()


def submit_after_ingest(result, info: Dict[str, Any]) -> None:
    """on_finalize callback. Signature matches core.ingest_records contract.

    Skips entirely when:
      - Lumilake is disabled (`LUMILAKE_BASE_URL` unset)
      - The run wrote 0 rows (no point notifying for empty writes)
    """
    if not is_enabled():
        return
    inserted = getattr(result, "inserted", 0) or 0
    updated = getattr(result, "updated", 0) or 0
    if (inserted + updated) == 0:
        return

    payload: Dict[str, Any] = {
        "data": [
            {
                "workflow": settings.lumilake_workflow,
                "inputs": {
                    "run_id":          [getattr(result, "run_id", "") or ""],
                    "target_schema":   [info.get("target_schema") or ""],
                    "target_table":    [info.get("target_table") or ""],
                    "rows_inserted":   [str(inserted)],
                    "rows_updated":    [str(updated)],
                    "mode":            [info.get("mode") or "typed"],
                    "declared_endpoint": [info.get("declared_endpoint") or ""],
                    "submitted_by":    [info.get("submitted_by") or ""],
                },
            }
        ]
    }
    _fire_and_forget(payload)
