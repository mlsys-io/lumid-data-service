"""Blob-plane ingest core.

`ingest_blob` is the dual of `core.ingest_records` for the binary plane.
It:
  1. Computes sha256, checks `raw.blobs` — if present, returns the existing
     row (no re-PUT to the object store).
  2. Otherwise opens a provenance.runs row, PUTs the bytes to lumid.data,
     INSERTs the metadata into raw.blobs, closes the run.
  3. Returns a BlobIngestResult including the storage URL the caller can
     thread into a follow-up typed-row write.
"""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import psycopg2

from ..writeengine import engine as loaders_lib
from .errors import IngestError
from .pool import connection
from .storage import BlobResult, put_object

log = logging.getLogger("findata.ingest.blob_core")


@dataclass
class BlobIngestResult:
    run_id: str
    blob_sha256: str
    storage_url: str
    content_type: str
    size_bytes: int
    already_existed: bool
    status: str = "ok"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["run_id"] = str(self.run_id)
        return d


def _lookup_existing(conn: psycopg2.extensions.connection,
                     sha256_hex: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT blob_sha256, storage_url, content_type, size_bytes
              FROM raw.blobs WHERE blob_sha256 = %s
            """,
            (sha256_hex,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "blob_sha256": row[0],
        "storage_url": row[1],
        "content_type": row[2],
        "size_bytes": row[3],
    }


def _insert_blob_row(
    conn: psycopg2.extensions.connection,
    *,
    sha256_hex: str,
    storage_url: str,
    content_type: str,
    size_bytes: int,
    suggested_name: Optional[str],
    metadata: Optional[Dict[str, Any]],
    source: str,
    source_endpoint: str,
    source_run_id: uuid.UUID,
    submitted_by: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.blobs (
                blob_sha256, storage_url, content_type, size_bytes,
                suggested_name, metadata, source, source_endpoint,
                source_run_id, submitted_by
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (blob_sha256) DO NOTHING
            """,
            (
                sha256_hex, storage_url, content_type, size_bytes,
                suggested_name, json.dumps(metadata or {}, default=str),
                source, source_endpoint, str(source_run_id), submitted_by,
            ),
        )
    conn.commit()


async def ingest_blob(
    *,
    body: bytes,
    content_type: Optional[str],
    suggested_name: Optional[str],
    metadata: Optional[Dict[str, Any]],
    source: str,
    source_endpoint: str,
    submitted_by: str,
    declared_endpoint: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> BlobIngestResult:
    """End-to-end blob ingest. Async — does the lumid.data PUT in-flight."""
    # 1) Compute sha256 + check the dedup table (sync hop).
    import hashlib
    sha = hashlib.sha256(body).hexdigest()

    def _check_existing() -> Optional[Dict[str, Any]]:
        with connection() as conn:
            return _lookup_existing(conn, sha)

    existing = await asyncio.to_thread(_check_existing)
    if existing:
        log.debug("blob sha256=%s already in raw.blobs; short-circuiting", sha[:8])
        return BlobIngestResult(
            run_id="",
            blob_sha256=sha,
            storage_url=existing["storage_url"],
            content_type=existing["content_type"],
            size_bytes=existing["size_bytes"],
            already_existed=True,
            status="ok",
        )

    # 2) PUT to lumid.data.
    put_result, _ = await put_object(
        body=body, content_type=content_type, suggested_name=suggested_name,
    )

    # 3) Open a run row + insert raw.blobs in one sync transaction.
    def _persist() -> str:
        with connection() as conn:
            run_args = {
                "target_schema": "raw",
                "target_table": "blobs",
                "mode": "blob",
                "blob_sha256": sha,
                "content_type": put_result.content_type,
                "size_bytes": put_result.size_bytes,
            }
            if declared_endpoint:
                run_args["declared_endpoint"] = declared_endpoint
            if user_agent:
                run_args["user_agent"] = user_agent
            if submitted_by:
                run_args["submitted_by"] = submitted_by
            run_id = loaders_lib.open_run(
                conn, endpoint_id="ingress:generic", args=run_args,
            )
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE provenance.runs SET submitted_by = %s WHERE run_id = %s",
                    (submitted_by, run_id),
                )
            conn.commit()
            try:
                _insert_blob_row(
                    conn,
                    sha256_hex=sha,
                    storage_url=put_result.storage_url,
                    content_type=put_result.content_type,
                    size_bytes=put_result.size_bytes,
                    suggested_name=suggested_name,
                    metadata=metadata,
                    source=source,
                    source_endpoint=source_endpoint,
                    source_run_id=run_id,
                    submitted_by=submitted_by,
                )
                loaders_lib.close_run(
                    conn, run_id, "ok",
                    rows_inserted=1, rows_updated=0, rows_failed=0,
                )
            except Exception as e:
                err = traceback.format_exc()[-4000:]
                try:
                    loaders_lib.close_run(
                        conn, run_id, "failed",
                        rows_inserted=0, rows_updated=0, rows_failed=1,
                        error_text=err,
                    )
                except Exception as close_err:
                    log.warning("failed to close failed blob run %s: %s",
                                run_id, close_err)
                raise IngestError(f"failed to record blob: {e}") from e
            return str(run_id)

    run_id = await asyncio.to_thread(_persist)
    return BlobIngestResult(
        run_id=run_id,
        blob_sha256=sha,
        storage_url=put_result.storage_url,
        content_type=put_result.content_type,
        size_bytes=put_result.size_bytes,
        already_existed=False,
        status="ok",
    )
