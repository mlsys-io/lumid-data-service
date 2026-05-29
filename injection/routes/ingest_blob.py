"""POST /ingest/blob — binary blob upload (v1).

Accepts ANY binary payload (image / PDF / HTML / opaque). Bytes are PUT
through to the sibling lumid.data /storage/v1 endpoint; metadata lands in
raw.blobs. Same-sha256 re-upload short-circuits the network round trip.

Two body shapes supported:
  - `application/octet-stream` (or image/*, application/pdf, …) — raw bytes
    in the request body. Optional headers carry metadata:
      X-Ingress-Filename: <suggested-name>
      X-Ingress-Source-Endpoint: <ingress:partner-foo/...>
      X-Ingress-Metadata: <JSON>
  - `multipart/form-data` — one `file` part + optional `metadata`
    (JSON string) + optional `source_endpoint` form fields.

Returns {blob_sha256, storage_url, content_type, size_bytes, already_existed}.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from ..auth import require_identity
from ..config import settings
from ..ingest.blob_core import ingest_blob
from ..ingest.errors import IngestError
from ..ingest.models import BlobResultModel
from ..ingest import storage
from ..lumid import Identity

log = logging.getLogger("findata.routes.ingest_blob")

router = APIRouter(prefix="", tags=["Ingress"])


def _parse_metadata_header(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else None
    except json.JSONDecodeError:
        raise IngestError(f"X-Ingress-Metadata is not valid JSON: {raw[:80]}")


@router.post(
    "/ingest/blob",
    summary="Upload a binary blob (image / PDF / opaque bytes)",
    description=(
        "Stores arbitrary bytes in the sibling object store and records a "
        "metadata row in `raw.blobs` with full provenance. Idempotent: "
        "same sha256 → same row, no second PUT. Use the returned "
        "`storage_url` in a follow-up typed-row write that names the "
        "column it belongs in.\n\n"
        "Two body shapes supported:\n"
        "1. Raw bytes — `Content-Type: image/* | application/pdf | text/* | application/octet-stream`. "
        "Optional headers: `X-Ingress-Filename`, `X-Ingress-Source-Endpoint`, "
        "`X-Ingress-Metadata` (JSON).\n"
        "2. Multipart — `multipart/form-data` with field `file`; optional fields "
        "`metadata` (JSON string), `source_endpoint`."
    ),
    response_model=BlobResultModel,
)
async def post_blob(
    request: Request,
    identity: Identity = Depends(require_identity),
):
    if not storage.is_configured():
        raise HTTPException(
            status_code=503,
            detail="blob storage is not configured on this deployment",
        )

    src = f"ingress:{identity.sub}"
    ua = request.headers.get("user-agent")
    metadata: Optional[Dict[str, Any]] = None
    suggested_name: Optional[str] = None
    declared_endpoint: Optional[str] = None
    content_type: Optional[str] = request.headers.get("content-type")
    body: bytes

    if content_type and content_type.lower().startswith("multipart/form-data"):
        form = await request.form()
        upload = form.get("file")
        if not isinstance(upload, UploadFile):
            raise HTTPException(status_code=400, detail="multipart body must include a 'file' part")
        body = await upload.read()
        content_type = upload.content_type or content_type
        suggested_name = upload.filename
        md_raw = form.get("metadata")
        if isinstance(md_raw, str):
            try:
                metadata = _parse_metadata_header(md_raw)
            except IngestError as e:
                raise HTTPException(status_code=400, detail=str(e))
        se = form.get("source_endpoint")
        if isinstance(se, str) and se:
            declared_endpoint = se
    else:
        body = await request.body()
        suggested_name = request.headers.get("x-ingress-filename")
        declared_endpoint = request.headers.get("x-ingress-source-endpoint")
        try:
            metadata = _parse_metadata_header(request.headers.get("x-ingress-metadata"))
        except IngestError as e:
            raise HTTPException(status_code=400, detail=str(e))

    if not body:
        raise HTTPException(status_code=400, detail="empty body — no bytes to store")
    if len(body) > settings.blob_max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"blob too large ({len(body)} > {settings.blob_max_bytes} bytes)",
        )

    try:
        result = await ingest_blob(
            body=body,
            content_type=content_type,
            suggested_name=suggested_name,
            metadata=metadata,
            source=src,
            source_endpoint=declared_endpoint or f"ingress:{identity.sub}",
            submitted_by=identity.sub,
            declared_endpoint=declared_endpoint,
            user_agent=ua,
        )
    except IngestError as e:
        raise HTTPException(status_code=getattr(e, "http_status", 400), detail=str(e))
    return result.to_dict()
