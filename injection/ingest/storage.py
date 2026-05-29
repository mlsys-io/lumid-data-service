"""Blob storage — local filesystem.

Bytes go to `${FINDATA_BLOB_ROOT}/<prefix>/sha256=<hex>` where <prefix>
carves the namespace by content type (images / pdf / html / text / blob).
The API itself serves them back at `/blobs/<prefix>/sha256=<hex>` via a
StaticFiles mount (Lumid-PAT-authenticated, same rate limit as everything
else).

No external storage service is involved — the previous lumid.data hop
was replaced by a single bind mount. The function signature is kept
stable so callers (`blob_core.ingest_blob`) don't change.
"""
from __future__ import annotations

import hashlib
import logging
import mimetypes
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from ..config import settings
from .errors import IngestError

log = logging.getLogger("findata.ingest.storage")


@dataclass
class BlobResult:
    blob_sha256: str
    storage_url: str
    content_type: str
    size_bytes: int
    already_existed: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "blob_sha256": self.blob_sha256,
            "storage_url": self.storage_url,
            "content_type": self.content_type,
            "size_bytes": self.size_bytes,
            "already_existed": self.already_existed,
        }


def _key_prefix_for(content_type: str) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return "images"
    if ct == "application/pdf":
        return "pdf"
    if ct == "text/html":
        return "html"
    if ct in ("text/plain", "text/markdown"):
        return "text"
    if ct.startswith("audio/"):
        return "audio"
    if ct.startswith("video/"):
        return "video"
    return "blob"


def _content_type_for(content_type: Optional[str],
                     suggested_name: Optional[str]) -> str:
    if content_type and content_type.strip():
        return content_type.strip().split(";")[0]
    if suggested_name:
        ct, _ = mimetypes.guess_type(suggested_name)
        if ct:
            return ct
    return "application/octet-stream"


def is_configured() -> bool:
    return bool(settings.blob_root)


def storage_enabled() -> None:
    if not is_configured():
        e = IngestError("blob storage not configured (set FINDATA_BLOB_ROOT)")
        e.http_status = 503
        raise e


def _blob_path(sha256_hex: str, prefix: str) -> str:
    return os.path.join(settings.blob_root, prefix, f"sha256={sha256_hex}")


def _public_url_for(key: str) -> str:
    """Build the externally-visible URL for a storage key like
    'images/sha256=...'. Default shape: <base>/blobs/<key>."""
    base = (settings.blob_public_base_url or "").rstrip("/")
    if base:
        return f"{base}/blobs/{key}"
    # Caller will need to substitute. We keep this stable + relative-style
    # so the response is portable.
    return f"/blobs/{key}"


async def put_object(
    *,
    body: bytes,
    content_type: Optional[str] = None,
    suggested_name: Optional[str] = None,
    key_override: Optional[str] = None,
) -> Tuple[BlobResult, bytes]:
    """Write `body` to the local blob root. sha256 PK = key.

    Idempotency: if the target path already exists, we trust the bytes
    (same sha256 → same content); otherwise we fsync the write.
    """
    storage_enabled()
    if not body:
        raise IngestError("empty body")
    size = len(body)
    if size > settings.blob_max_bytes:
        raise IngestError(
            f"blob too large ({size} bytes > FINDATA_BLOB_MAX_BYTES={settings.blob_max_bytes})"
        )
    sha = hashlib.sha256(body).hexdigest()
    ct = _content_type_for(content_type, suggested_name)
    prefix = _key_prefix_for(ct)
    key = key_override or f"{prefix}/sha256={sha}"
    target = _blob_path(sha, prefix)

    os.makedirs(os.path.dirname(target), exist_ok=True)
    if not os.path.exists(target):
        # Atomic write: tmp → rename.
        tmp = target + ".tmp"
        with open(tmp, "wb") as f:
            f.write(body)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)

    storage_url = _public_url_for(key)
    return (
        BlobResult(
            blob_sha256=sha,
            storage_url=storage_url,
            content_type=ct,
            size_bytes=size,
            already_existed=False,
        ),
        body,
    )
