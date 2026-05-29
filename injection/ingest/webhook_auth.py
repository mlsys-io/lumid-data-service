"""Webhook HMAC verification.

Webhook authentication is NOT a PAT — it's a pre-registered (webhook_id,
secret) pair with the secret only revealed at creation time. The caller:

  1. Computes hex(hmac_sha256(secret, body)).
  2. Sends it in `X-Webhook-Signature` (just the hex, no scheme prefix).
  3. POSTs to /webhook/{webhook_id} with the body.

We verify by recomputing the HMAC server-side with constant-time compare.

The webhook is bound at creation time to EITHER:
  - a typed target (target_schema + target_table), OR
  - an adapter (adapter_id).
The body shape is exactly the corresponding ingress route's body.
"""
from __future__ import annotations

import hmac
import logging
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Dict, Optional

from .errors import IngestError
from .pool import connection

log = logging.getLogger("findata.ingest.webhook_auth")

# Accept hex digests with or without an optional "sha256=" scheme prefix.
_SIG_RE = re.compile(r"^(?:sha256=)?([0-9a-fA-F]{64})$")


@dataclass
class WebhookRow:
    webhook_id: str
    owner_sub: str
    secret_plain: str
    target_schema: Optional[str]
    target_table: Optional[str]
    adapter_id: Optional[str]
    source_endpoint: Optional[str]
    label: Optional[str]
    active: bool

    @property
    def mode(self) -> str:
        return "adapter" if self.adapter_id else "typed"

    def as_safe_dict(self) -> Dict[str, Any]:
        """Public view — never includes secret_plain."""
        return {
            "webhook_id": str(self.webhook_id),
            "owner_sub": self.owner_sub,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "adapter_id": self.adapter_id,
            "source_endpoint": self.source_endpoint,
            "label": self.label,
            "active": self.active,
            "mode": self.mode,
        }


def fetch_webhook(webhook_id: str) -> Optional[WebhookRow]:
    """Load one webhook row by id. Returns None if missing or inactive."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT webhook_id::text, owner_sub, secret_plain,
                       target_schema, target_table, adapter_id,
                       source_endpoint, label, active
                  FROM provenance.webhooks
                 WHERE webhook_id::text = %s
                """,
                (str(webhook_id),),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return WebhookRow(
        webhook_id=row[0],
        owner_sub=row[1],
        secret_plain=row[2],
        target_schema=row[3],
        target_table=row[4],
        adapter_id=row[5],
        source_endpoint=row[6],
        label=row[7],
        active=row[8],
    )


def verify_signature(secret_plain: str, body: bytes, signature_header: str) -> bool:
    """Constant-time compare of HMAC-SHA256(secret, body) vs supplied signature."""
    if not signature_header:
        return False
    m = _SIG_RE.match(signature_header.strip())
    if not m:
        return False
    expected = hmac.new(
        secret_plain.encode("utf-8"), body, sha256
    ).hexdigest()
    return hmac.compare_digest(expected.lower(), m.group(1).lower())


def authenticate(
    webhook_id: str, body: bytes, signature_header: str
) -> WebhookRow:
    """Look up + verify in one call. Raises IngestError on any auth failure."""
    wh = fetch_webhook(webhook_id)
    if wh is None or not wh.active:
        e = IngestError(f"unknown webhook {webhook_id!r}")
        e.http_status = 404
        raise e
    if not verify_signature(wh.secret_plain, body, signature_header):
        e = IngestError("invalid webhook signature")
        e.http_status = 401
        raise e
    return wh


def stamp_used(webhook_id: str) -> None:
    """Increment use_count + bump last_used_at. Fire-and-forget."""
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE provenance.webhooks
                       SET use_count = use_count + 1,
                           last_used_at = now()
                     WHERE webhook_id::text = %s
                    """,
                    (str(webhook_id),),
                )
            conn.commit()
    except Exception as e:
        log.warning("stamp_used %s failed: %s", webhook_id, e)
