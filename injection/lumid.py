"""Lumid identity bridge.

The findata API treats lumid (`https://lum.id`) as the sole token authority.
Any incoming bearer token (PAT-shaped `lm_pat_live_*` / `rm_pat_live_*` or
RS256 JWT) is validated via `POST /api/v1/identity/introspect`.

We cache introspection responses in-process for a short TTL (default 5 min)
so a chatty caller doesn't put load on lumid. The cache is keyed on a SHA-256
hash of the token rather than the raw token, so heap dumps don't expose
plaintext secrets.

Failure mode is **fail-closed**: if lumid is unreachable, an authed call
returns 401 rather than silently downgrading to anonymous. Anonymous calls
(no token) are unaffected and never touch lumid.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

import httpx
from cachetools import TTLCache

from .config import settings

log = logging.getLogger("findata.lumid")


@dataclass(frozen=True)
class Identity:
    """The subset of the lumid introspect response we use internally."""
    sub: str
    role: str
    email: Optional[str] = None
    active: bool = True


class LumidClient:
    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: TTLCache = TTLCache(
            maxsize=4096,
            ttl=settings.lumid_cache_ttl,
        )
        self._lock = asyncio.Lock()

    async def startup(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.lumid_url,
            timeout=httpx.Timeout(settings.lumid_timeout_s),
            headers={"User-Agent": "findata-api/0.1"},
        )

    async def shutdown(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def _looks_like_lumid_token(token: str) -> bool:
        """PAT prefix or JWT shape. Anything else is rejected without a
        network call so we don't leak random strings to lumid."""
        if not token:
            return False
        if token.startswith(("lm_pat_live_", "rm_pat_live_")):
            return True
        # RS256 JWT: three base64url segments separated by dots.
        parts = token.split(".")
        if len(parts) == 3 and all(p for p in parts):
            return True
        return False

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    async def introspect(self, token: str) -> Optional[Identity]:
        """Return the validated Identity or None if rejected.
        Raises httpx.HTTPError on transport failures (caller fails closed)."""
        if not settings.lumid_enabled or not self._looks_like_lumid_token(token):
            return None

        key = self._hash_token(token)
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                # cached value may be Identity or the literal False (negative cache)
                return cached if isinstance(cached, Identity) else None

        if self._client is None:
            log.warning("lumid client not started; rejecting token")
            return None

        try:
            r = await self._client.post(
                "/api/v1/identity/introspect",
                json={"token": token},
            )
        except httpx.HTTPError as e:
            log.warning("lumid introspect transport error: %s", e)
            raise

        if r.status_code != 200:
            log.info("lumid introspect status=%d body=%r", r.status_code, r.text[:200])
            async with self._lock:
                self._cache[key] = False
            return None

        try:
            body = r.json()
        except ValueError:
            log.warning("lumid introspect returned non-JSON: %r", r.text[:200])
            return None

        # lum.id wraps responses in {"data": {...}, "ret_code": 0}
        data = body.get("data", body)

        if not data.get("active"):
            async with self._lock:
                self._cache[key] = False
            return None

        identity = Identity(
            sub=str(data.get("sub", "")),
            role=str(data.get("role", "user")),
            email=data.get("email"),
            active=True,
        )
        async with self._lock:
            self._cache[key] = identity
        return identity


lumid = LumidClient()
