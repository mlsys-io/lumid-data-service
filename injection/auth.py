"""Auth + rate limiting.

Token sources, checked in order:
  1. **Local env keys** (`FINDATA_API_KEYS=key1:label1,key2:label2`) -- ops /
     test bypass; never needs network.
  2. **Lumid PAT / JWT** -- introspected against `https://lum.id` and cached.

Anonymous callers (no `Authorization` / `X-API-Key` header) are not rejected;
they get a stricter rate-limit tier. Authed callers (either local key or
lumid identity) get the higher tier.

If a caller presents a lumid-shaped token but lumid is unreachable, the
request fails with **401** (fail-closed).
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
from fastapi import Header, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address

from .config import settings
from .lumid import Identity, lumid

log = logging.getLogger("findata.auth")


def _parse_local_keys() -> dict[str, str]:
    raw = os.environ.get("FINDATA_API_KEYS", "")
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" in chunk:
            key, label = chunk.split(":", 1)
        else:
            key, label = chunk, "unnamed"
        out[key.strip()] = label.strip()
    return out


_LOCAL_KEYS = _parse_local_keys()


def _extract_token(authorization: Optional[str], x_api_key: Optional[str]) -> Optional[str]:
    if x_api_key:
        return x_api_key.strip() or None
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token:
            return token.strip()
    return None


async def get_identity(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(default=None),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Optional[Identity]:
    """FastAPI dependency. Returns:
        - None         -> anonymous (allowed; rate-limited stricter)
        - Identity     -> authed (lumid or local key)
        - raises 401   -> presented an invalid/lumid-shaped token that failed

    The identity is stashed on `request.state.identity` so the rate-limit key
    function can look it up without re-running the dependency.
    """
    request.state.identity = None
    token = _extract_token(authorization, x_api_key)
    if token is None:
        return None

    # Local env-key bypass (never needs network). Useful for ops / e2e tests
    # / internal services where a Lumid PAT is overkill.
    label = _LOCAL_KEYS.get(token)
    if label is not None:
        ident = Identity(sub=f"local:{label}", role="local")
        request.state.identity = ident
        response.headers["X-Identity"] = f"sub={ident.sub},role={ident.role}"
        return ident

    # Lumid introspect (sole auth path).
    if settings.lumid_enabled:
        try:
            ident = await lumid.introspect(token)
        except httpx.HTTPError as e:
            log.warning("lumid unreachable while validating bearer: %s", e)
            raise HTTPException(status_code=503, detail="auth service unreachable")
        if ident is not None:
            request.state.identity = ident
            response.headers["X-Identity"] = f"sub={ident.sub},role={ident.role}"
            return ident

    # Token was presented but neither path accepted it.
    raise HTTPException(status_code=401, detail="invalid or unknown token")


async def require_identity(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(default=None),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Identity:
    """Like get_identity but rejects anonymous callers with 401.

    Use on routers that must not be free-tier (e.g. LLM serving): no PAT,
    no service. Authenticated callers (Lumid PAT or local key) pass through;
    anonymous callers get 401, invalid tokens still get 401 from get_identity.
    """
    ident = await get_identity(request, response, authorization, x_api_key)
    if ident is None:
        raise HTTPException(
            status_code=401,
            detail="authentication required — present a Lumid PAT as 'Authorization: Bearer <token>'",
        )
    return ident


def _rate_limit_key(request: Request) -> str:
    """slowapi key fn: authenticated identity if present, else client IP.
    Reads `request.state.identity` populated by get_identity (if it ran);
    otherwise falls back to scanning headers cheaply."""
    ident = getattr(request.state, "identity", None)
    if isinstance(ident, Identity):
        return f"id:{ident.sub}"
    # The dependency may not have run yet (slowapi runs at middleware layer).
    # Cheap header peek -- if the caller looks authed, give them an authed
    # bucket optimistically; the dependency will reject invalid tokens later.
    if request.headers.get("authorization") or request.headers.get("x-api-key"):
        return f"presented:{get_remote_address(request)}"
    return f"ip:{get_remote_address(request)}"


def _dynamic_limit(key: str) -> str:
    """slowapi limit provider: pick anon vs authed tier from the rate-limit key.
    Authed users (verified `id:` OR header-bearing `presented:`) get the higher
    tier; raw-IP callers get the stricter anon tier."""
    if key.startswith(("id:", "presented:")):
        return settings.rate_limit_authed
    return settings.rate_limit_anon


class _TieredLimiter(Limiter):
    """slowapi 0.1.9 doesn't bind the request to default-limit LimitGroups
    before iterating, so `key`-aware limit providers raise. We bind first."""
    def _check_request_limit(self, request, endpoint_func, in_middleware=True):
        for lg in self._default_limits:
            lg.with_request(request)
        return super()._check_request_limit(request, endpoint_func, in_middleware)


limiter = _TieredLimiter(key_func=_rate_limit_key, default_limits=[_dynamic_limit])
