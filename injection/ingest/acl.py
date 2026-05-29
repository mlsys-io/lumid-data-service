"""Role-based ingress ACL.

ACL rows live in `provenance.ingress_acl` (DDL 20_ingress.sql). One row per
(role, target_schema, target_table). '*' wildcards in either schema or
table cell. super_admin + 'local' get blanket allow at seed time.

Lookup order for a write to (schema, table):
  1. exact (role, schema, table)
  2. (role, schema, '*')
  3. (role, '*', '*')
First row with can_write=true wins. No match = deny.

Cached for 30 s in-process to keep the check off the hot path.
Refresh via /admin/ingress/refresh-acl (super_admin only, wired in v2).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Iterable, Tuple

from .errors import ACLError
from .pool import connection

log = logging.getLogger("findata.ingest.acl")

_CACHE_TTL_S = 30.0

# (role, schema, table) -> can_write
_cache: Dict[Tuple[str, str, str], bool] = {}
_cache_loaded_at: float = 0.0
_cache_lock = threading.Lock()


def _load() -> Dict[Tuple[str, str, str], bool]:
    """Read every row of provenance.ingress_acl into the in-process dict.
    Small table (<100 rows expected); one SELECT is fine."""
    out: Dict[Tuple[str, str, str], bool] = {}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT role, target_schema, target_table, can_write "
                "FROM provenance.ingress_acl"
            )
            for role, sch, tbl, can in cur.fetchall():
                out[(role, sch, tbl)] = bool(can)
    return out


def _get_cache() -> Dict[Tuple[str, str, str], bool]:
    global _cache, _cache_loaded_at
    now = time.monotonic()
    if now - _cache_loaded_at < _CACHE_TTL_S and _cache:
        return _cache
    with _cache_lock:
        # Re-check under lock (another thread may have refreshed)
        if time.monotonic() - _cache_loaded_at >= _CACHE_TTL_S or not _cache:
            _cache = _load()
            _cache_loaded_at = time.monotonic()
            log.debug("acl cache reloaded — %d rows", len(_cache))
    return _cache


def invalidate() -> None:
    """Force the next check_can_write to re-read from Postgres."""
    global _cache_loaded_at
    with _cache_lock:
        _cache_loaded_at = 0.0


def _candidate_keys(role: str, schema: str, table: str) -> Iterable[Tuple[str, str, str]]:
    """Generate lookup keys in priority order."""
    # Exact match first.
    yield (role, schema, table)
    # Wildcard table within schema.
    yield (role, schema, "*")
    # Wildcard schema + table.
    yield (role, "*", "*")


def check_can_write(role: str, schema: str, table: str) -> None:
    """Raise ACLError if `role` cannot write to (schema, table). Otherwise return.

    Resolves wildcards as documented above. Caller is expected to have already
    resolved any provider-managed roles (e.g. mapping Lumid roles to local
    role strings). 'role' should be the literal value from Identity.role.
    """
    cache = _get_cache()
    for key in _candidate_keys(role, schema, table):
        if cache.get(key) is True:
            return
    raise ACLError(
        f"role {role!r} not authorized to write {schema}.{table}"
    )


def can_propose(role: str) -> bool:
    """Permissive gate for the sandbox / net-new-target path.

    A role can propose a new shape if it has ANY `can_write=true` row in
    `provenance.ingress_acl`. This admits known partners (who already
    have at least one allowlist entry for an existing table) without
    auto-granting unknown roles.

    Blanket-allow roles (super_admin, local) pass trivially via the
    standard wildcard rows. A role with no ACL rows at all returns False.
    """
    cache = _get_cache()
    for (r, _sch, _tbl), allow in cache.items():
        if r == role and allow:
            return True
    return False
