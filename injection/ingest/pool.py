"""Sync psycopg2 connection pool for the ingest write path.

The HTTP read path uses asyncpg. The write path can't — loaders/lib.py is
sync (psycopg2 .copy_expert + xmax-based RETURNING) and rewriting it in
asyncpg would put the load-bearing DISTINCT-FROM merge at risk. Instead we
keep a separate sync pool here, and HTTP routes hop into it via
asyncio.to_thread(...).

Pool is created lazily on first use and torn down in api/server.py's
lifespan. Connections are returned via a context manager that does
.rollback() on error to keep psycopg2's transaction state clean.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from ..config import settings

log = logging.getLogger("findata.ingest.pool")

_pool: Optional[ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _build_pool() -> ThreadedConnectionPool:
    return ThreadedConnectionPool(
        minconn=max(1, settings.pool_min),
        maxconn=settings.pool_max,
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        dbname=settings.db_name,
        application_name="findata-ingress",
    )


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is None:
            _pool = _build_pool()
            log.info("ingest psycopg2 pool ready (min=%d, max=%d)",
                     settings.pool_min, settings.pool_max)
    return _pool


def close_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.closeall()
            _pool = None
            log.info("ingest psycopg2 pool closed")


@contextmanager
def connection() -> Iterator[psycopg2.extensions.connection]:
    """Borrow a sync psycopg2 conn from the pool.

    On exception inside the with-block: rollback + return to pool (still
    usable). On clean exit: any open tx is left as-is (callers commit
    themselves via lib.copy_into_staging / merge_staging_into_target) and
    the conn is returned to the pool.
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        # Per-checkout statement_timeout. Pooled conns persist the GUC across
        # checkouts, so re-applying each time is idempotent and cheap. Commit
        # immediately so the SET isn't rolled back by a later caller rollback.
        if settings.ingest_statement_timeout_ms > 0:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s",
                            (settings.ingest_statement_timeout_ms,))
            conn.commit()
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception as rb_err:
            log.warning("rollback after error failed: %s", rb_err)
        raise
    finally:
        pool.putconn(conn)
