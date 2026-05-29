"""findata ingest package.

One sync Python core library, two entry surfaces:
  - HTTP routes under api/routes/ingest_*.py call core.ingest_records()
    inside asyncio.to_thread, holding a sync psycopg2 conn from this
    package's ThreadedConnectionPool.
  - phase4 scrapers (post-migration) call core.ingest_records() in-process
    with their own caller-owned conn.

Module map:
  - core.ingest_records()        the single write function
  - pool                         sync psycopg2 ThreadedConnectionPool
  - acl.check_can_write()        role-based authorization
  - validation.model_for()       per-table Pydantic v2 model from information_schema
  - adapter_registry             sys.path bridge into loaders/adapters
  - errors                       IngressError taxonomy
"""
from __future__ import annotations

from .core import IngestResult, ingest_records
from .errors import (
    ACLError,
    IngestError,
    SchemaIntrospectionError,
    ValidationError,
)

__all__ = [
    "IngestResult",
    "ingest_records",
    "IngestError",
    "ACLError",
    "ValidationError",
    "SchemaIntrospectionError",
]
