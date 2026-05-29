"""Per-target-table Pydantic model factory.

Walks `information_schema.columns` for the target table and builds a
Pydantic v2 BaseModel:
  - required = NOT NULL columns that have no default AND are not provenance
  - optional = nullable cols + cols with defaults
  - types    = pg type -> python type (numeric->Decimal, date->date,
               timestamptz->datetime, jsonb->dict|list, bool->bool, …)
  - extra='forbid' — unknown keys are rejected with 422

The provenance columns (source, source_endpoint, source_run_id, ingest_ts,
raw) are NOT exposed in the model — the server stamps those itself from
the authenticated identity. A request that includes them gets a 422 (extra
keys forbidden).

Cached via @lru_cache(256). Invalidate after DDL via `refresh()` (wired to
a super_admin /admin/ingress/refresh-schemas route in v2).
"""
from __future__ import annotations

import decimal
import logging
from datetime import date, datetime, time
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict, Field, create_model

from .errors import SchemaIntrospectionError
from .pool import connection

log = logging.getLogger("findata.ingest.validation")

# Columns the server fills in itself — partners must NOT supply these
# (we don't trust source-side claims of provenance, and `id` is a serial PK).
SERVER_STAMPED_COLS = frozenset({
    "source", "source_endpoint", "source_run_id", "ingest_ts", "id"
})

# Back-compat alias retained for callers that imported PROVENANCE_COLS;
# semantically: "columns NOT exposed in the Pydantic input model".
# `raw` was previously in this set; it's now allowed through (as Optional)
# so adapters/partners can persist the original upstream payload.
PROVENANCE_COLS = SERVER_STAMPED_COLS


# Map of pg type name (from information_schema.columns.data_type) -> python type.
# Anything unmapped falls back to str (caller can still send numbers,
# Pydantic will coerce text to str).
_PG_TO_PY: Dict[str, Type[Any]] = {
    "text": str,
    "character varying": str,
    "varchar": str,
    "character": str,
    "char": str,
    "uuid": str,
    "boolean": bool,
    "bigint": int,
    "integer": int,
    "smallint": int,
    "numeric": decimal.Decimal,
    "real": float,
    "double precision": float,
    "date": date,
    "time without time zone": time,
    "time with time zone": time,
    "timestamp without time zone": datetime,
    "timestamp with time zone": datetime,
    "jsonb": dict,  # also accepts list — Pydantic Union below
    "json": dict,
    "bytea": bytes,
}


def _python_type_for(pg_type: str, udt_name: str) -> Any:
    """Resolve a pg type string to a Python annotation."""
    py = _PG_TO_PY.get(pg_type)
    if py is None:
        # Arrays: data_type='ARRAY', udt_name='_text' etc.
        if pg_type == "ARRAY":
            return List[Any]
        return str
    if py is dict:
        # jsonb may be either an object or an array.
        return Any
    return py


def _introspect(schema: str, table: str) -> Tuple[List[Tuple[str, str, str, bool, Optional[str]]], List[str]]:
    """Return (columns_info, natural_key_cols).

    columns_info elements: (name, data_type, udt_name, is_nullable, column_default)
    """
    cols: List[Tuple[str, str, str, bool, Optional[str]]] = []
    key: List[str] = []
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, udt_name, is_nullable, column_default,
                       is_generated, identity_generation
                  FROM information_schema.columns
                 WHERE table_schema = %s AND table_name = %s
                 ORDER BY ordinal_position
                """,
                (schema, table),
            )
            for name, dtype, udt, nullable, default, gen, ident in cur.fetchall():
                if gen == "ALWAYS" or ident == "ALWAYS":
                    continue
                cols.append((name, dtype, udt, nullable == "YES", default))
        if not cols:
            raise SchemaIntrospectionError(f"unknown table: {schema}.{table}")
        # Natural-key cols — pulled from loaders.lib.get_unique_columns so we
        # match the same heuristic the merge uses.
        from ..writeengine import engine as loaders_lib
        try:
            key = loaders_lib.get_unique_columns(conn, schema, table) or []
        except Exception as e:
            log.warning("get_unique_columns(%s,%s) failed: %s", schema, table, e)
            key = []
    return cols, key


@lru_cache(maxsize=256)
def model_for(schema: str, table: str) -> Type[BaseModel]:
    """Build (or fetch cached) Pydantic model for `<schema>.<table>` ingress."""
    cols, _key = _introspect(schema, table)

    fields: Dict[str, Tuple[Any, Any]] = {}
    for name, dtype, _udt, nullable, default in cols:
        if name in SERVER_STAMPED_COLS:
            continue
        py_type = _python_type_for(dtype, _udt)
        required = (not nullable) and (default is None)
        # `raw` is always Optional regardless of the column's NOT NULL flag —
        # partners can supply it, but it's never required.
        if name == "raw":
            fields[name] = (Optional[Any], Field(default=None))
            continue
        if required:
            fields[name] = (py_type, Field(...))
        else:
            fields[name] = (Optional[py_type], Field(default=None))

    if not fields:
        raise SchemaIntrospectionError(
            f"{schema}.{table} has no writable columns "
            f"(all provenance / generated / identity)"
        )

    model_name = f"Ingest_{schema}_{table}"
    Model = create_model(
        model_name,
        __config__=ConfigDict(extra="forbid", arbitrary_types_allowed=True),
        **fields,
    )
    log.debug("built model %s with %d fields", model_name, len(fields))
    return Model


@lru_cache(maxsize=256)
def natural_key_for(schema: str, table: str) -> List[str]:
    _, key = _introspect(schema, table)
    return key


def schema_json_for(schema: str, table: str) -> Dict[str, Any]:
    """Return the JSON Schema for the target table's input model.
    Used by GET /catalog/tables/{s}/{t}/schema.json."""
    return model_for(schema, table).model_json_schema()


def refresh() -> None:
    """Invalidate all cached models (call after DDL changes)."""
    model_for.cache_clear()
    natural_key_for.cache_clear()
    log.info("validation cache cleared")
