"""Vendored write engine — the COPY-staging + DISTINCT-FROM merge.

This is the self-contained, portable copy of the write path that the
injection service depends on. It is a trimmed extraction of the upstream
`loaders/lib.py` keeping ONLY the functions the ingress write path calls:

    open_run, close_run,
    get_target_columns, get_unique_columns,
    copy_into_staging, merge_staging_into_target

plus the value-coercion + CSV-serialization helpers they use.

Deliberately DROPPED from the upstream module (not on the write path, and a
portability / secret hazard if carried along):
    * the hardcoded DB_CONFIG dict + plaintext password + connect()
      — the injection service owns its connection via ingest/pool.py;
    * the json/ijson file-streaming helpers (stream_dict / stream_list);
    * the scrape-cursor helpers (update_cursor / get_cursor / get_endpoint);
    * project-root autodetection (FINAI_ROOT / fr).

The merge behaviour (re-runs produce 0/0; non-key column changes UPDATE;
provenance columns always refresh) is load-bearing for idempotency — it is
reproduced here verbatim from the upstream merge.

`ingest.core` / `ingest.validation` / `ingest.sandbox` / `ingest.blob_core`
import this as `from ..writeengine import engine as loaders_lib`, so their
call sites (`loaders_lib.open_run(...)`) stay identical to the upstream
codebase. The `engine` alias at the bottom of this module is what makes that
work — there is no separate `loaders/` package in this repo.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras

log = logging.getLogger("findata.writeengine")

# COPY batch size — flush staging COPY after this many rows.
COPY_BATCH_SIZE = 50_000


# ---------------------------------------------------------------------------
# camelCase -> snake_case
# ---------------------------------------------------------------------------
_CAMEL_RE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_RE_2 = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake(s: str) -> str:
    """Convert camelCase / PascalCase / kebab-case to snake_case."""
    if not s:
        return s
    s = s.replace("-", "_").replace(" ", "_")
    s = _CAMEL_RE_1.sub(r"\1_\2", s)
    s = _CAMEL_RE_2.sub(r"\1_\2", s)
    return s.lower()


# ---------------------------------------------------------------------------
# coerce_value -- tolerant numeric/date/string parsing for COPY
# ---------------------------------------------------------------------------
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_BAD_FLOAT_TOKENS = {"", "n/a", "na", "null", "none", "nan", "-", "--", "inf", "-inf"}


def _parse_date(v) -> Optional[date]:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    m = _DATE_RE.match(s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def _parse_ts(v) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(v))
        except (OSError, ValueError, OverflowError):
            return None
    s = str(v).strip()
    if not s:
        return None
    # Common ISO variants
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_numeric(v) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, (int, float)):
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError):
            return None
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in _BAD_FLOAT_TOKENS:
        return None
    # strip $ and trailing %
    s2 = s.lstrip("$").rstrip("%")
    try:
        return Decimal(s2)
    except (InvalidOperation, ValueError):
        return None


def _parse_int(v) -> Optional[int]:
    d = _parse_numeric(v)
    if d is None:
        return None
    try:
        return int(d)
    except (InvalidOperation, OverflowError, ValueError):
        return None


def coerce_value(value, target_pg_type: str):
    """Return a Python value suitable for COPY into a column of *target_pg_type*.

    target_pg_type is the unqualified Postgres type name (e.g. 'numeric',
    'date', 'timestamptz', 'text', 'jsonb', 'int', 'bigint', 'boolean').
    Returns None if the value cannot be coerced (caller should log).
    """
    if value is None:
        return None
    t = target_pg_type.lower()
    if t.startswith("text") or t.startswith("varchar") or t == "character varying":
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return str(value)
    if t == "date":
        return _parse_date(value)
    if t.startswith("timestamp"):
        return _parse_ts(value)
    if t in ("numeric", "decimal", "double precision", "real", "float", "float8", "float4"):
        return _parse_numeric(value)
    if t in ("int", "integer", "int4", "smallint", "int2", "bigint", "int8"):
        return _parse_int(value)
    if t in ("boolean", "bool"):
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "t", "1", "yes", "y"):
            return True
        if s in ("false", "f", "0", "no", "n"):
            return False
        return None
    if t == "jsonb" or t == "json":
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        if isinstance(value, str):
            return value
        return json.dumps(value, default=str)
    if t.endswith("[]"):
        # text[] etc.
        if value is None:
            return None
        if isinstance(value, str):
            return [value]
        try:
            return list(value)
        except TypeError:
            return [value]
    # Fallback: stringify.
    return str(value)


# ---------------------------------------------------------------------------
# Run lifecycle (provenance.runs)
# ---------------------------------------------------------------------------
def open_run(conn, endpoint_id: str, args: Optional[dict] = None,
             credential_label: Optional[str] = None) -> uuid.UUID:
    """Insert a 'running' row into provenance.runs and return its UUID."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO provenance.runs (endpoint_id, credential_label, status, args)
            VALUES (%s, %s, 'running', %s::jsonb)
            RETURNING run_id
            """,
            (endpoint_id, credential_label, json.dumps(args or {}, default=str)),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def close_run(conn, run_id, status: str,
              rows_inserted: int = 0, rows_updated: int = 0, rows_failed: int = 0,
              error_text: Optional[str] = None,
              response_schema_hash: Optional[str] = None) -> None:
    """Stamp ended_at / status / counts on a run row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE provenance.runs
               SET ended_at = now(),
                   status = %s,
                   rows_inserted = %s,
                   rows_updated = %s,
                   rows_failed = %s,
                   error_text = %s,
                   response_schema_hash = COALESCE(%s, response_schema_hash)
             WHERE run_id = %s
            """,
            (status, rows_inserted, rows_updated, rows_failed,
             error_text, response_schema_hash, run_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Target table introspection
# ---------------------------------------------------------------------------
def get_target_columns(conn, schema: str, table: str) -> list:
    """Return list of (column_name, data_type, is_nullable) for the target table,
    excluding generated/identity columns (those can't be inserted into directly).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, is_generated, identity_generation
              FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
             ORDER BY ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall()
    out = []
    for col, dtype, nullable, gen, ident in rows:
        if gen == "ALWAYS":
            continue
        if ident == "ALWAYS":
            continue
        out.append((col, dtype, nullable == "YES"))
    return out


# ---------------------------------------------------------------------------
# CSV serialization for COPY ... FORMAT csv
# ---------------------------------------------------------------------------
_NULL_TOKEN = "__FINAI_NULL_b8e3__"


def _format_value_for_copy(v, dtype: str) -> str:
    """Return a CSV cell string for one value.

    NULL marker is _NULL_TOKEN (NUL-delimited) — won't collide with real strings,
    and survives csv.writer escaping unchanged. COPY is invoked with NULL '<token>'.
    """
    if v is None:
        return _NULL_TOKEN
    if isinstance(v, str):
        s_strip = v.strip()
        if s_strip == "":
            return _NULL_TOKEN
        low = dtype.lower() if dtype else ""
        if low not in ("text", "varchar", "char", "character varying", "tsvector", "jsonb", "json"):
            if s_strip.lower() in ("none", "null", "nan", "n/a"):
                return _NULL_TOKEN
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v, default=str)
    if isinstance(v, float) and (v != v):  # NaN
        return _NULL_TOKEN
    return str(v)


def _rows_to_csv(rows: Iterable[Sequence], dtypes: Sequence[str]) -> io.StringIO:
    """Serialize an iterable of rows into an in-memory CSV stream."""
    buf = io.StringIO()
    # No escapechar — rely on QUOTE_MINIMAL to double-quote fields containing
    # the quote/delimiter/newline; backslashes survive literally.
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL,
                        lineterminator="\n")
    for row in rows:
        out = [_format_value_for_copy(v, dt) for v, dt in zip(row, dtypes)]
        writer.writerow(out)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Staging + merge
# ---------------------------------------------------------------------------
def copy_into_staging(
    conn,
    columns: Sequence[str],
    rows_iter: Iterable[Sequence],
    schema: str,
    table: str,
    source: str,
    source_endpoint: str,
    source_run_id,
) -> Tuple[str, int]:
    """Create a temp table that mirrors *schema.table*, COPY rows into it.

    *columns* lists the per-row payload columns. The provenance columns
    (source, source_endpoint, source_run_id) are appended automatically.

    Returns (tmp_table_name, row_count).
    """
    target_cols = get_target_columns(conn, schema, table)
    if not target_cols:
        raise RuntimeError(f"No columns found for {schema}.{table}")

    col_types = {c: t for c, t, _ in target_cols}
    # Always-present provenance columns we'll set ourselves:
    prov_cols = ["source", "source_endpoint", "source_run_id"]
    # Optional default-getters
    has_ingest_ts = "ingest_ts" in col_types

    full_cols = list(columns) + prov_cols
    if has_ingest_ts:
        full_cols.append("ingest_ts")

    # Build the temp table LIKE-clone (no constraints, no defaults except DEFAULT now()).
    tmp_name = f"_stg_{table}_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cur:
        # CREATE TEMP TABLE LIKE picks up defaults (we want ingest_ts default),
        # but also picks up identity columns; INCLUDING DEFAULTS without
        # INCLUDING IDENTITY is fine.
        cur.execute(
            f"CREATE TEMP TABLE {tmp_name} (LIKE {schema}.{table} "
            f"INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        # Drop generated columns from temp (they'd fail on insert).
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
             WHERE table_schema=%s AND table_name=%s AND is_generated='ALWAYS'
            """,
            (schema, table),
        )
        for (gcol,) in cur.fetchall():
            cur.execute(f"ALTER TABLE {tmp_name} DROP COLUMN IF EXISTS {gcol}")

    # Materialize rows now; we COPY in batches.
    dtypes_for_copy = [col_types.get(c, "text") for c in full_cols]

    # Stream rows into in-memory CSV in batches.
    total = 0
    batch: list = []

    def _flush(b):
        nonlocal total
        if not b:
            return
        buf = _rows_to_csv(b, dtypes_for_copy)
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {tmp_name} ({','.join(full_cols)}) "
                f"FROM STDIN WITH (FORMAT csv, NULL '{_NULL_TOKEN}', QUOTE '\"')",
                buf,
            )
        total += len(b)

    for row in rows_iter:
        # Append the provenance triplet (and ingest_ts default skipped).
        row = list(row) + [source, source_endpoint, str(source_run_id)]
        if has_ingest_ts:
            row.append(datetime.utcnow())
        batch.append(row)
        if len(batch) >= COPY_BATCH_SIZE:
            _flush(batch)
            batch = []
    _flush(batch)
    return tmp_name, total


def merge_staging_into_target(
    conn,
    tmp_table: str,
    schema: str,
    target_table: str,
    conflict_cols: Sequence[str],
) -> Tuple[int, int]:
    """Merge *tmp_table* into *schema.target_table* with newest-wins upsert.

    DO UPDATE only fires WHERE any non-key/non-provenance column would actually
    change -- so re-running on the same file produces (inserted=0, updated=0).

    Returns (rows_inserted, rows_updated).
    """
    target_cols = get_target_columns(conn, schema, target_table)
    all_cols = [c for c, _, _ in target_cols]
    key_set = set(conflict_cols)
    # Columns we always re-stamp from EXCLUDED (provenance refresh):
    prov_cols = {"source", "source_endpoint", "source_run_id", "ingest_ts", "raw", "payload"}
    # Flat columns we'll compare for change-detection:
    flat_cols = [c for c in all_cols if c not in key_set and c not in prov_cols
                 and c not in ("id",)]
    # Build the SET clause:
    # - prov columns: always set
    # - flat columns: set from EXCLUDED
    update_targets = []
    distinct_clauses = []
    for c in flat_cols:
        update_targets.append(f"{c} = EXCLUDED.{c}")
        distinct_clauses.append(f"{schema}.{target_table}.{c} IS DISTINCT FROM EXCLUDED.{c}")
    # Provenance always re-stamped:
    for c in ("source_endpoint", "source_run_id", "ingest_ts", "raw"):
        if c in all_cols:
            update_targets.append(f"{c} = EXCLUDED.{c}")
    # If there are no flat columns to compare we still upsert with prov refresh
    # but WHERE distinct -- which means: only update if raw differs.
    if "raw" in all_cols and "raw" not in flat_cols:
        distinct_clauses.append(
            f"{schema}.{target_table}.raw IS DISTINCT FROM EXCLUDED.raw"
        )
    if "payload" in all_cols:
        update_targets.append("payload = EXCLUDED.payload")
        distinct_clauses.append(
            f"{schema}.{target_table}.payload IS DISTINCT FROM EXCLUDED.payload"
        )

    insert_cols = [c for c in all_cols if c not in ("id",)]
    insert_cols_str = ", ".join(insert_cols)
    select_cols_str = ", ".join(insert_cols)

    on_conflict = ", ".join(conflict_cols)
    set_clause = ", ".join(update_targets)
    where_clause = " OR ".join(distinct_clauses) if distinct_clauses else "FALSE"

    # Deduplicate within the batch: pick one row per natural key (latest wins
    # by tmp ctid order — arbitrary but deterministic). Without this, two rows
    # in tmp with the same conflict_cols would trip
    # "ON CONFLICT DO UPDATE command cannot affect row a second time".
    #
    # Filter only on the NOT NULL conflict cols — nullable keys (e.g. firm in
    # estimates.grades_historical) legitimately store NULL and would otherwise
    # be silently dropped here, despite PG's NULLS DISTINCT allowing multiple
    # NULLs in a UNIQUE constraint.
    nullable_map = {c: n for c, _, n in target_cols}
    not_null_keys = [c for c in conflict_cols if not nullable_map.get(c, True)]
    on_conflict_quoted = ", ".join(conflict_cols)
    where_keys = " AND ".join(f"{c} IS NOT NULL" for c in not_null_keys) or "TRUE"
    dedupe_select = (
        f"SELECT DISTINCT ON ({on_conflict_quoted}) {select_cols_str} "
        f"FROM {tmp_table} "
        f"WHERE {where_keys} "
        f"ORDER BY {on_conflict_quoted}, ctid DESC"
    )
    # xmax = 0 means a fresh insert; non-zero means an update fired.
    sql = f"""
        INSERT INTO {schema}.{target_table} ({insert_cols_str})
        {dedupe_select}
        ON CONFLICT ({on_conflict}) DO UPDATE
           SET {set_clause}
         WHERE {where_clause}
        RETURNING (xmax = 0) AS inserted
    """
    inserted = updated = 0
    with conn.cursor() as cur:
        cur.execute(sql)
        for (was_insert,) in cur.fetchall():
            if was_insert:
                inserted += 1
            else:
                updated += 1
    conn.commit()
    return inserted, updated


# ---------------------------------------------------------------------------
# Helper: get UNIQUE-constraint columns for a table (used to derive conflict_cols)
# ---------------------------------------------------------------------------
def get_unique_columns(conn, schema: str, table: str) -> list:
    """Return the columns of the table's primary unique constraint (excluding 'id').

    Picks the first UNIQUE constraint in the table that does NOT contain a
    serial/identity 'id' column. Used by adapters that don't hardcode keys.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name, tc.constraint_type, tc.constraint_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = %s AND tc.table_name = %s
               AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY')
             ORDER BY tc.constraint_type DESC, kcu.ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall()
    # Group by constraint_name; pick the first UNIQUE constraint that doesn't include 'id'
    groups: dict = {}
    for col, ctype, cname in rows:
        groups.setdefault((ctype, cname), []).append(col)
    for (ctype, cname), cols in groups.items():
        if ctype == "UNIQUE" and "id" not in cols:
            return cols
    # Fallback: first group
    if groups:
        return next(iter(groups.values()))
    return []


# ---------------------------------------------------------------------------
# Self-reference so consumers can `from ..writeengine import engine as loaders_lib`
# and keep call sites identical to the upstream `loaders.lib` API.
# ---------------------------------------------------------------------------
engine = sys.modules[__name__]
