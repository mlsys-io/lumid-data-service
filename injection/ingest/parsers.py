"""Wire-format parsers.

Each parser takes a raw bytes payload (or an async byte iterator for the
streaming shapes) and yields dict records suitable for core.ingest_records.

Dispatch by Content-Type (preferred) or filename suffix (multipart upload).
Compression (gzip / zstd) is decoded upstream by api/ingest/decompress.py,
not at the parser layer.

Heavy/optional dependencies (pyarrow, lxml, pyyaml) are imported lazily so
the typed/JSON path never pays import cost.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from .errors import IngestError

log = logging.getLogger("findata.ingest.parsers")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

# Map of normalised content type (lowercased, no parameters) -> parser key.
_CT_ALIAS = {
    "application/json":                    "json",
    "application/x-ndjson":                "ndjson",
    "application/jsonl":                   "ndjson",
    "application/x-jsonlines":             "ndjson",
    "text/csv":                            "csv",
    "text/comma-separated-values":         "csv",
    "application/csv":                     "csv",
    "text/tab-separated-values":           "tsv",
    "text/tsv":                            "tsv",
    "application/xml":                     "xml",
    "text/xml":                            "xml",
    "application/yaml":                    "yaml",
    "application/x-yaml":                  "yaml",
    "text/yaml":                           "yaml",
    "text/x-yaml":                         "yaml",
    "application/vnd.apache.parquet":      "parquet",
    "application/x-parquet":               "parquet",
    "application/vnd.apache.arrow.stream": "arrow",
    "application/vnd.apache.arrow.file":   "arrow",
    "application/octet-stream":            "blob",
    "text/plain":                          "text_blob",
    # PDFs / images / html / audio / video are all routed through the blob plane.
    "application/pdf":                     "blob",
}


_SUFFIX_ALIAS = {
    ".json":    "json",
    ".ndjson":  "ndjson",
    ".jsonl":   "ndjson",
    ".csv":     "csv",
    ".tsv":     "tsv",
    ".xml":     "xml",
    ".yaml":    "yaml",
    ".yml":     "yaml",
    ".parquet": "parquet",
    ".pq":      "parquet",
    ".arrow":   "arrow",
    ".pdf":     "blob",
    ".txt":     "text_blob",
    ".md":      "text_blob",
}


def kind_for(content_type: Optional[str], filename: Optional[str] = None) -> str:
    """Resolve a wire-format kind from a Content-Type header or filename.
    Returns 'json' / 'ndjson' / 'csv' / 'tsv' / 'xml' / 'yaml' / 'parquet' /
    'arrow' / 'blob' / 'text_blob'.

    Raises IngestError if neither can be resolved.
    """
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in _CT_ALIAS:
        return _CT_ALIAS[ct]
    # Image / audio / video — always blob plane.
    if ct.startswith(("image/", "audio/", "video/")):
        return "blob"
    if ct == "text/html":
        return "blob"
    # Fall back to filename suffix (multipart uploads often use generic CT).
    if filename:
        _, ext = os.path.splitext(filename.lower())
        if ext in _SUFFIX_ALIAS:
            return _SUFFIX_ALIAS[ext]
    raise IngestError(
        f"unsupported wire format (content_type={content_type!r}, filename={filename!r})"
    )


# ---------------------------------------------------------------------------
# Structured-plane parsers — all yield iterators of dict
# ---------------------------------------------------------------------------

def parse_json(body: bytes) -> List[Dict[str, Any]]:
    """Single envelope: either {records:[...]} or a top-level list."""
    try:
        doc = json.loads(body or b"{}")
    except json.JSONDecodeError as e:
        raise IngestError(f"invalid JSON: {e}") from e
    if isinstance(doc, list):
        return list(doc)
    if isinstance(doc, dict):
        recs = doc.get("records")
        if recs is None:
            # Bare object → single record.
            return [doc]
        if not isinstance(recs, list):
            raise IngestError("'records' must be a list")
        return list(recs)
    raise IngestError("top-level JSON must be an object or array")


def iter_ndjson(body_iter: Iterable[bytes]) -> Iterator[Dict[str, Any]]:
    """Stream-decode line-delimited JSON. Tolerates blank lines and trailing newlines.
    `body_iter` yields arbitrary-sized chunks; we re-split on b'\\n'."""
    buf = b""
    line_no = 0
    for chunk in body_iter:
        if not chunk:
            continue
        buf += chunk
        while True:
            i = buf.find(b"\n")
            if i < 0:
                break
            line = buf[:i]
            buf = buf[i + 1:]
            line_no += 1
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except json.JSONDecodeError as e:
                raise IngestError(f"ndjson line {line_no}: {e}") from e
    s = buf.strip()
    if s:
        line_no += 1
        try:
            yield json.loads(s)
        except json.JSONDecodeError as e:
            raise IngestError(f"ndjson line {line_no}: {e}") from e


async def aiter_ndjson(body_aiter: AsyncIterator[bytes]) -> AsyncIterator[Dict[str, Any]]:
    """Async variant of iter_ndjson for FastAPI request.stream()."""
    buf = b""
    line_no = 0
    async for chunk in body_aiter:
        if not chunk:
            continue
        buf += chunk
        while True:
            i = buf.find(b"\n")
            if i < 0:
                break
            line = buf[:i]
            buf = buf[i + 1:]
            line_no += 1
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except json.JSONDecodeError as e:
                raise IngestError(f"ndjson line {line_no}: {e}") from e
    s = buf.strip()
    if s:
        line_no += 1
        try:
            yield json.loads(s)
        except json.JSONDecodeError as e:
            raise IngestError(f"ndjson line {line_no}: {e}") from e


def parse_csv(body: bytes, *, delimiter: str = ",") -> List[Dict[str, Any]]:
    text = body.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows: List[Dict[str, Any]] = []
    for r in reader:
        # csv.DictReader sets unset fields to None already.
        # Empty strings → None for cleaner Pydantic coercion.
        rows.append({k: (v if v != "" else None) for k, v in r.items() if k is not None})
    return rows


def parse_tsv(body: bytes) -> List[Dict[str, Any]]:
    return parse_csv(body, delimiter="\t")


def parse_xml(body: bytes) -> List[Dict[str, Any]]:
    """Streaming XML parser via lxml.iterparse.

    Expected shape: either <records><record>...</record><record>...</record></records>
    or any root that contains repeating <record> child elements. Each <record>
    becomes one dict with child-tag → text value (nested children become
    nested dicts).
    """
    try:
        from lxml import etree  # type: ignore
    except Exception as e:  # pragma: no cover — lxml optional at install time
        raise IngestError(f"XML parsing requires lxml ({e})") from e

    def _elem_to_dict(el) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for child in el:
            tag = etree.QName(child).localname
            if list(child):
                # has nested children → recurse
                out[tag] = _elem_to_dict(child)
            else:
                out[tag] = child.text
        # Attributes folded in with '@' prefix to avoid colliding with child tags.
        for k, v in el.attrib.items():
            out[f"@{k}"] = v
        return out

    rows: List[Dict[str, Any]] = []
    try:
        ctx = etree.iterparse(io.BytesIO(body), events=("end",), tag="record")
        for _, el in ctx:
            rows.append(_elem_to_dict(el))
            el.clear()
    except etree.XMLSyntaxError as e:
        raise IngestError(f"invalid XML: {e}") from e
    return rows


def parse_yaml(body: bytes) -> List[Dict[str, Any]]:
    """YAML parser.

    Accepts either:
      - one document with top-level `records:` list
      - multi-document stream (--- separated) — each doc is one record
      - bare list at top level
    """
    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover — pyyaml optional at install time
        raise IngestError(f"YAML parsing requires pyyaml ({e})") from e

    docs = list(yaml.safe_load_all(body))
    if len(docs) > 1:
        # Multi-document: each doc is one record.
        return [d for d in docs if isinstance(d, dict)]
    if not docs:
        return []
    d = docs[0]
    if isinstance(d, list):
        return list(d)
    if isinstance(d, dict):
        recs = d.get("records")
        if recs is None:
            return [d]
        if not isinstance(recs, list):
            raise IngestError("'records' must be a list in YAML payload")
        return list(recs)
    raise IngestError("top-level YAML must be a mapping or sequence")


def iter_parquet(body: bytes) -> Iterator[Dict[str, Any]]:
    """Yield one dict per row from a Parquet payload. Streams by row group."""
    try:
        import pyarrow.parquet as pq  # type: ignore
    except Exception as e:  # pragma: no cover — pyarrow optional
        raise IngestError(f"Parquet parsing requires pyarrow ({e})") from e
    pf = pq.ParquetFile(io.BytesIO(body))
    for i in range(pf.num_row_groups):
        tbl = pf.read_row_group(i)
        for row in tbl.to_pylist():
            yield row


def iter_arrow_stream(body: bytes) -> Iterator[Dict[str, Any]]:
    """Yield one dict per row from an Arrow IPC stream payload."""
    try:
        import pyarrow as pa  # type: ignore
    except Exception as e:  # pragma: no cover — pyarrow optional
        raise IngestError(f"Arrow parsing requires pyarrow ({e})") from e
    with pa.ipc.open_stream(io.BytesIO(body)) as reader:
        for batch in reader:
            for row in batch.to_pylist():
                yield row


# ---------------------------------------------------------------------------
# Top-level entry point for non-streaming (typed/JSON/CSV/XML/YAML/Parquet/Arrow)
# ---------------------------------------------------------------------------
def parse_to_records(body: bytes, kind: str) -> List[Dict[str, Any]]:
    """Eager parse for routes that need the full record list (typed mode, file mode).
    Streaming kinds raise; use iter_ndjson / iter_parquet / iter_arrow_stream
    explicitly for those."""
    if kind == "json":
        return parse_json(body)
    if kind == "ndjson":
        return list(iter_ndjson([body]))
    if kind == "csv":
        return parse_csv(body)
    if kind == "tsv":
        return parse_tsv(body)
    if kind == "xml":
        return parse_xml(body)
    if kind == "yaml":
        return parse_yaml(body)
    if kind == "parquet":
        return list(iter_parquet(body))
    if kind == "arrow":
        return list(iter_arrow_stream(body))
    raise IngestError(f"kind {kind!r} is not a structured format")
