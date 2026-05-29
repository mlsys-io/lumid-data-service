"""Shared Pydantic response models for the ingress + catalog surfaces.

Importing one IngestResultModel everywhere keeps the wire shape stable
across typed / adapter / stream / file / webhook and gives OpenAPI
consumers (AI agents, Scalar UI, codegen) a single named schema instead
of five copies of the same anonymous dict.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class RejectedRecord(BaseModel):
    """One record the server couldn't ingest, with its source-side index."""
    index: int
    error: str
    record: Optional[Dict[str, Any]] = None


class IngestResultModel(BaseModel):
    """The single response shape returned by every ingest mode.

    `run_id` is the UUID of the `provenance.runs` row for this batch; use it
    against `GET /catalog/lineage/run/{run_id}` to retrieve the full trace.
    """
    run_id: str = Field(..., description="provenance.runs UUID for this batch (empty string only on pure-validation failure).")
    target_schema: str
    target_table: str
    received: int = Field(..., description="Records the server actually parsed from the request.")
    inserted: int
    updated: int
    failed: int
    rejected: List[RejectedRecord] = Field(default_factory=list,
        description="Per-record errors; capped at 50 entries for stream/file uploads.")
    status: Literal["ok", "partial", "failed"]


class BlobResultModel(BaseModel):
    run_id: str
    blob_sha256: str
    storage_url: str
    content_type: str
    size_bytes: int
    already_existed: bool
    status: Literal["ok"]


class TableSummary(BaseModel):
    schema_: str = Field(..., alias="schema")
    table: str
    rule_source: Literal["explicit", "wildcard"]
    schema_url: str
    notes: Optional[str] = None

    model_config = {"populate_by_name": True}


class IngressOverviewModel(BaseModel):
    """One-call discovery for AI agents.

    Returned by `GET /catalog/ingress`. Combines:
      - all wire-format kinds the server understands
      - every adapter id available for /ingest/adapter/{id}
      - the writable (schema, table) list for the *calling* identity
      - the current ACL row count per role
      - links to per-table JSON Schemas (for client-side validation)
    """
    role: str
    sub: str
    formats: List[str] = Field(..., description="Supported wire formats (json, ndjson, csv, tsv, xml, yaml, parquet, arrow, blob).")
    modes: List[str] = Field(..., description="Ingress modes (typed, adapter, stream, file, webhook, blob).")
    blob_plane_enabled: bool
    adapters_count: int
    writable_count: int
    writable_tables: List[Dict[str, Any]]
    example_payloads: Dict[str, Any]
