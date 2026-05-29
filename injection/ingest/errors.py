"""Exception taxonomy for the ingress service.

Mapped to HTTP status codes by the route handlers:
  IngestError                  -> 400 (generic bad input)
  ValidationError              -> 422 (Pydantic validation failed)
  ACLError                     -> 403 (role not allowed to write target)
  SchemaIntrospectionError     -> 500 (target table missing / unintrospectable)
"""
from __future__ import annotations


class IngestError(Exception):
    """Generic ingress failure. Maps to HTTP 400."""
    http_status: int = 400


class ValidationError(IngestError):
    """Pydantic validation of one or more records failed."""
    http_status = 422

    def __init__(self, message: str, *, rejected: list | None = None):
        super().__init__(message)
        self.rejected = rejected or []


class ACLError(IngestError):
    """Authenticated identity is not allowed to write to the target table."""
    http_status = 403


class SchemaIntrospectionError(IngestError):
    """Target table doesn't exist or its schema can't be introspected."""
    http_status = 500
