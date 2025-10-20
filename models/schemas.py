"""
Shared Pydantic schemas used across the EDL pipeline.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EntryType(str, Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    IPV4_WITH_PORT = "ipv4_with_port"
    IPV6_WITH_PORT = "ipv6_with_port"
    CIDR = "cidr"
    IP_RANGE = "ip_range"
    FQDN = "fqdn"
    URL = "url"
    UNKNOWN = "unknown"


class FetchedEntry(BaseModel):
    source: str = Field(..., description="Name of the feed this entry came from")
    raw: str = Field(..., description="Raw line as fetched from the source")
    line_number: Optional[int] = Field(
        None, description="Line number inside the original feed when available"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata captured during fetch (e.g., source_type, url)",
    )


class IngestedEntry(BaseModel):
    source: str = Field(..., description="Name of the feed this entry came from")
    entry: str = Field(..., description="Sanitized entry string ready for validation")
    line_number: Optional[int] = Field(
        None, description="Line number within the feed post-ingestion"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional ingestion metadata (comments, annotations, etc.)",
    )


class ValidationErrorDetail(BaseModel):
    code: str = Field(..., description="Stable machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    hint: Optional[str] = Field(None, description="Optional remediation hint")


class ValidatedEntry(BaseModel):
    source: Optional[str] = Field(None, description="Source feed name or URL")
    original: str = Field(..., description="Original raw entry string")
    entry_type: EntryType = Field(
        default=EntryType.UNKNOWN, description="Detected type of entry"
    )
    error: Optional[ValidationErrorDetail] = Field(
        None, description="If invalid, structured error"
    )
    normalized: str = Field(
        ..., description="In validation-only mode, identical to original input"
    )
    meta: Dict[str, Any] = Field(
        default_factory=dict, description="Parsed metadata (host, port, scheme, etc.)"
    )

    @property
    def valid(self) -> bool:
        return self.error is None
