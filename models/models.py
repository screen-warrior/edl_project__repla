from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum


class EntryType(str, Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    CIDR = "cidr"
    IP_RANGE = "ip_range"
    FQDN = "fqdn"
    URL = "url"
    UNKNOWN = "unknown"


class IngestedEntry(BaseModel):
    source: str = Field(..., description="Name of the feed this entry came from")
    entry: str = Field(..., description="Raw string entry fetched from the source")


class ValidatedEntry(BaseModel):
    source: Optional[str] = Field(None, description="Source feed name or URL")
    original: str = Field(..., description="Original raw entry string")
    entry_type: EntryType = Field(..., description="Detected type of entry")
    normalized: str = Field(..., description="Canonical form")
    error: Optional[str] = Field(None, description="Validation error if invalid")

    @property
    def valid(self) -> bool:
        """Entry is valid if no error is set."""
        return self.error is None


class AugmentedEntry(ValidatedEntry):
    augmented: str = Field(..., description="Final augmented value")
    changes: List[str] = Field(default_factory=list, description="List of applied augmentations")
