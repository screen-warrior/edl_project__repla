"""
SQLModel schema for persisting EDL pipeline results (relationship-light version).
"""

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import Column, Enum as SAEnum, Index, JSON, UniqueConstraint
from sqlmodel import Field, SQLModel


class IndicatorType(str, enum.Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    IPV4_WITH_PORT = "ipv4_with_port"
    IPV6_WITH_PORT = "ipv6_with_port"
    CIDR = "cidr"
    IP_RANGE = "ip_range"
    FQDN = "fqdn"
    URL = "url"
    UNKNOWN = "unknown"


class PipelineRun(SQLModel, table=True):
    __tablename__ = "pipeline_runs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    mode: str = Field(index=True)
    started_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, index=True)
    completed_at: Optional[datetime] = Field(default=None)
    total_fetched: int = Field(default=0, nullable=False)
    total_ingested: int = Field(default=0, nullable=False)
    total_valid: int = Field(default=0, nullable=False)
    total_invalid: int = Field(default=0, nullable=False)
    total_augmented: int = Field(default=0, nullable=False)
    metadata_snapshot: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, default=dict),
    )


class Feed(SQLModel, table=True):
    __tablename__ = "feeds"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True, nullable=False)
    source_url: Optional[str] = Field(default=None)
    source_type: Optional[str] = Field(default=None, index=True)
    description: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class Indicator(SQLModel, table=True):
    __tablename__ = "indicators"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="pipeline_runs.id", nullable=False, index=True)
    feed_id: Optional[int] = Field(default=None, foreign_key="feeds.id", index=True)
    source_name: str = Field(nullable=False, index=True)
    original: str = Field(nullable=False)
    normalized: str = Field(nullable=False)
    entry_type: IndicatorType = Field(
        sa_column=Column(SAEnum(IndicatorType, name="indicator_type"), nullable=False),
    )
    is_valid: bool = Field(default=True, nullable=False, index=True)
    error_code: Optional[str] = Field(default=None, index=True)
    error_message: Optional[str] = Field(default=None)
    error_hint: Optional[str] = Field(default=None)
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, default=dict),
    )
    ingested_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "source_name",
            "normalized",
            "entry_type",
            name="uq_indicator_run_normalized_type",
        ),
    )


class AugmentedIndicator(SQLModel, table=True):
    __tablename__ = "augmented_indicators"

    id: Optional[int] = Field(default=None, primary_key=True)
    indicator_id: int = Field(foreign_key="indicators.id", nullable=False, unique=True, index=True)
    augmented_value: str = Field(nullable=False)
    changes: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, default=list),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, index=True)


class Firewall(SQLModel, table=True):
    __tablename__ = "firewalls"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(nullable=False, unique=True, index=True)
    description: Optional[str] = Field(default=None)
    last_seen_at: Optional[datetime] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class FirewallConsumption(SQLModel, table=True):
    __tablename__ = "firewall_consumptions"

    id: Optional[int] = Field(default=None, primary_key=True)
    firewall_id: int = Field(foreign_key="firewalls.id", nullable=False, index=True)
    indicator_id: int = Field(foreign_key="indicators.id", nullable=False, index=True)
    feed_id: Optional[int] = Field(default=None, foreign_key="feeds.id", index=True)
    consumed_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, index=True)
    notes: Optional[str] = Field(default=None)

    __table_args__ = (
        UniqueConstraint(
            "firewall_id",
            "indicator_id",
            name="uq_firewall_indicator",
        ),
    )


__all__ = [
    "SQLModel",
    "IndicatorType",
    "PipelineRun",
    "Feed",
    "Indicator",
    "AugmentedIndicator",
    "Firewall",
    "FirewallConsumption",
]
