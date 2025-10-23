"""
SQLModel schema for the EDL pipeline.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import Column, Enum as SAEnum, JSON, Text, UniqueConstraint
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


class RunState(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ArtifactStatus(str, enum.Enum):
    REQUESTED = "REQUESTED"
    WRITING = "WRITING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class ArtifactType(str, enum.Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    CIDR = "cidr"
    FQDN = "fqdn"
    URL = "url"
    JSON = "json"


class Profile(SQLModel, table=True):
    __tablename__ = "profiles"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = Field(default=None)
    refresh_interval_minutes: Optional[float] = Field(default=None, index=True)
    last_refreshed_at: Optional[datetime] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class ProfileConfig(SQLModel, table=True):
    __tablename__ = "profile_configs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    profile_id: str = Field(foreign_key="profiles.id", nullable=False, index=True)
    version: int = Field(default=1, nullable=False)
    config_hash: str = Field(nullable=False, index=True)
    sources_yaml: str = Field(sa_column=Column(Text, nullable=False))
    augment_yaml: Optional[str] = Field(default=None, sa_column=Column(Text))
    pipeline_settings: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, default=dict),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    created_by: Optional[str] = Field(default=None)

    __table_args__ = (
        UniqueConstraint("profile_id", "version", name="uq_profile_config_version"),
    )


class PipelineRun(SQLModel, table=True):
    __tablename__ = "pipeline_runs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    profile_id: Optional[str] = Field(default=None, foreign_key="profiles.id", index=True)
    profile_config_id: Optional[str] = Field(default=None, foreign_key="profile_configs.id", index=True)
    mode: str = Field(index=True)
    queued_at: datetime = Field(default_factory=datetime.utcnow, nullable=False, index=True)
    started_at: Optional[datetime] = Field(default=None, index=True)
    completed_at: Optional[datetime] = Field(default=None)
    state: RunState = Field(
        default=RunState.QUEUED,
        sa_column=Column(SAEnum(RunState, name="run_state"), nullable=False, index=True),
    )
    sub_state: Optional[str] = Field(default=None)
    percent_complete: float = Field(default=0.0)
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
    refresh_interval_minutes: Optional[float] = Field(default=None, index=True)
    last_refreshed_at: Optional[datetime] = Field(default=None, index=True)
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


class HostedFeed(SQLModel, table=True):
    __tablename__ = "hosted_feeds"

    id: Optional[int] = Field(default=None, primary_key=True)
    indicator_type: IndicatorType = Field(
        sa_column=Column(SAEnum(IndicatorType, name="hosted_indicator_type"), nullable=False, unique=True),
    )
    run_id: str = Field(foreign_key="pipeline_runs.id", nullable=False, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class Artifact(SQLModel, table=True):
    __tablename__ = "artifacts"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    run_id: str = Field(foreign_key="pipeline_runs.id", nullable=False, index=True)
    artifact_type: ArtifactType = Field(
        sa_column=Column(SAEnum(ArtifactType, name="artifact_type"), nullable=False, index=True),
    )
    location: str = Field(nullable=False)
    status: ArtifactStatus = Field(
        default=ArtifactStatus.REQUESTED,
        sa_column=Column(SAEnum(ArtifactStatus, name="artifact_status"), nullable=False),
    )
    checksum: Optional[str] = Field(default=None)
    size_bytes: Optional[int] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class RunError(SQLModel, table=True):
    __tablename__ = "run_errors"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    run_id: str = Field(foreign_key="pipeline_runs.id", nullable=False, index=True)
    phase: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    message: str = Field(nullable=False)
    detail: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


__all__ = [
    "SQLModel",
    "IndicatorType",
    "RunState",
    "ArtifactStatus",
    "ArtifactType",
    "Profile",
    "ProfileConfig",
    "PipelineRun",
    "Feed",
    "Indicator",
    "AugmentedIndicator",
    "HostedFeed",
    "Artifact",
    "RunError",
]
