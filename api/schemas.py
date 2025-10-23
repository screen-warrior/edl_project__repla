from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from db.models import ArtifactStatus, ArtifactType, RunState


class APIKeySettings(BaseModel):
    header_name: str = Field(
        "X-API-Key",
        description="HTTP header that must carry the shared API key.",
    )
    env_var: str = Field(
        "EDL_API_KEY",
        description="Environment variable holding the shared API key value.",
    )


class ProfileCreateRequest(BaseModel):
    name: str = Field(..., description="Human-friendly profile name.")
    description: Optional[str] = Field(
        None, description="Optional description for the profile."
    )


class ProfileResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    latest_config_version: Optional[int] = None


class ProfileSummary(ProfileResponse):
    total_runs: int = Field(..., description="How many runs have been recorded for this profile.")
    active_runs: int = Field(..., description="How many runs are currently queued or running.")


class ProfileConfigCreateRequest(BaseModel):
    sources_yaml: str = Field(..., description="YAML content describing sources.")
    augment_yaml: Optional[str] = Field(
        None, description="Optional YAML content describing augmentation settings."
    )
    pipeline_settings: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional pipeline settings (mode, timeout, log-level, etc.).",
    )
    created_by: Optional[str] = Field(
        None, description="Optional identifier for the engineer creating this configuration."
    )


class ProfileConfigResponse(BaseModel):
    id: str
    profile_id: str
    version: int
    config_hash: str
    created_at: datetime
    created_by: Optional[str]
    pipeline_settings: Dict[str, Any]


class RunCreateRequest(BaseModel):
    profile_id: str = Field(..., description="Profile to execute.")
    profile_config_id: Optional[str] = Field(
        None,
        description="Specific configuration version. If omitted, the latest version is used.",
    )
    overrides: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional overrides (mode, timeout, persist_to_db, etc.).",
    )


class RunSubmissionResponse(BaseModel):
    job_id: str
    run_id: str
    profile_id: Optional[str]
    profile_config_id: Optional[str]
    state: RunState
    detail: str


class ArtifactResponse(BaseModel):
    id: str
    run_id: str
    artifact_type: ArtifactType
    location: str
    status: ArtifactStatus
    checksum: Optional[str]
    size_bytes: Optional[int]
    created_at: datetime
    updated_at: datetime


class RunErrorResponse(BaseModel):
    id: str
    phase: Optional[str]
    source: Optional[str]
    message: str
    detail: Optional[Dict[str, Any]]
    created_at: datetime


class PipelineRunSummary(BaseModel):
    run_id: str = Field(..., description="Database primary key for the pipeline run.")
    profile_id: Optional[str]
    profile_config_id: Optional[str]
    mode: str
    state: RunState
    sub_state: Optional[str]
    percent_complete: float
    queued_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    total_fetched: int
    total_ingested: int
    total_valid: int
    total_invalid: int
    total_augmented: int


class PipelineRunDetail(PipelineRunSummary):
    metadata_snapshot: Dict[str, Any]
    artifacts: List[ArtifactResponse] = Field(default_factory=list)
    errors: List[RunErrorResponse] = Field(default_factory=list)


class RunListResponse(BaseModel):
    runs: List[PipelineRunSummary]


class JobStatusResponse(BaseModel):
    job_id: str
    run_id: Optional[str]
    profile_id: Optional[str]
    profile_config_id: Optional[str]
    state: RunState
    created_at: datetime
    updated_at: datetime
    detail: Optional[str] = None


class ProfileConfigSummary(BaseModel):
    id: str
    profile_id: str
    version: int
    created_at: datetime
    created_by: Optional[str]
    pipeline_settings: Dict[str, Any]

