from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from db.models import ArtifactStatus, ArtifactType, RunState
from models.schemas import EntryType


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


class ProfileBootstrapResponse(ProfileResponse):
    api_key: str


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
    refresh_interval_minutes: Optional[int] = Field(
        None,
        ge=1,
        description="If provided, pipelines using this config auto-refresh every N minutes.",
    )


class ProfileConfigResponse(BaseModel):
    id: str
    profile_id: str
    version: int
    config_hash: str
    created_at: datetime
    created_by: Optional[str]
    pipeline_settings: Dict[str, Any]
    refresh_interval_minutes: Optional[int]


class RunCreateRequest(BaseModel):
    pipeline_id: str = Field(..., description="Pipeline to execute.")
    overrides: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional overrides (mode, timeout, persist_to_db, etc.).",
    )
    requested_by: Optional[str] = Field(
        None, description="Optional identifier for the caller triggering the run."
    )


class RunSubmissionResponse(BaseModel):
    job_id: str
    run_id: str
    pipeline_id: str
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
    pipeline_id: str
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
    pipeline_id: Optional[str]
    profile_id: Optional[str]
    profile_config_id: Optional[str]
    state: RunState
    created_at: datetime
    updated_at: datetime
    detail: Optional[str] = None
    cancel_requested: bool = False


class ProfileConfigSummary(BaseModel):
    id: str
    profile_id: str
    version: int
    created_at: datetime
    created_by: Optional[str]
    pipeline_settings: Dict[str, Any]


class ManualEntry(BaseModel):
    value: str = Field(..., description="Raw manual entry to inject.")
    type: Optional[EntryType] = Field(
        None, description="Optional declared type to assist validation."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional metadata (tags, analyst notes).",
    )


class ManualSubmissionRequest(BaseModel):
    pipeline_ids: List[str] = Field(..., description="Pipelines that should ingest the entries.")
    entries: List[ManualEntry] = Field(..., description="One or more manual EDL entries.")
    source: Optional[str] = Field(
        default="manual",
        description="Source label recorded with the entries.",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional analyst note for audit trails.",
    )


class ManualSubmissionResult(BaseModel):
    pipeline_id: str
    run_id: str
    job_id: str
    submission_id: str
    entry_count: int


class ManualSubmissionRejection(BaseModel):
    pipeline_id: str
    reason: str


class ManualSubmissionResponse(BaseModel):
    submissions: List[ManualSubmissionResult] = Field(default_factory=list)
    rejections: List[ManualSubmissionRejection] = Field(default_factory=list)
    skipped_entries: List[str] = Field(
        default_factory=list,
        description="Manual entries that were ignored prior to queuing (empty or duplicate).",
    )


class PipelineCreateRequest(BaseModel):
    profile_id: str
    profile_config_id: str
    name: str
    description: Optional[str] = None
    concurrency_limit: Optional[int] = Field(
        default=1, description="Maximum concurrent runs for this pipeline (1 = serial)."
    )
    created_by: Optional[str] = Field(
        None, description="Optional identifier for the engineer creating the pipeline."
    )


class PipelineResponse(BaseModel):
    id: str
    profile_id: str
    profile_config_id: str
    name: str
    description: Optional[str]
    concurrency_limit: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class PipelineListResponse(BaseModel):
    pipelines: List[PipelineResponse]


class ConfigUsageSummary(BaseModel):
    config: ProfileConfigResponse
    pipelines_total: int
    pipelines_active: int
    runs_active: int


class PipelineScheduleEntry(BaseModel):
    pipeline: PipelineResponse
    config: ProfileConfigResponse
    last_run_id: Optional[str]
    last_run_state: Optional[RunState]
    last_completed_at: Optional[datetime]
    next_run_at: Optional[datetime]


class PipelineScheduleResponse(BaseModel):
    profile_id: str
    pipelines: List[PipelineScheduleEntry]

