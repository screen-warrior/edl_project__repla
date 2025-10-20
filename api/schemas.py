from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class SourceConfig(BaseModel):
    """Inline representation of a single feed definition."""

    name: str = Field(..., description="Human-friendly name of the feed.")
    type: Optional[str] = Field(
        None, description="Optional feed type (e.g., file, http, s3)."
    )
    location: Optional[str] = Field(
        None, description="Location of the feed (URL, file path, etc.)."
    )
    description: Optional[str] = Field(None, description="Optional description.")
    enabled: Optional[bool] = Field(
        True,
        description="If present, controls whether the feed should be processed.",
    )

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        # Filter out None values to align with existing config loader expectations.
        return {k: v for k, v in data.items() if v is not None}


class RunRequest(BaseModel):
    """
    Request payload for launching a pipeline execution.

    Mirrors CLI arguments while supporting inline source and augmentor payloads.
    """

    sources_path: Optional[str] = Field(
        None, description="Filesystem path to sources.yaml."
    )
    sources: Optional[List[SourceConfig]] = Field(
        None,
        description="Inline list of sources. Required if sources_path is not provided.",
    )
    mode: Literal["validate", "augment"] = Field(
        "validate", description="Pipeline mode to execute."
    )
    augmentor_config_path: Optional[str] = Field(
        None, description="Filesystem path to augmentor configuration."
    )
    augmentor_config: Optional[Dict[str, Any]] = Field(
        None, description="Inline augmentor configuration dictionary."
    )
    output_path: str = Field(
        "test_output_data/validated_output.json",
        description="Destination path for the JSON output written by the pipeline.",
    )
    timeout: int = Field(
        15, ge=1, le=300, description="Timeout (seconds) applied to fetch operations."
    )
    log_level: str = Field(
        "INFO",
        description="Logging level propagated to pipeline components (INFO, DEBUG, ...).",
    )
    persist_to_db: bool = Field(
        False, description="If true, persist run results into the configured database."
    )

    @model_validator(mode="after")
    def validate_sources_and_mode(self) -> RunRequest:
        if not self.sources and not self.sources_path:
            raise ValueError("Either 'sources' or 'sources_path' must be provided.")
        if self.mode == "augment" and not (
            self.augmentor_config or self.augmentor_config_path
        ):
            raise ValueError(
                "Augmentor configuration must be provided (inline or path) when mode='augment'."
            )
        return self


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunSubmissionResponse(BaseModel):
    job_id: str = Field(..., description="Identifier assigned to the submitted job.")
    status: JobStatus = Field(..., description="Initial job status.")
    persist_to_db: bool = Field(
        ..., description="Whether the job was instructed to persist results."
    )
    detail: str = Field(..., description="Human readable acknowledgement message.")


class PipelineRunSummary(BaseModel):
    run_id: str = Field(..., description="Database primary key for the pipeline run.")
    mode: str = Field(..., description="Pipeline mode that was executed.")
    started_at: datetime = Field(..., description="Timestamp when the run started.")
    completed_at: Optional[datetime] = Field(
        None, description="Timestamp when the run finished."
    )
    total_fetched: int = Field(..., description="Total entries fetched.")
    total_ingested: int = Field(..., description="Total entries ingested.")
    total_valid: int = Field(..., description="Total entries marked valid.")
    total_invalid: int = Field(..., description="Total entries flagged invalid.")
    total_augmented: int = Field(..., description="Total entries augmented.")
    metadata_snapshot: Dict[str, Any] = Field(
        default_factory=dict, description="Snapshot of run metadata."
    )


class JobStatusResponse(BaseModel):
    job_id: str = Field(..., description="Identifier of the requested job.")
    status: JobStatus = Field(..., description="Current execution state.")
    created_at: datetime = Field(..., description="Submission timestamp.")
    updated_at: datetime = Field(..., description="Last update timestamp.")
    persist_to_db: bool = Field(
        ..., description="Whether the job persisted into the database."
    )
    run_id: Optional[str] = Field(
        None, description="Database run identifier (when persistence is enabled)."
    )
    error: Optional[str] = Field(None, description="Error message if the job failed.")
    detail: Optional[str] = Field(
        None, description="Additional detail about the job outcome."
    )
    pipeline_run: Optional[PipelineRunSummary] = Field(
        None, description="Summary of the persisted pipeline run if available."
    )
