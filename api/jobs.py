from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Optional

from db.models import RunState

from .schemas import JobStatusResponse, PipelineRunSummary


@dataclass
class JobRecord:
    """Internal representation of an asynchronous pipeline job."""

    id: str
    request: Dict[str, Any]
    profile_id: Optional[str] = None
    profile_config_id: Optional[str] = None
    run_id: Optional[str] = None
    state: RunState = RunState.QUEUED
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    detail: Optional[str] = None
    error: Optional[str] = None

    def to_response(
        self,
        *,
        pipeline_run: Optional[PipelineRunSummary] = None,
    ) -> JobStatusResponse:
        return JobStatusResponse(
            job_id=self.id,
            run_id=self.run_id,
            profile_id=self.profile_id,
            profile_config_id=self.profile_config_id,
            state=self.state,
            created_at=self.created_at,
            updated_at=self.updated_at,
            detail=self.detail,
        )


class JobStore:
    """Thread-safe in-memory registry for submitted jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, JobRecord] = {}
        self._lock = Lock()

    def create_job(
        self,
        *,
        job_id: str,
        request: Dict[str, Any],
        profile_id: Optional[str] = None,
        profile_config_id: Optional[str] = None,
    ) -> JobRecord:
        record = JobRecord(
            id=job_id,
            request=request,
            profile_id=profile_id or request.get("profile_id"),
            profile_config_id=profile_config_id or request.get("profile_config_id"),
        )
        with self._lock:
            self._jobs[job_id] = record
        return record

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"Unknown job_id '{job_id}'")
            record = self._jobs[job_id]
            for key, value in changes.items():
                setattr(record, key, value)
            if "request" in changes and isinstance(record.request, dict):
                record.profile_id = record.request.get("profile_id", record.profile_id)
                record.profile_config_id = record.request.get("profile_config_id", record.profile_config_id)
            record.updated_at = datetime.utcnow()
            return record

    def has_active_job_for_profile(self, profile_id: str) -> bool:
        with self._lock:
            return any(
                record.profile_id == profile_id and record.state in {RunState.QUEUED, RunState.RUNNING}
                for record in self._jobs.values()
            )

    def all(self) -> Dict[str, JobRecord]:
        with self._lock:
            return dict(self._jobs)
