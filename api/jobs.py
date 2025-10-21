from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Optional

from .schemas import JobStatus, JobStatusResponse, PipelineRunSummary


@dataclass
class JobRecord:
    """Internal representation of an asynchronous pipeline job."""

    id: str
    request: Dict[str, Any]
    persist_to_db: bool
    profile_id: Optional[str] = None
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    run_id: Optional[str] = None
    error: Optional[str] = None
    detail: Optional[str] = None

    def to_response(
        self,
        *,
        pipeline_run: Optional[PipelineRunSummary] = None,
    ) -> JobStatusResponse:
        return JobStatusResponse(
            job_id=self.id,
            status=self.status,
            created_at=self.created_at,
            updated_at=self.updated_at,
            persist_to_db=self.persist_to_db,
            profile_id=self.profile_id,
            run_id=self.run_id,
            error=self.error,
            detail=self.detail,
            pipeline_run=pipeline_run,
        )


class JobStore:
    """Thread-safe in-memory registry for submitted jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, JobRecord] = {}
        self._lock = Lock()

    def create_job(self, *, job_id: str, request: Dict[str, Any], persist_to_db: bool, profile_id: Optional[str] = None) -> JobRecord:
        record = JobRecord(id=job_id, request=request, persist_to_db=persist_to_db, profile_id=profile_id or request.get("profile_id"))
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
            if "profile_id" not in changes and record.profile_id is None and isinstance(record.request, dict):
                record.profile_id = record.request.get("profile_id")
            record.updated_at = datetime.utcnow()
            return record

    def has_active_job_for_profile(self, profile_id: str) -> bool:
        with self._lock:
            return any(
                record.profile_id == profile_id and record.status in {JobStatus.QUEUED, JobStatus.RUNNING}
                for record in self._jobs.values()
            )

    def all(self) -> Dict[str, JobRecord]:
        with self._lock:
            return dict(self._jobs)
