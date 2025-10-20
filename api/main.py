from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, status

from db.models import PipelineRun
from db.session import get_session
from pipeline.engine_1 import EngineConfig, run_pipeline
from utils.logger import get_logger

from .jobs import JobStore
from .schemas import (
    JobStatus,
    JobStatusResponse,
    PipelineRunSummary,
    RunRequest,
    RunSubmissionResponse,
)


app = FastAPI(
    title="EDL Pipeline API",
    version="1.0.0",
    description=(
        "Enterprise API surface for orchestrating the EDL ingestion/validation pipeline. "
        "Submit runs, monitor progress, and query persisted results."
    ),
)

job_store = JobStore()
api_logger = get_logger("api.app", "INFO", "api.log")


@app.get("/health", tags=["system"])
def healthcheck() -> Dict[str, str]:
    """
    Lightweight health probe used by orchestration systems.
    """

    return {"status": "ok"}


@app.post(
    "/runs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunSubmissionResponse,
    tags=["runs"],
)
async def submit_run(request: RunRequest, background_tasks: BackgroundTasks) -> RunSubmissionResponse:
    """
    Submit a pipeline execution request. Work is processed asynchronously; query
    `/jobs/{job_id}` to monitor progress.
    """

    job_id = str(uuid4())
    job_store.create_job(job_id=job_id, request=request.model_dump(), persist_to_db=request.persist_to_db)

    api_logger.info(
        "Queued pipeline job %s | mode=%s persist_to_db=%s",
        job_id,
        request.mode,
        request.persist_to_db,
    )

    background_tasks.add_task(_execute_pipeline_job, job_id, request.model_dump())

    return RunSubmissionResponse(
        job_id=job_id,
        status=JobStatus.QUEUED,
        persist_to_db=request.persist_to_db,
        detail=f"Pipeline run accepted. Track status via GET /jobs/{job_id}.",
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["runs"])
def get_job(job_id: str) -> JobStatusResponse:
    """
    Retrieve the status of a previously submitted job.
    """

    record = job_store.get(job_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")

    pipeline_summary: Optional[PipelineRunSummary] = None
    if record.run_id:
        pipeline_summary = _fetch_pipeline_summary(record.run_id)

    return record.to_response(pipeline_run=pipeline_summary)


@app.get("/runs/{run_id}", response_model=PipelineRunSummary, tags=["runs"])
def get_run(run_id: str) -> PipelineRunSummary:
    """
    Fetch persisted metadata for a specific pipeline run.
    """

    summary = _fetch_pipeline_summary(run_id)
    if not summary:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline run not found.")
    return summary


def _execute_pipeline_job(job_id: str, request_payload: Dict[str, Any]) -> None:
    """
    Background worker responsible for executing the pipeline.
    """

    request = RunRequest.model_validate(request_payload)
    logger = get_logger("api.job", request.log_level, "api.log", run_id=job_id)

    job_store.update(job_id, status=JobStatus.RUNNING, detail="Pipeline execution started.")

    try:
        sources = _load_sources(request, logger)
        augmentor_cfg = _load_augmentor(request, logger)

        run_id = run_pipeline(
            sources=sources,
            output=request.output_path,
            mode=request.mode,
            augmentor_cfg=augmentor_cfg,
            timeout=request.timeout,
            log_level=request.log_level,
            persist_to_db=request.persist_to_db,
        )

        detail = "Pipeline execution completed successfully."
        job_store.update(job_id, status=JobStatus.SUCCEEDED, run_id=run_id, detail=detail)
        logger.info("Pipeline job %s finished | run_id=%s", job_id, run_id)
    except Exception as exc:  # noqa: BLE001 - capture and surface any failure
        logger.exception("Pipeline job %s failed", job_id)
        job_store.update(
            job_id,
            status=JobStatus.FAILED,
            error=str(exc),
            detail="Pipeline execution failed; inspect logs for details.",
        )


def _load_sources(request: RunRequest, logger) -> list[Dict[str, Any]]:
    if request.sources:
        return [src.to_dict() for src in request.sources]
    if not request.sources_path:
        raise RuntimeError("sources_path must be provided or pre-validated.")
    return EngineConfig.load_sources(request.sources_path, logger)


def _load_augmentor(request: RunRequest, logger) -> Optional[Dict[str, Any]]:
    if request.mode != "augment":
        return None
    if request.augmentor_config:
        return request.augmentor_config
    if not request.augmentor_config_path:
        raise RuntimeError("augmentor_config_path must be provided or pre-validated.")
    return EngineConfig.load_augmentor(request.augmentor_config_path, logger)


def _fetch_pipeline_summary(run_id: str) -> Optional[PipelineRunSummary]:
    session = get_session()
    try:
        run: Optional[PipelineRun] = session.get(PipelineRun, run_id)
        if not run:
            return None
        return PipelineRunSummary(
            run_id=run.id,
            mode=run.mode,
            started_at=run.started_at,
            completed_at=run.completed_at,
            total_fetched=run.total_fetched,
            total_ingested=run.total_ingested,
            total_valid=run.total_valid,
            total_invalid=run.total_invalid,
            total_augmented=run.total_augmented,
            metadata_snapshot=run.metadata_snapshot or {},
        )
    finally:
        session.close()


__all__ = ["app"]
