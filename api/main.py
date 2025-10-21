from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, List
from uuid import uuid4
from datetime import datetime, timedelta
import hashlib
from threading import Thread

import yaml
from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, UploadFile, status, Request
from pydantic import ValidationError
from fastapi.responses import StreamingResponse

from db.models import HostedFeed, Indicator, IndicatorType, PipelineRun, PipelineConfigProfile, ConfigArtifact, AugmentedIndicator
from db.session import get_session, session_scope, ensure_schema
from pipeline.engine import EngineConfig, run_pipeline
from utils.logger import get_logger

from sqlalchemy import func
from sqlmodel import select

from .auth import require_api_key
from .jobs import JobStore
from .refresh import RefreshScheduler
from .schemas import (
    JobStatus,
    JobStatusResponse,
    PipelineRunSummary,
    RunRequest,
    RunSubmissionResponse,
    SourceConfig,
    ConfigProfileSummary,
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
refresh_scheduler = RefreshScheduler()
api_logger = get_logger("api.app", "INFO", "api.log")

HOSTED_TYPES = {
    "url": IndicatorType.URL,
    "fqdn": IndicatorType.FQDN,
    "ipv4": IndicatorType.IPV4,
    "ipv6": IndicatorType.IPV6,
    "cidr": IndicatorType.CIDR,
}

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
    dependencies=[Depends(require_api_key)],
)
async def submit_run(
    request: Request,
    background_tasks: BackgroundTasks,
) -> RunSubmissionResponse:
    """
    Submit a pipeline execution request. Work is processed asynchronously; query
    `/jobs/{job_id}` to monitor progress.
    """

    try:
        payload = RunRequest.model_validate(await request.json())
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        ) from exc
    payload = _prepare_run_request(payload)
    job_id = _queue_pipeline_job(payload, background_tasks)
    return RunSubmissionResponse(
        job_id=job_id,
        status=JobStatus.QUEUED,
        persist_to_db=payload.persist_to_db,
        profile_id=payload.profile_id,
        detail=f"Pipeline run accepted. Track status via GET /jobs/{job_id}.",
    )


@app.post(
    "/runs/upload",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunSubmissionResponse,
    tags=["runs"],
)
async def submit_run_with_files(
    background_tasks: BackgroundTasks,
    sources_file: UploadFile = File(...),
    augmentor_file: Optional[UploadFile] = File(None),
    firewall_file: Optional[UploadFile] = File(None),
    mode: str = Form("validate"),
    persist_to_db: bool = Form(False),
    output_path: str = Form("test_output_data/api_output.json"),
    timeout: int = Form(15),
    log_level: str = Form("INFO"),
    profile_name: Optional[str] = Form(None),
    refresh_interval_minutes: Optional[float] = Form(None),
    _: str = Depends(require_api_key),
) -> RunSubmissionResponse:
    sources_bytes = await sources_file.read()
    try:
        sources_payload = yaml.safe_load(sources_bytes) or {}
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid sources YAML: {exc}") from exc

    raw_sources = sources_payload.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise HTTPException(status_code=400, detail="sources.yaml must define a non-empty 'sources' list.")

    try:
        sources = [SourceConfig(**item) for item in raw_sources]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid source entry: {exc}") from exc

    augmentor_cfg = None
    augmentor_bytes = None
    if augmentor_file:
        augmentor_bytes = await augmentor_file.read()
        try:
            augmentor_cfg = yaml.safe_load(augmentor_bytes) or {}
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid augmentor YAML: {exc}") from exc

    firewall_cfg = None
    firewall_bytes = None
    if firewall_file:
        firewall_bytes = await firewall_file.read()
        try:
            firewall_cfg = yaml.safe_load(firewall_bytes) or {}
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid firewall YAML: {exc}") from exc

    artifacts = [("sources", sources_file.filename or "sources.yaml", sources_bytes)]
    if augmentor_bytes is not None:
        artifacts.append(("augmentor", augmentor_file.filename or "augmentor.yaml", augmentor_bytes))
    if firewall_bytes is not None:
        artifacts.append(("firewall", firewall_file.filename or "firewall.yaml", firewall_bytes))

    profile_settings = {
        "mode": mode,
        "output_path": output_path,
        "timeout": timeout,
        "log_level": log_level,
        "persist_to_db": persist_to_db,
        "refresh_interval_minutes": refresh_interval_minutes,
    }

    profile_id = _create_config_profile(profile_name, artifacts, profile_settings)

    payload = RunRequest(
        sources=sources,
        mode=mode,
        augmentor_config=augmentor_cfg,
        output_path=output_path,
        timeout=timeout,
        log_level=log_level,
        persist_to_db=persist_to_db,
        profile_id=profile_id,
    )

    job_id = _queue_pipeline_job(payload, background_tasks)

    uploads_dir = Path("config/uploads") / job_id
    uploads_dir.mkdir(parents=True, exist_ok=True)
    (uploads_dir / "sources.yaml").write_bytes(sources_bytes)
    if augmentor_bytes is not None:
        (uploads_dir / "augmentor.yaml").write_bytes(augmentor_bytes)
    if firewall_bytes is not None:
        (uploads_dir / "firewall.yaml").write_bytes(firewall_bytes)

    record = job_store.get(job_id)
    if record:
        updated_request = dict(record.request)
        updated_request["firewall_config"] = firewall_cfg
        updated_request["profile_id"] = profile_id
        job_store.update(
            job_id,
            request=updated_request,
            detail="Pipeline execution queued from uploaded configuration.",
        )

    api_logger.info(
        "Uploaded configuration queued for job %s | mode=%s persist_to_db=%s",
        job_id,
        mode,
        persist_to_db,
    )

    return RunSubmissionResponse(
        job_id=job_id,
        status=JobStatus.QUEUED,
        persist_to_db=persist_to_db,
        profile_id=profile_id,
        detail=f"Configuration accepted (profile {profile_id}) and pipeline run queued. Track status via GET /jobs/{job_id}.",
    )




@app.get("/jobs", response_model=List[JobStatusResponse], tags=["runs"])
def list_jobs() -> List[JobStatusResponse]:
    responses: List[JobStatusResponse] = []
    for record in job_store.all().values():
        pipeline_summary = _fetch_pipeline_summary(record.run_id) if record.run_id else None
        responses.append(record.to_response(pipeline_run=pipeline_summary))
    return responses
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




@app.get("/profiles", response_model=List[ConfigProfileSummary], tags=["config"])
def list_profiles() -> List[ConfigProfileSummary]:
    session = get_session()
    try:
        profiles = session.exec(select(PipelineConfigProfile)).all()
    finally:
        session.close()

    now = datetime.utcnow()
    summaries: List[ConfigProfileSummary] = []
    for profile in profiles or []:
        interval = profile.refresh_interval_minutes or 0.0
        next_run = None
        if interval > 0:
            last = profile.last_refreshed_at or profile.updated_at or profile.created_at
            if last:
                next_run = last + timedelta(minutes=interval)
        summaries.append(
            ConfigProfileSummary(
                id=profile.id,
                name=profile.name,
                description=profile.description,
                refresh_interval_minutes=profile.refresh_interval_minutes,
                last_refreshed_at=profile.last_refreshed_at,
                next_run_at=next_run,
                default_mode=profile.default_mode,
                default_output_path=profile.default_output_path,
                default_timeout=profile.default_timeout,
                default_log_level=profile.default_log_level,
                default_persist_to_db=profile.default_persist_to_db,
            )
        )
    return summaries
@app.get(
    "/edl/{indicator_type}",
    response_class=StreamingResponse,
    tags=["edl"],
    summary="Hosted EDL output segregated by indicator type.",
)
def serve_edl(indicator_type: str) -> StreamingResponse:
    indicator_key = indicator_type.lower()
    if indicator_key not in HOSTED_TYPES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unsupported indicator type.")

    indicator_enum = HOSTED_TYPES[indicator_key]
    run_id = _resolve_hosted_run(indicator_enum)

    if not run_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No pipeline runs available.")

    def stream(run_id: str):
        session = get_session()
        try:
            query = (
                select(Indicator.normalized, AugmentedIndicator.augmented_value)
                .select_from(Indicator)
                .outerjoin(AugmentedIndicator, AugmentedIndicator.indicator_id == Indicator.id)
                .where(
                    Indicator.run_id == run_id,
                    Indicator.entry_type == indicator_enum,
                    Indicator.is_valid == True,  # noqa: E712
                )
                .order_by(Indicator.normalized)
            )
            result = session.exec(query)
            for normalized, augmented_value in result:
                value = augmented_value or normalized
                yield f"{value}\n"
        finally:
            session.close()

    headers = {
        "Cache-Control": "no-cache",
        "X-EDL-Run-ID": run_id,
        "Content-Disposition": f'inline; filename="{indicator_key}.txt"',
    }
    return StreamingResponse(stream(run_id), media_type="text/plain", headers=headers)


@app.on_event("startup")
def _configure_scheduler() -> None:
    try:
        ensure_schema()
    except Exception:
        api_logger.exception("Failed to ensure database schema on startup.")
        raise
    interval_env = os.getenv("EDL_AUTO_REFRESH_MINUTES")
    if not interval_env:
        return
    try:
        interval = float(interval_env)
    except ValueError:
        api_logger.warning("Invalid EDL_AUTO_REFRESH_MINUTES value '%s'; skipping scheduler.", interval_env)
        return
    if interval <= 0:
        return

    api_logger.info("Starting auto-refresh scheduler (every %.2f minutes).", interval)
    refresh_scheduler.start(interval, _auto_refresh_tick)


@app.on_event("shutdown")
def _shutdown_scheduler() -> None:
    refresh_scheduler.stop()


def _prepare_run_request(run_request: RunRequest) -> RunRequest:
    if not run_request.profile_id:
        return run_request
    profile = _get_profile(run_request.profile_id)
    if not profile:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Configuration profile not found.")
    return run_request


def _queue_pipeline_job(run_request: RunRequest, background_tasks: Optional[BackgroundTasks] = None) -> str:
    job_id = str(uuid4())
    job_store.create_job(
        job_id=job_id,
        request=run_request.model_dump(),
        persist_to_db=run_request.persist_to_db,
        profile_id=run_request.profile_id,
    )

    api_logger.info(
        "Queued pipeline job %s | mode=%s persist_to_db=%s profile=%s",
        job_id,
        run_request.mode,
        run_request.persist_to_db,
        run_request.profile_id,
    )

    payload = run_request.model_dump()
    if background_tasks is not None:
        background_tasks.add_task(_execute_pipeline_job, job_id, payload)
    else:
        Thread(target=_execute_pipeline_job, args=(job_id, payload), daemon=True).start()
    return job_id



def _execute_pipeline_job(job_id: str, request_payload: Dict[str, Any]) -> None:
    """
    Background worker responsible for executing the pipeline.
    """

    request = RunRequest.model_validate(request_payload)
    logger = get_logger("api.job", request.log_level, "api.log", run_id=job_id)

    job_store.update(job_id, status=JobStatus.RUNNING, detail="Pipeline execution started.", profile_id=request.profile_id)

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
            profile_id=request.profile_id,
        )

        if run_id:
            try:
                _update_hosted_feeds(run_id)
            except Exception as update_exc:  # noqa: BLE001
                logger.exception("Failed to update hosted feeds for run %s: %s", run_id, update_exc)
        else:
            logger.info("Pipeline job %s completed without persistence; hosted feeds unchanged.", job_id)

        if request.profile_id:
            with session_scope() as session:
                profile = session.get(PipelineConfigProfile, request.profile_id)
                if profile:
                    profile.last_refreshed_at = datetime.utcnow()
                    profile.updated_at = datetime.utcnow()
                    session.add(profile)

        detail = "Pipeline execution completed successfully."
        job_store.update(
            job_id,
            status=JobStatus.SUCCEEDED,
            run_id=run_id,
            detail=detail,
            profile_id=request.profile_id,
        )
        logger.info("Pipeline job %s finished | run_id=%s", job_id, run_id)
    except Exception as exc:  # noqa: BLE001 - capture and surface any failure
        logger.exception("Pipeline job %s failed", job_id)
        job_store.update(
            job_id,
            status=JobStatus.FAILED,
            error=str(exc),
            detail="Pipeline execution failed; inspect logs for details.",
            profile_id=request.profile_id,
        )


def _update_hosted_feeds(run_id: str) -> None:
    with session_scope() as session:
        for indicator_type in IndicatorType:
            count = session.exec(
                select(func.count())
                .select_from(Indicator)
                .where(
                    Indicator.run_id == run_id,
                    Indicator.entry_type == indicator_type,
                    Indicator.is_valid == True,  # noqa: E712
                )
            ).one()
            total = count[0] if isinstance(count, tuple) else count
            if total and total > 0:
                hosted = session.exec(
                    select(HostedFeed).where(HostedFeed.indicator_type == indicator_type)
                ).one_or_none()
                if hosted:
                    hosted.run_id = run_id
                    hosted.updated_at = datetime.utcnow()
                else:
                    session.add(HostedFeed(indicator_type=indicator_type, run_id=run_id))


def _create_config_profile(name: Optional[str], artifacts: list[tuple[str, str, bytes]], settings: Optional[Dict[str, Any]] = None) -> str:
    settings = settings or {}
    generated_name = name or f"profile-{uuid4().hex[:8]}"
    with session_scope() as session:
        profile = PipelineConfigProfile(
            name=generated_name,
            description=settings.get('description'),
            refresh_interval_minutes=settings.get('refresh_interval_minutes'),
            default_mode=settings.get('mode', 'validate'),
            default_output_path=settings.get('output_path', 'test_output_data/validated_output.json'),
            default_timeout=settings.get('timeout', 15),
            default_log_level=settings.get('log_level', 'INFO'),
            default_persist_to_db=settings.get('persist_to_db', True),
        )
        session.add(profile)
        session.flush()

        for kind, filename, content_bytes in artifacts:
            if content_bytes is None:
                continue
            content_text = content_bytes.decode('utf-8', errors='replace')
            sha = hashlib.sha256(content_bytes).hexdigest()
            session.add(
                ConfigArtifact(
                    profile_id=profile.id,
                    kind=kind,
                    filename=filename,
                    content=content_text,
                    sha256=sha,
                )
            )

        profile.updated_at = datetime.utcnow()
        session.add(profile)
        profile_id = profile.id

    return profile_id


def _get_profile(profile_id: str) -> Optional[PipelineConfigProfile]:
    session = get_session()
    try:
        return session.get(PipelineConfigProfile, profile_id)
    finally:
        session.close()


def _get_config_artifact(profile_id: str, kind: str) -> Optional[ConfigArtifact]:
    session = get_session()
    try:
        return session.exec(
            select(ConfigArtifact)
            .where(
                ConfigArtifact.profile_id == profile_id,
                ConfigArtifact.kind == kind,
            )
            .order_by(ConfigArtifact.created_at.desc())
        ).first()
    finally:
        session.close()


def _deserialize_sources_yaml(content: str) -> list[Dict[str, Any]]:
    payload = yaml.safe_load(content) or {}
    if isinstance(payload, dict):
        raw_sources = payload.get('sources')
    elif isinstance(payload, list):
        raw_sources = payload
    else:
        raise RuntimeError('Invalid sources YAML structure.')
    if not isinstance(raw_sources, list) or not raw_sources:
        raise RuntimeError("sources.yaml must define a non-empty 'sources' list.")
    sources: list[Dict[str, Any]] = []
    for item in raw_sources:
        if not isinstance(item, dict):
            raise RuntimeError('Each source entry must be a mapping.')
        sources.append(SourceConfig(**item).model_dump(exclude_none=True))
    return sources



def _resolve_hosted_run(indicator_enum: IndicatorType) -> Optional[str]:
    session = get_session()
    try:
        hosted_run_id = session.exec(
            select(HostedFeed.run_id).where(HostedFeed.indicator_type == indicator_enum)
        ).one_or_none()
        if hosted_run_id:
            return hosted_run_id
        return session.exec(select(PipelineRun.id).order_by(PipelineRun.started_at.desc())).first()
    finally:
        session.close()


def _build_run_request_for_profile(profile: PipelineConfigProfile) -> RunRequest:
    return RunRequest(
        profile_id=profile.id,
        mode=profile.default_mode or "validate",
        output_path=profile.default_output_path or "test_output_data/validated_output.json",
        timeout=profile.default_timeout or 15,
        log_level=profile.default_log_level or "INFO",
        persist_to_db=bool(profile.default_persist_to_db),
    )


def _auto_refresh_tick() -> None:
    session = get_session()
    try:
        profiles = session.exec(
            select(PipelineConfigProfile)
            .where(
                PipelineConfigProfile.refresh_interval_minutes.is_not(None),
                PipelineConfigProfile.refresh_interval_minutes > 0,
            )
        ).all()
    finally:
        session.close()

    now = datetime.utcnow()
    for profile in profiles or []:
        interval = profile.refresh_interval_minutes or 0.0
        if interval <= 0:
            continue
        if job_store.has_active_job_for_profile(profile.id):
            continue
        last = profile.last_refreshed_at or profile.updated_at or profile.created_at
        if not last or (now - last).total_seconds() >= interval * 60:
            api_logger.info(
                "Auto-refresh scheduling profile %s (interval %.2f minutes)",
                profile.id,
                interval,
            )
            run_request = _build_run_request_for_profile(profile)
            _queue_pipeline_job(run_request)


def _load_sources(request: RunRequest, logger) -> list[Dict[str, Any]]:
    if request.profile_id:
        artifact = _get_config_artifact(request.profile_id, "sources")
        if not artifact:
            raise RuntimeError(f"No sources artifact found for profile {request.profile_id}")
        return _deserialize_sources_yaml(artifact.content)
    if request.sources:
        return [src.to_dict() for src in request.sources]
    if not request.sources_path:
        raise RuntimeError("sources_path must be provided or pre-validated.")
    return EngineConfig.load_sources(request.sources_path, logger)


def _load_augmentor(request: RunRequest, logger) -> Optional[Dict[str, Any]]:
    if request.mode != "augment":
        return None
    if request.profile_id:
        artifact = _get_config_artifact(request.profile_id, "augmentor")
        if artifact:
            try:
                return yaml.safe_load(artifact.content) or {}
            except yaml.YAMLError as exc:
                raise RuntimeError(f"Invalid augmentor YAML for profile {request.profile_id}: {exc}") from exc
        # Fall through to other sources if no artifact present
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
            profile_id=run.profile_id,
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
