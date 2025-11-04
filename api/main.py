from __future__ import annotations

import os
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Dict, List, Optional, Set, Type, TypeVar
from uuid import uuid4

import yaml
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_
from sqlmodel import select

from db.models import (
    Artifact,
    AdminAccount,
    HostedFeed,
    Indicator,
    IndicatorType,
    Pipeline,
    PipelineRun,
    Profile,
    ProfileConfig,
    RunError,
    RunState,
    AugmentedIndicator,
)
from db.persistence import (
    cancel_pipeline_run,
    create_admin_account,
    create_pipeline,
    create_pipeline_run,
    create_profile,
    create_profile_config,
    link_admin_to_profile,
    unlink_admin_from_profile,
    list_profile_ids_for_admin,
    get_admin_account,
    list_admin_accounts,
    update_profile_config as persistence_update_profile_config,
    update_pipeline_config as persistence_update_pipeline_config,
    finalize_pipeline_run,
    generate_profile_api_key,
    list_config_usage,
    list_pipelines,
    record_run_error,
    soft_delete_pipeline,
    update_run_state,
)
from db.session import ensure_schema, get_session, session_scope
from pipeline.engine import EngineConfig, run_pipeline
from utils.logger import get_logger
from utils.log_reader import read_run_log

from .auth import AuthContext, require_profile, require_reader, require_operator, require_super_admin
from .jobs import JobStore
from .refresh import RefreshScheduler
from pydantic import BaseModel, ValidationError
from .schemas import (
    ProfileCreateRequest,
    ProfileResponse,
    ProfileBootstrapResponse,
    ProfileSummary,
    ProfileConfigCreateRequest,
    ProfileConfigResponse,
    ProfileConfigUpdateRequest,
    ConfigUsageSummary,
    PipelineCreateRequest,
    PipelineListResponse,
    PipelineResponse,
    RunCreateRequest,
    RunSubmissionResponse,
    RunListResponse,
    PipelineRunSummary,
    PipelineRunDetail,
    ArtifactResponse,
    RunErrorResponse,
    JobStatusResponse,
    ActivePipelineEntry,
    ActivePipelineListResponse,
    PipelineIndicatorSearchResponse,
    PipelineConfigUpdateRequest,
    AdminAccountCreateRequest,
    AdminAccountResponse,
    AdminAccountBootstrapResponse,
    AdminProfileLinkResponse,
)


app = FastAPI(
    title="EDL Pipeline API",
    version="2.0.0",
    description=(
        "API surface for orchestrating the EDL ingestion/validation pipeline. "
        "Manage profiles, versioned configurations, and track pipeline runs."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://localhost:5176",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5175",
        "http://127.0.0.1:5176",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

job_store = JobStore()
refresh_scheduler = RefreshScheduler()
api_logger = get_logger("api.app", "INFO", "api.log")

PayloadModel = TypeVar("PayloadModel", bound=BaseModel)

HOSTED_TYPES = {
    "url": IndicatorType.URL,
    "fqdn": IndicatorType.FQDN,
    "ipv4": IndicatorType.IPV4,
    "ipv6": IndicatorType.IPV6,
    "cidr": IndicatorType.CIDR,
}


def _get_admin_scope(context: AuthContext) -> Optional[Set[str]]:
    if context.is_super_admin or not context.admin_id:
        return None
    scope: Optional[Set[str]] = getattr(context, "_profile_scope", None)
    if scope is None:
        scope = set(list_profile_ids_for_admin(context.admin_id))
        setattr(context, "_profile_scope", scope)
    return scope


def _assert_profile_access(context: AuthContext, profile_id: str) -> None:
    if not profile_id:
        return
    if context.profile_id and context.profile_id != profile_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")
    if context.profile_id == profile_id:
        return
    if context.is_super_admin:
        return
    if context.admin_id:
        scope = _get_admin_scope(context)
        if scope is not None and profile_id not in scope:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")
        return
    if not context.profile_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")


def _assert_pipeline_access(context: AuthContext, pipeline: Pipeline) -> None:
    if pipeline.deleted_at:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    if pipeline.profile_id:
        _assert_profile_access(context, pipeline.profile_id)


def _assert_run_access(context: AuthContext, run: PipelineRun) -> None:
    if run.profile_id:
        _assert_profile_access(context, run.profile_id)
        return
    if run.pipeline_id:
        session = get_session()
        try:
            pipeline = session.get(Pipeline, run.pipeline_id)
            if pipeline:
                _assert_pipeline_access(context, pipeline)
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------
@app.on_event("startup")
def startup_event() -> None:
    ensure_schema()
    interval_env = os.getenv("EDL_AUTO_REFRESH_MINUTES")
    if interval_env:
        try:
            interval = float(interval_env)
        except ValueError:
            api_logger.warning("Invalid EDL_AUTO_REFRESH_MINUTES value '%s'; skipping scheduler.", interval_env)
            interval = 0.0
        if interval > 0:
            refresh_scheduler.start(interval, _auto_refresh_tick)


@app.on_event("shutdown")
def shutdown_event() -> None:
    refresh_scheduler.stop()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", tags=["system"])
def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Admin management
# ---------------------------------------------------------------------------
@app.post(
    "/admin/accounts",
    response_model=AdminAccountBootstrapResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["admin"],
)
async def create_admin_account_endpoint(
    request: Request,
    context: AuthContext = Depends(require_super_admin),
) -> AdminAccountBootstrapResponse:
    body = await _safe_json(request)
    payload = _extract_payload(body, AdminAccountCreateRequest)
    role = (payload.role or "operator").strip().lower()
    if role not in {"operator", "reader"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Role must be 'operator' or 'reader'.",
        )
    account, api_key = create_admin_account(
        name=payload.name,
        role=role,
        is_super_admin=bool(payload.is_super_admin),
    )
    base = _serialize_admin_account(account)
    return AdminAccountBootstrapResponse(**base.model_dump(), api_key=api_key)


@app.get(
    "/admin/accounts",
    response_model=List[AdminAccountResponse],
    tags=["admin"],
)
def list_admin_accounts_endpoint(
    context: AuthContext = Depends(require_super_admin),
) -> List[AdminAccountResponse]:
    accounts = list_admin_accounts()
    return [_serialize_admin_account(account) for account in accounts]


@app.post(
    "/admin/accounts/{admin_id}/profiles/{profile_id}",
    response_model=AdminProfileLinkResponse,
    status_code=status.HTTP_200_OK,
    tags=["admin"],
)
def link_admin_profile_endpoint(
    admin_id: str,
    profile_id: str,
    context: AuthContext = Depends(require_super_admin),
) -> AdminProfileLinkResponse:
    admin = get_admin_account(admin_id)
    if not admin:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin account not found.")
    session = get_session()
    try:
        _ensure_profile_exists(session, profile_id)
    finally:
        session.close()
    link_admin_to_profile(admin_id, profile_id)
    return AdminProfileLinkResponse(admin_account_id=admin_id, profile_id=profile_id)


@app.delete(
    "/admin/accounts/{admin_id}/profiles/{profile_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["admin"],
)
def unlink_admin_profile_endpoint(
    admin_id: str,
    profile_id: str,
    context: AuthContext = Depends(require_super_admin),
) -> Response:
    admin = get_admin_account(admin_id)
    if not admin:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin account not found.")
    unlink_admin_from_profile(admin_id, profile_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Profile management
# ---------------------------------------------------------------------------
@app.post(
    "/profiles",
    response_model=ProfileBootstrapResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["profiles"],
)
async def create_profile_endpoint(
    request: Request,
    owner_admin_id: Optional[str] = None,
    context: AuthContext = Depends(require_operator),
) -> ProfileBootstrapResponse:
    if context.profile_id is not None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin API key required.")
    body = await _safe_json(request)
    payload = _extract_payload(body, ProfileCreateRequest)
    profile = create_profile(name=payload.name, description=payload.description)
    api_key = generate_profile_api_key(profile.id)

    assigned_admin_id: Optional[str] = None
    if owner_admin_id:
        if not context.is_super_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only super admins may assign profile ownership.",
            )
        owner = get_admin_account(owner_admin_id)
        if not owner:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Admin account not found.",
            )
        assigned_admin_id = owner.id
    elif context.admin_id and not context.is_super_admin:
        assigned_admin_id = context.admin_id

    if assigned_admin_id:
        try:
            link_admin_to_profile(assigned_admin_id, profile.id)
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    serialized = _serialize_profile(profile)
    return ProfileBootstrapResponse(**serialized.model_dump(), api_key=api_key)


@app.get("/profiles", response_model=List[ProfileSummary], tags=["profiles"])
def list_profiles_endpoint(context: AuthContext = Depends(require_reader)) -> List[ProfileSummary]:
    if context.profile_id is not None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin API key required.")
    scope = _get_admin_scope(context)
    session = get_session()
    try:
        profiles = session.exec(select(Profile)).all()
        if scope is not None:
            profiles = [profile for profile in profiles if profile.id in scope]
        if scope is not None and not profiles:
            return []
        results: List[ProfileSummary] = []
        for profile in profiles:
            latest_version = session.exec(
                select(func.max(ProfileConfig.version)).where(ProfileConfig.profile_id == profile.id)
            ).one()
            total_runs = session.exec(
                select(func.count()).select_from(PipelineRun).where(PipelineRun.profile_id == profile.id)
            ).one()
            active_runs = session.exec(
                select(func.count()).select_from(PipelineRun).where(
                    PipelineRun.profile_id == profile.id,
                    PipelineRun.state.in_([RunState.QUEUED, RunState.RUNNING]),
                )
            ).one()
            results.append(
                ProfileSummary(
                    id=profile.id,
                    name=profile.name,
                    description=profile.description,
                    created_at=profile.created_at,
                    updated_at=profile.updated_at,
                    latest_config_version=int(latest_version or 0) or None,
                    total_runs=int(total_runs or 0),
                    active_runs=int(active_runs or 0),
                )
            )
        return results
    finally:
        session.close()


@app.post(
    "/profiles/{profile_id}/configs",
    response_model=ProfileConfigResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["profiles"],
)
async def create_profile_config_endpoint(
    profile_id: str,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> ProfileConfigResponse:
    _assert_profile_access(context, profile_id)
    body = await _safe_json(request)
    payload = _extract_payload(body, ProfileConfigCreateRequest)
    try:
        config = create_profile_config(
            profile_id=profile_id,
            sources_yaml=payload.sources_yaml,
            augment_yaml=payload.augment_yaml,
            rules_yaml=payload.rules_yaml,
            pipeline_settings=payload.pipeline_settings,
            created_by=payload.created_by or context.api_key,
            refresh_interval_minutes=payload.refresh_interval_minutes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return _serialize_profile_config(config)


@app.patch(
    "/profiles/{profile_id}/configs/{config_id}",
    response_model=ProfileConfigResponse,
    tags=["profiles"],
)
async def update_profile_config_endpoint(
    profile_id: str,
    config_id: str,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> ProfileConfigResponse:
    _assert_profile_access(context, profile_id)
    body = await _safe_json(request)
    payload = _extract_payload(body, ProfileConfigUpdateRequest)

    if not any(
        [
            payload.sources_yaml is not None,
            payload.augment_yaml is not None,
            payload.rules_yaml is not None,
            payload.pipeline_settings is not None,
            payload.refresh_interval_minutes is not None,
        ]
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one field must be provided to update the configuration.",
        )

    try:
        config = persistence_update_profile_config(
            config_id=config_id,
            profile_id=profile_id,
            sources_yaml=payload.sources_yaml,
            augment_yaml=payload.augment_yaml,
            rules_yaml=payload.rules_yaml,
            pipeline_settings=payload.pipeline_settings,
            refresh_interval_minutes=payload.refresh_interval_minutes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return _serialize_profile_config(config)


@app.get(
    "/profiles/{profile_id}/configs",
    response_model=List[ConfigUsageSummary],
    tags=["profiles"],
)
def list_profile_configs_endpoint(
    profile_id: str,
    context: AuthContext = Depends(require_reader),
) -> List[ConfigUsageSummary]:
    _assert_profile_access(context, profile_id)
    usage = list_config_usage(profile_id)
    if not usage:
        session = get_session()
        try:
            _ensure_profile_exists(session, profile_id)
        finally:
            session.close()
        return []

    results: List[ConfigUsageSummary] = []
    for entry in usage:
        config: ProfileConfig = entry["config"]
        results.append(
            ConfigUsageSummary(
                config=_serialize_profile_config(config),
                pipelines_total=entry["pipelines_total"],
                pipelines_active=entry["pipelines_active"],
                runs_active=entry["runs_active"],
            )
        )
    return results


@app.get(
    "/profiles/{profile_id}/pipelines/active",
    response_model=ActivePipelineListResponse,
    tags=["pipelines"],
)
def list_active_pipelines(
    profile_id: str,
    context: AuthContext = Depends(require_reader),
) -> ActivePipelineListResponse:
    _assert_profile_access(context, profile_id)
    session = get_session()
    try:
        pipelines = session.exec(
            select(Pipeline)
            .where(
                Pipeline.profile_id == profile_id,
                Pipeline.deleted_at.is_(None),
                Pipeline.is_active.is_(True),
            )
            .order_by(Pipeline.created_at)
        ).all()
        if not pipelines:
            _ensure_profile_exists(session, profile_id)
            return ActivePipelineListResponse(profile_id=profile_id, pipelines=[])

        pipeline_ids = [pipeline.id for pipeline in pipelines]
        config_ids = {pipeline.profile_config_id for pipeline in pipelines if pipeline.profile_config_id}

        configs = {}
        if config_ids:
            configs = {
                cfg.id: cfg
                for cfg in session.exec(select(ProfileConfig).where(ProfileConfig.id.in_(config_ids))).all()
            }

        last_runs: Dict[str, PipelineRun] = {}
        if pipeline_ids:
            runs = session.exec(
                select(PipelineRun)
                .where(PipelineRun.pipeline_id.in_(pipeline_ids))
                .order_by(PipelineRun.pipeline_id, PipelineRun.queued_at.desc())
            ).all()
            for run in runs:
                if run.pipeline_id not in last_runs:
                    last_runs[run.pipeline_id] = run

        active_entries: List[ActivePipelineEntry] = []
        for pipeline in pipelines:
            config = configs.get(pipeline.profile_config_id)
            if not config:
                api_logger.warning(
                    "Pipeline %s references missing config %s", pipeline.id, pipeline.profile_config_id
                )
                continue

            last_run = last_runs.get(pipeline.id)
            reference = None
            if last_run:
                reference = last_run.completed_at or last_run.started_at or last_run.queued_at
            if not reference:
                reference = pipeline.updated_at or pipeline.created_at

            interval = config.refresh_interval_minutes
            next_run_at = None
            is_scheduled = bool(interval and interval > 0)
            if is_scheduled and reference:
                next_run_at = reference + timedelta(minutes=interval)

            active_entries.append(
                ActivePipelineEntry(
                    pipeline_id=pipeline.id,
                    pipeline_name=pipeline.name,
                    profile_config_id=pipeline.profile_config_id,
                    profile_config_version=config.version,
                    last_run_id=last_run.id if last_run else None,
                    last_run_state=last_run.state if last_run else None,
                    last_run_started_at=last_run.started_at if last_run else None,
                    last_run_completed_at=last_run.completed_at if last_run else None,
                    is_scheduled=is_scheduled,
                    next_run_at=next_run_at,
                )
            )

        return ActivePipelineListResponse(profile_id=profile_id, pipelines=active_entries)
    finally:
        session.close()


@app.get(
    "/profiles/{profile_id}/pipelines/search",
    response_model=PipelineIndicatorSearchResponse,
    tags=["pipelines"],
)
def search_pipelines_by_indicator(
    profile_id: str,
    indicator: str = Query(
        ...,
        min_length=1,
        description="Indicator value (domain, IP, URL, etc.) to locate within hosted EDLs.",
    ),
    context: AuthContext = Depends(require_reader),
) -> PipelineIndicatorSearchResponse:
    _assert_profile_access(context, profile_id)
    raw_value = indicator.strip()
    if not raw_value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Indicator value must be a non-empty string.",
        )

    normalized_value = raw_value.lower()

    session = get_session()
    try:
        pipelines = session.exec(
            select(Pipeline)
            .where(
                Pipeline.profile_id == profile_id,
                Pipeline.deleted_at.is_(None),
                Pipeline.is_active.is_(True),
            )
            .order_by(Pipeline.created_at)
        ).all()

        if not pipelines:
            _ensure_profile_exists(session, profile_id)
            return PipelineIndicatorSearchResponse(profile_id=profile_id, indicator=raw_value, pipelines=[])

        pipeline_ids = [pipeline.id for pipeline in pipelines]
        config_ids = {pipeline.profile_config_id for pipeline in pipelines if pipeline.profile_config_id}

        configs = {}
        if config_ids:
            configs = {
                cfg.id: cfg
                for cfg in session.exec(select(ProfileConfig).where(ProfileConfig.id.in_(config_ids))).all()
            }

        last_runs: Dict[str, PipelineRun] = {}
        if pipeline_ids:
            runs = session.exec(
                select(PipelineRun)
                .where(PipelineRun.pipeline_id.in_(pipeline_ids))
                .order_by(PipelineRun.pipeline_id, PipelineRun.queued_at.desc())
            ).all()
            for run in runs:
                if run.pipeline_id not in last_runs:
                    last_runs[run.pipeline_id] = run

        if not last_runs:
            return PipelineIndicatorSearchResponse(profile_id=profile_id, indicator=raw_value, pipelines=[])

        run_ids = [run.id for run in last_runs.values()]
        indicator_run_ids: set[str] = set()
        if run_ids:
            indicator_run_ids = set(
                session.exec(
                    select(Indicator.run_id).where(
                        Indicator.run_id.in_(run_ids),
                        Indicator.is_valid.is_(True),
                        or_(
                            func.lower(Indicator.normalized) == normalized_value,
                            func.lower(Indicator.original) == normalized_value,
                        ),
                    )
                ).all()
            )

        matches: List[ActivePipelineEntry] = []
        for pipeline in pipelines:
            last_run = last_runs.get(pipeline.id)
            if not last_run or last_run.id not in indicator_run_ids:
                continue

            config = configs.get(pipeline.profile_config_id)
            if not config:
                api_logger.warning(
                    "Pipeline %s references missing config %s", pipeline.id, pipeline.profile_config_id
                )
                continue

            interval = config.refresh_interval_minutes
            is_scheduled = bool(interval and interval > 0)

            reference = (
                last_run.completed_at
                or last_run.started_at
                or last_run.queued_at
                or pipeline.updated_at
                or pipeline.created_at
            )
            next_run_at = None
            if is_scheduled and reference:
                next_run_at = reference + timedelta(minutes=interval)

            matches.append(
                ActivePipelineEntry(
                    pipeline_id=pipeline.id,
                    pipeline_name=pipeline.name,
                    profile_config_id=pipeline.profile_config_id,
                    profile_config_version=config.version,
                    last_run_id=last_run.id,
                    last_run_state=last_run.state,
                    last_run_started_at=last_run.started_at,
                    last_run_completed_at=last_run.completed_at,
                    is_scheduled=is_scheduled,
                    next_run_at=next_run_at,
                )
            )

        return PipelineIndicatorSearchResponse(
            profile_id=profile_id,
            indicator=raw_value,
            pipelines=matches,
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Pipeline management
# ---------------------------------------------------------------------------
@app.post(
    "/pipelines",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["pipelines"],
)
async def create_pipeline_endpoint(
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> PipelineResponse:
    body = await _safe_json(request)
    payload = _extract_payload(body, PipelineCreateRequest)
    _assert_profile_access(context, payload.profile_id)
    idempotency_key = request.headers.get("Idempotency-Key")
    pipeline = create_pipeline(
        profile_id=payload.profile_id,
        profile_config_id=payload.profile_config_id,
        name=payload.name,
        description=payload.description,
        concurrency_limit=payload.concurrency_limit,
        idempotency_key=idempotency_key,
        created_by=payload.created_by or context.api_key,
    )
    return _serialize_pipeline(pipeline)


@app.patch(
    "/pipelines/{pipeline_id}/config",
    response_model=PipelineResponse,
    tags=["pipelines"],
)
async def update_pipeline_config_endpoint(
    pipeline_id: str,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> PipelineResponse:
    session = get_session()
    try:
        pipeline = session.get(Pipeline, pipeline_id)
        if not pipeline or pipeline.deleted_at:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        _assert_pipeline_access(context, pipeline)
    finally:
        session.close()

    body = await _safe_json(request)
    payload = _extract_payload(body, PipelineConfigUpdateRequest)

    try:
        updated = persistence_update_pipeline_config(
            pipeline_id=pipeline_id,
            profile_config_id=payload.profile_config_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return _serialize_pipeline(updated)


@app.delete(
    "/pipelines/{pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["pipelines"],
)
async def delete_pipeline_endpoint(
    pipeline_id: str,
    context: AuthContext = Depends(require_operator),
) -> Response:
    session = get_session()
    try:
        pipeline = session.get(Pipeline, pipeline_id)
        if not pipeline or pipeline.deleted_at:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
        _assert_pipeline_access(context, pipeline)
    finally:
        session.close()

    soft_delete_pipeline(pipeline_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/pipelines",
    response_model=PipelineListResponse,
    tags=["pipelines"],
)
def list_pipelines_endpoint(
    profile_id: Optional[str] = None,
    profile_config_id: Optional[str] = None,
    active_only: bool = False,
    limit: int = 50,
    offset: int = 0,
    context: AuthContext = Depends(require_reader),
) -> PipelineListResponse:
    scope = _get_admin_scope(context)
    if context.profile_id:
        if profile_id and profile_id != context.profile_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")
        profile_id = context.profile_id
    elif scope is not None:
        if profile_id and profile_id not in scope:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")

    if scope is not None and not scope and profile_id is None:
        return PipelineListResponse(pipelines=[])

    pipelines = list_pipelines(profile_id=profile_id, profile_config_id=profile_config_id)
    if scope is not None and profile_id is None:
        pipelines = [p for p in pipelines if p.profile_id in scope]
    if active_only:
        pipelines = [p for p in pipelines if p.is_active]
    if offset:
        pipelines = pipelines[offset:]
    if limit:
        pipelines = pipelines[:limit]
    return PipelineListResponse(pipelines=[_serialize_pipeline(p) for p in pipelines])


@app.get(
    "/profiles/{profile_id}/pipelines",
    response_model=PipelineListResponse,
    tags=["pipelines"],
)
def list_profile_pipelines_endpoint(
    profile_id: str,
    active_only: bool = False,
    limit: int = 50,
    offset: int = 0,
    context: AuthContext = Depends(require_reader),
) -> PipelineListResponse:
    _assert_profile_access(context, profile_id)
    pipelines = list_pipelines(profile_id=profile_id)
    if active_only:
        pipelines = [p for p in pipelines if p.is_active]
    if offset:
        pipelines = pipelines[offset:]
    if limit:
        pipelines = pipelines[:limit]
    return PipelineListResponse(pipelines=[_serialize_pipeline(p) for p in pipelines])


# ---------------------------------------------------------------------------
# Run management
# ---------------------------------------------------------------------------
@app.post(
    "/runs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunSubmissionResponse,
    tags=["runs"],
)
async def create_run_endpoint(
    background_tasks: BackgroundTasks,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> RunSubmissionResponse:
    body = await _safe_json(request)
    payload = _extract_payload(body, RunCreateRequest)
    idempotency_key = request.headers.get("Idempotency-Key")

    session = get_session()
    try:
        pipeline = session.get(Pipeline, payload.pipeline_id)
        if not pipeline or pipeline.deleted_at or not pipeline.is_active:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found or inactive")
        _assert_pipeline_access(context, pipeline)
        config = session.get(ProfileConfig, pipeline.profile_config_id)
        if not config:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Pipeline configuration missing")
        session.expunge(pipeline)
        session.expunge(config)
    finally:
        session.close()

    overrides = dict(payload.overrides or {})
    return _enqueue_pipeline_run(
        pipeline=pipeline,
        config=config,
        overrides=overrides,
        requested_by=payload.requested_by or context.api_key,
        idempotency_key=idempotency_key,
        background_tasks=background_tasks,
    )


@app.post(
    "/pipelines/{pipeline_id}/runs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunSubmissionResponse,
    tags=["runs"],
)
async def create_pipeline_scoped_run_endpoint(
    pipeline_id: str,
    background_tasks: BackgroundTasks,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> RunSubmissionResponse:
    body = await _safe_json(request)
    body.setdefault("pipeline_id", pipeline_id)
    payload = _extract_payload(body, RunCreateRequest)
    idempotency_key = request.headers.get("Idempotency-Key")

    session = get_session()
    try:
        pipeline = session.get(Pipeline, pipeline_id)
        if not pipeline or pipeline.deleted_at or not pipeline.is_active:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found or inactive")
        _assert_pipeline_access(context, pipeline)
        config = session.get(ProfileConfig, pipeline.profile_config_id)
        if not config:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Pipeline configuration missing")
        session.expunge(pipeline)
        session.expunge(config)
    finally:
        session.close()

    overrides = dict(payload.overrides or {})
    return _enqueue_pipeline_run(
        pipeline=pipeline,
        config=config,
        overrides=overrides,
        requested_by=payload.requested_by or context.api_key,
        idempotency_key=idempotency_key,
        background_tasks=background_tasks,
    )


@app.get("/runs", response_model=RunListResponse, tags=["runs"])
def list_runs_endpoint(
    state: Optional[RunState] = None,
    profile_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    context: AuthContext = Depends(require_reader),
) -> RunListResponse:
    scope = _get_admin_scope(context)
    if context.profile_id:
        if profile_id and profile_id != context.profile_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")
        profile_id = context.profile_id
    elif scope is not None:
        if profile_id and profile_id not in scope:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")

    if scope is not None and not scope and profile_id is None:
        return RunListResponse(runs=[])

    session = get_session()
    try:
        if pipeline_id:
            pipeline = session.get(Pipeline, pipeline_id)
            if not pipeline or pipeline.deleted_at:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
            _assert_pipeline_access(context, pipeline)

        query = select(PipelineRun)
        if state:
            query = query.where(PipelineRun.state == state)
        if profile_id:
            query = query.where(PipelineRun.profile_id == profile_id)
        elif scope:
            query = query.where(PipelineRun.profile_id.in_(scope))
        if pipeline_id:
            query = query.where(PipelineRun.pipeline_id == pipeline_id)
        query = query.order_by(PipelineRun.queued_at.desc())
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        runs = session.exec(query).all()
        summaries = [_serialize_run_summary(run) for run in runs]
        return RunListResponse(runs=summaries)
    finally:
        session.close()


@app.get("/runs/{run_id}", response_model=PipelineRunDetail, tags=["runs"])
def get_run_detail_endpoint(run_id: str, context: AuthContext = Depends(require_reader)) -> PipelineRunDetail:
    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        _assert_run_access(context, run)
        artifacts = session.exec(select(Artifact).where(Artifact.run_id == run.id)).all()
        errors = session.exec(select(RunError).where(RunError.run_id == run.id)).all()
        return _serialize_run_detail(run, artifacts, errors)
    finally:
        session.close()




@app.get("/runs/{run_id}/logs", tags=["runs"])
def get_run_logs_endpoint(
    run_id: str,
    limit: Optional[int] = None,
    context: AuthContext = Depends(require_reader),
) -> Dict[str, Any]:
    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        _assert_run_access(context, run)
    finally:
        session.close()

    lines = read_run_log(run_id, limit=limit)
    return {"run_id": run_id, "lines": lines}

@app.get("/jobs", response_model=List[JobStatusResponse], tags=["runs"])
def list_jobs_endpoint(
    pipeline_id: Optional[str] = None,
    profile_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    context: AuthContext = Depends(require_reader),
) -> List[JobStatusResponse]:
    scope = _get_admin_scope(context)
    if context.profile_id:
        if profile_id and profile_id != context.profile_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")
        profile_id = context.profile_id
    elif scope is not None:
        if profile_id and profile_id not in scope:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden for this profile.")

    if scope is not None and not scope and profile_id is None:
        return []

    responses: List[JobStatusResponse] = []
    runs_cache: Dict[str, PipelineRunSummary] = {}
    for job_id, record in job_store.all().items():
        if pipeline_id and record.pipeline_id != pipeline_id:
            continue
        if profile_id and record.profile_id != profile_id:
            continue
        if scope is not None and record.profile_id not in scope:
            continue
        summary = None
        if record.run_id:
            summary = runs_cache.get(record.run_id)
            if not summary:
                summary = _get_run_summary(record.run_id)
                if summary:
                    runs_cache[record.run_id] = summary
        responses.append(record.to_response(pipeline_run=summary))
    if offset:
        responses = responses[offset:]
    if limit:
        responses = responses[:limit]
    return responses


@app.post(
    "/runs/{run_id}/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["runs"],
)
async def cancel_run_endpoint(
    run_id: str,
    request: Request,
    context: AuthContext = Depends(require_operator),
) -> Dict[str, str]:
    body: Dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        body = await _safe_json(request)
    reason = body.get("reason") if isinstance(body, dict) else None

    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        _assert_run_access(context, run)
    finally:
        session.close()

    job_record = job_store.find_by_run_id(run_id)
    if job_record:
        job_store.update(job_record.id, cancel_requested=True, detail="Cancellation requested")

    cancelled = cancel_pipeline_run(run_id, cancelled_by=context.api_key, reason=reason)
    if not cancelled and not job_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found or not cancellable")

    return {"status": "cancellation_requested", "run_id": run_id}


# ---------------------------------------------------------------------------
# Hosted feed endpoints
# ---------------------------------------------------------------------------
@app.get(
    "/edl/{indicator_type}",
    response_class=StreamingResponse,
    tags=["feeds"],
    summary="Hosted EDL output segregated by indicator type.",
)
def serve_edl(
    indicator_type: str,
    pipeline_id: Optional[str] = None,
) -> StreamingResponse:
    indicator_key = indicator_type.lower()
    if indicator_key not in HOSTED_TYPES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unsupported indicator type.")

    indicator_enum = HOSTED_TYPES[indicator_key]
    run_id = _resolve_hosted_run(indicator_enum, pipeline_id=pipeline_id)

    if not run_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No pipeline runs available.")

    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        if pipeline_id:
            pipeline = session.get(Pipeline, pipeline_id)
            if not pipeline or pipeline.deleted_at:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    finally:
        session.close()

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
                if value:
                    yield f"{value}\n"
        finally:
            session.close()

    headers = {
        "Cache-Control": "no-cache",
        "X-EDL-Run-ID": run_id,
        "X-EDL-Pipeline-ID": pipeline_id or "",
        "Content-Disposition": f'inline; filename="{indicator_key}.txt"',
    }
    return StreamingResponse(stream(run_id), media_type="text/plain", headers=headers)

@app.get(
    "/pipelines/{pipeline_id}/edl/{indicator_type}",
    response_class=StreamingResponse,
    tags=["feeds"],
    summary="Hosted EDL output for a specific pipeline.",
)
def serve_pipeline_edl(
    pipeline_id: str,
    indicator_type: str,
) -> StreamingResponse:
    return serve_edl(indicator_type, pipeline_id=pipeline_id)



# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _execute_pipeline_job(job_id: str) -> None:
    record = job_store.get(job_id)
    if not record:
        return

    def _is_cancelled() -> bool:
        current = job_store.get(job_id)
        return bool(current and current.cancel_requested)

    try:
        if _is_cancelled():
            cancel_pipeline_run(record.run_id, cancelled_by="system", reason="Cancelled before start")
            job_store.update(job_id, state=RunState.CANCELLED, detail="Cancelled before start")
            return

        job_store.update(job_id, state=RunState.RUNNING, detail="Pipeline execution started")
        update_run_state(
            record.run_id,
            state=RunState.RUNNING,
            sub_state="fetch",
            percent_complete=0.0,
            metadata={"job_id": job_id},
            started_at=datetime.utcnow(),
        )

        session = get_session()
        try:
            pipeline = session.get(Pipeline, record.pipeline_id) if record.pipeline_id else None
            if not pipeline or pipeline.deleted_at or not pipeline.is_active:
                raise RuntimeError("Pipeline not found or inactive")
            config = session.get(ProfileConfig, pipeline.profile_config_id)
            if not config:
                raise RuntimeError("Pipeline configuration not found")
            sources_yaml = config.sources_yaml
            augment_yaml = config.augment_yaml
            rules_yaml = config.rules_yaml
            pipeline_settings = dict(config.pipeline_settings or {})
        finally:
            session.close()

        overrides = record.request.get("overrides") or {}
        pipeline_settings.update(overrides)

        if _is_cancelled():
            cancel_pipeline_run(record.run_id, cancelled_by="system", reason="Cancelled before fetch")
            job_store.update(job_id, state=RunState.CANCELLED, detail="Cancelled before fetch")
            return

        sources = EngineConfig.parse_sources_yaml(sources_yaml, api_logger)
        augmentor_cfg = EngineConfig.parse_augmentor_yaml(augment_yaml, api_logger)
        rules_cfg = EngineConfig.parse_rules_yaml(rules_yaml, api_logger)

        mode = pipeline_settings.get("mode", "validate")
        timeout = int(pipeline_settings.get("timeout", 15))
        log_level = pipeline_settings.get("log_level", "INFO")
        output_path = pipeline_settings.get("output_path", "test_output_data/validated_output.json")
        persist_flag = bool(pipeline_settings.get("persist_to_db", True))
        proxy_url = pipeline_settings.get("proxy")
        use_proxy = bool(pipeline_settings.get("use_proxy", False))

        run_pipeline(
            sources=sources,
            output=output_path,
            mode=mode,
            augmentor_cfg=augmentor_cfg if mode == "augment" else None,
            timeout=timeout,
            log_level=log_level,
            persist_to_db=persist_flag,
            proxy=proxy_url,
            use_proxy=use_proxy,
            profile_id=pipeline.profile_id,
            profile_config_id=pipeline.profile_config_id,
            run_id=record.run_id,
            rules=rules_cfg,
        )

        _update_hosted_feeds(record.run_id, pipeline_id=pipeline.id)

        final_state = RunState.SUCCESS
        session = get_session()
        try:
            run = session.get(PipelineRun, record.run_id)
            if run:
                final_state = run.state
        finally:
            session.close()

        detail_message = "Pipeline completed successfully" if final_state == RunState.SUCCESS else f"Pipeline finished with state {final_state.value}"
        job_store.update(job_id, state=final_state, detail=detail_message)

        if pipeline.profile_id:
            with session_scope() as inner:
                profile = inner.get(Profile, pipeline.profile_id)
                if profile:
                    profile.last_refreshed_at = datetime.utcnow()
                    inner.add(profile)

    except Exception as exc:  # noqa: BLE001
        api_logger.exception("Pipeline job %s failed", job_id)
        if record.run_id:
            record_run_error(
                run_id=record.run_id,
                phase="pipeline",
                source=None,
                message=str(exc),
            )
            update_run_state(
                record.run_id,
                state=RunState.FAILED,
                sub_state=None,
                percent_complete=100.0,
            )
        job_store.update(job_id, state=RunState.FAILED, detail=str(exc), error=str(exc))


def _update_hosted_feeds(run_id: str, pipeline_id: str) -> None:
    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            return
        metadata_snapshot = run.metadata_snapshot or {}
        hosting_meta = metadata_snapshot.get("hosting_types")
        if hosting_meta is None or hosting_meta == "all":
            allowed_indicator_types = set(HOSTED_TYPES.values())
        else:
            if isinstance(hosting_meta, list):
                hosting_values = hosting_meta
            else:
                hosting_values = [hosting_meta]
            allowed_indicator_types = {
                HOSTED_TYPES.get(str(item).lower())
                for item in hosting_values
                if isinstance(item, str) and HOSTED_TYPES.get(str(item).lower())
            }
            if not allowed_indicator_types:
                allowed_indicator_types = set(HOSTED_TYPES.values())
        counts = session.exec(
            select(Indicator.entry_type, func.count()).where(Indicator.run_id == run.id, Indicator.is_valid == True).group_by(Indicator.entry_type)  # noqa: E712
        ).all()
        count_map = {entry_type.value: total for entry_type, total in counts}

        for key, indicator_enum in HOSTED_TYPES.items():
            if indicator_enum not in allowed_indicator_types:
                continue
            if not count_map.get(key):
                continue
            target_pipeline_id = pipeline_id or run.pipeline_id
            if not target_pipeline_id:
                continue
            if pipeline_id:
                hosted_query = select(HostedFeed).where(
                    HostedFeed.pipeline_id == pipeline_id,
                    HostedFeed.indicator_type == indicator_enum,
                )
            else:
                hosted_query = select(HostedFeed).where(HostedFeed.indicator_type == indicator_enum)
            hosted = session.exec(hosted_query).one_or_none()
            if hosted:
                hosted.run_id = run.id
                hosted.updated_at = datetime.utcnow()
                if hosted.pipeline_id != target_pipeline_id:
                    hosted.pipeline_id = target_pipeline_id
                session.add(hosted)
            else:
                session.add(
                    HostedFeed(
                        pipeline_id=target_pipeline_id,
                        indicator_type=indicator_enum,
                        run_id=run.id,
                    )
                )
        session.commit()
    finally:
        session.close()


def _resolve_hosted_run(
    indicator_enum: IndicatorType,
    pipeline_id: Optional[str] = None,
) -> Optional[str]:
    session = get_session()
    try:
        feed_query = select(HostedFeed).where(HostedFeed.indicator_type == indicator_enum)
        if pipeline_id:
            feed_query = feed_query.where(HostedFeed.pipeline_id == pipeline_id)
        feed = session.exec(feed_query.order_by(HostedFeed.updated_at.desc())).first()
        if feed:
            return feed.run_id

        run_query = select(PipelineRun.id).order_by(PipelineRun.queued_at.desc())
        if pipeline_id:
            run_query = run_query.where(PipelineRun.pipeline_id == pipeline_id)
        return session.exec(run_query).first()
    finally:
        session.close()


def _ensure_profile_exists(session, profile_id: str) -> None:
    profile = session.get(Profile, profile_id)
    if not profile:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")


def _enqueue_pipeline_run(
    *,
    pipeline: Pipeline,
    config: ProfileConfig,
    overrides: Dict[str, Any],
    requested_by: Optional[str],
    idempotency_key: Optional[str],
    background_tasks: Optional[BackgroundTasks],
) -> RunSubmissionResponse:
    settings = dict(config.pipeline_settings or {})
    settings.update(overrides)
    mode = settings.get("mode", "validate")

    metadata = {
        "requested_overrides": overrides,
        "pipeline_settings": settings,
    }
    if requested_by:
        metadata["requested_by"] = requested_by

    run_id = create_pipeline_run(
        pipeline_id=pipeline.id,
        mode=mode,
        metadata=metadata,
        idempotency_key=idempotency_key,
        created_by=requested_by,
    )

    job_id = str(uuid4())
    job_request_payload = {
        "overrides": overrides,
        "pipeline_id": pipeline.id,
        "profile_id": pipeline.profile_id,
        "profile_config_id": pipeline.profile_config_id,
    }
    job_store.create_job(
        job_id=job_id,
        request=job_request_payload,
        pipeline_id=pipeline.id,
        profile_id=pipeline.profile_id,
        profile_config_id=pipeline.profile_config_id,
    )
    job_store.update(job_id, run_id=run_id, detail="Queued pipeline run")

    if background_tasks is not None:
        background_tasks.add_task(_execute_pipeline_job, job_id)
    else:
        Thread(target=_execute_pipeline_job, args=(job_id,), daemon=True).start()

    return RunSubmissionResponse(
        job_id=job_id,
        run_id=run_id,
        pipeline_id=pipeline.id,
        profile_id=pipeline.profile_id,
        profile_config_id=pipeline.profile_config_id,
        state=RunState.QUEUED,
        detail=f"Pipeline run accepted. Track status via GET /runs/{run_id}",
    )


async def _safe_json(request: Request) -> Dict[str, Any]:
    try:
        data = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body must be valid JSON.",
        ) from exc
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="JSON body must be an object.",
        )
    return data


def _extract_payload(body: Dict[str, Any], model: Type[PayloadModel]) -> PayloadModel:
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid JSON payload.",
        )

    if "payload" in body and isinstance(body["payload"], dict):
        candidate = body["payload"]
    elif "request" in body and isinstance(body["request"], dict):
        candidate = body["request"]
    else:
        candidate = body

    try:
        return model.model_validate(candidate)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        ) from exc


def _serialize_profile(profile: Profile) -> ProfileResponse:
    return ProfileResponse(
        id=profile.id,
        name=profile.name,
        description=profile.description,
        created_at=profile.created_at,
        updated_at=profile.updated_at,
        latest_config_version=None,
    )


def _serialize_profile_config(config: ProfileConfig) -> ProfileConfigResponse:
    return ProfileConfigResponse(
        id=config.id,
        profile_id=config.profile_id,
        version=config.version,
        config_hash=config.config_hash,
        created_at=config.created_at,
        created_by=config.created_by,
        pipeline_settings=config.pipeline_settings,
        refresh_interval_minutes=config.refresh_interval_minutes,
        has_rules=bool(config.rules_yaml),
    )


def _serialize_admin_account(account: AdminAccount) -> AdminAccountResponse:
    return AdminAccountResponse(
        id=account.id,
        name=account.name,
        role=account.role,
        is_super_admin=account.is_super_admin,
        created_at=account.created_at,
        updated_at=account.updated_at,
    )


def _serialize_pipeline(pipeline: Pipeline) -> PipelineResponse:
    return PipelineResponse(
        id=pipeline.id,
        profile_id=pipeline.profile_id,
        profile_config_id=pipeline.profile_config_id,
        name=pipeline.name,
        description=pipeline.description,
        concurrency_limit=pipeline.concurrency_limit,
        is_active=pipeline.is_active,
        created_at=pipeline.created_at,
        updated_at=pipeline.updated_at,
    )


def _serialize_run_summary(run: PipelineRun) -> PipelineRunSummary:
    return PipelineRunSummary(
        run_id=run.id,
        pipeline_id=run.pipeline_id,
        profile_id=run.profile_id,
        profile_config_id=run.profile_config_id,
        mode=run.mode,
        state=run.state,
        sub_state=run.sub_state,
        percent_complete=run.percent_complete,
        queued_at=run.queued_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
        total_fetched=run.total_fetched,
        total_ingested=run.total_ingested,
        total_valid=run.total_valid,
        total_invalid=run.total_invalid,
        total_augmented=run.total_augmented,
    )


def _serialize_run_detail(
    run: PipelineRun,
    artifacts: List[Artifact],
    errors: List[RunError],
) -> PipelineRunDetail:
    return PipelineRunDetail(
        **_serialize_run_summary(run).model_dump(),
        metadata_snapshot=run.metadata_snapshot or {},
        artifacts=[_artifact_to_response(a) for a in artifacts],
        errors=[_error_to_response(e) for e in errors],
    )


def _artifact_to_response(artifact: Artifact) -> ArtifactResponse:
    return ArtifactResponse(
        id=artifact.id,
        run_id=artifact.run_id,
        artifact_type=artifact.artifact_type,
        location=artifact.location,
        status=artifact.status,
        checksum=artifact.checksum,
        size_bytes=artifact.size_bytes,
        created_at=artifact.created_at,
        updated_at=artifact.updated_at,
    )


def _error_to_response(error: RunError) -> RunErrorResponse:
    return RunErrorResponse(
        id=error.id,
        phase=error.phase,
        source=error.source,
        message=error.message,
        detail=error.detail,
        created_at=error.created_at,
    )


def _get_run_summary(run_id: str) -> Optional[PipelineRunSummary]:
    session = get_session()
    try:
        run = session.get(PipelineRun, run_id)
        if not run:
            return None
        return _serialize_run_summary(run)
    finally:
        session.close()


def _auto_refresh_tick() -> None:
    now = datetime.utcnow()
    session = get_session()
    try:
        candidates = session.exec(
            select(Pipeline, ProfileConfig)
            .join(ProfileConfig, Pipeline.profile_config_id == ProfileConfig.id)
            .where(
                Pipeline.is_active.is_(True),
                Pipeline.deleted_at.is_(None),
                ProfileConfig.refresh_interval_minutes.isnot(None),
                ProfileConfig.refresh_interval_minutes > 0,
            )
        ).all()

        for pipeline, config in candidates:
            interval = config.refresh_interval_minutes
            if not interval or interval <= 0:
                continue

            if job_store.has_active_job_for_pipeline(pipeline.id):
                continue

            active_run = session.exec(
                select(PipelineRun.id).where(
                    PipelineRun.pipeline_id == pipeline.id,
                    PipelineRun.state.in_([RunState.QUEUED, RunState.RUNNING]),
                )
            ).first()
            if active_run:
                continue

            last_run = session.exec(
                select(PipelineRun)
                .where(PipelineRun.pipeline_id == pipeline.id)
                .order_by(
                    PipelineRun.completed_at.desc(),
                    PipelineRun.started_at.desc(),
                    PipelineRun.queued_at.desc(),
                )
                .limit(1)
            ).first()

            reference = None
            if last_run:
                reference = last_run.completed_at or last_run.started_at or last_run.queued_at
            if not reference:
                reference = pipeline.updated_at or pipeline.created_at
            if not reference:
                reference = now

            next_run_at = reference + timedelta(minutes=interval)
            if next_run_at > now:
                continue

            api_logger.info(
                "Scheduler queuing pipeline %s for refresh (due %s)",
                pipeline.id,
                next_run_at.isoformat(),
            )
            _enqueue_pipeline_run(
                pipeline=pipeline,
                config=config,
                overrides={},
                requested_by="scheduler",
                idempotency_key=None,
                background_tasks=None,
            )
    except Exception:  # noqa: BLE001
        api_logger.exception("Refresh scheduler tick failed")
    finally:
        session.close()


__all__ = ["app"]

