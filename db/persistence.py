"""
Helpers for working with the relational database.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Optional, Sequence, Tuple

from sqlalchemy import func
from sqlmodel import select

from models.augmentation_model import AugmentedEntry
from models.schemas import FetchedEntry, IngestedEntry, ValidatedEntry

from db.models import (
    Artifact,
    ArtifactStatus,
    ArtifactType,
    AugmentedIndicator,
    Feed,
    Indicator,
    IndicatorType,
    PipelineRun,
    Profile,
    ProfileConfig,
    RunError,
    RunState,
)
from db.session import session_scope

logger = logging.getLogger("db.persistence")


# ---------------------------------------------------------------------------
# Profile helpers
# ---------------------------------------------------------------------------
def create_profile(*, name: str, description: Optional[str] = None) -> Profile:
    with session_scope() as session:
        profile = Profile(name=name, description=description)
        session.add(profile)
        session.flush()
        session.refresh(profile)
        return profile


def create_profile_config(
    *,
    profile_id: str,
    sources_yaml: str,
    augment_yaml: Optional[str] = None,
    pipeline_settings: Optional[Dict[str, Any]] = None,
    created_by: Optional[str] = None,
) -> ProfileConfig:
    payload = {
        "sources": sources_yaml or "",
        "augment": augment_yaml or "",
        "settings": pipeline_settings or {},
    }
    hash_input = json.dumps(payload, sort_keys=True).encode("utf-8")
    config_hash = hashlib.sha256(hash_input).hexdigest()

    with session_scope() as session:
        profile = session.get(Profile, profile_id)
        if not profile:
            raise ValueError(f"Profile {profile_id} does not exist")

        max_version = session.exec(
            select(func.max(ProfileConfig.version)).where(ProfileConfig.profile_id == profile_id)
        ).one()
        next_version = (max_version or 0) + 1

        profile_config = ProfileConfig(
            profile_id=profile_id,
            version=next_version,
            config_hash=config_hash,
            sources_yaml=sources_yaml,
            augment_yaml=augment_yaml,
            pipeline_settings=pipeline_settings or {},
            created_by=created_by,
        )
        session.add(profile_config)
        session.flush()
        session.refresh(profile_config)
        profile.updated_at = datetime.utcnow()
        return profile_config


# ---------------------------------------------------------------------------
# Run helpers
# ---------------------------------------------------------------------------
def create_pipeline_run_record(
    *,
    profile_id: Optional[str],
    profile_config_id: Optional[str],
    mode: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    with session_scope() as session:
        run = PipelineRun(
            profile_id=profile_id,
            profile_config_id=profile_config_id,
            mode=mode,
            metadata_snapshot=metadata or {},
        )
        session.add(run)
        session.flush()
        return run.id


def update_run_state(
    run_id: str,
    *,
    state: RunState,
    sub_state: Optional[str] = None,
    percent_complete: Optional[float] = None,
    started_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    with session_scope() as session:
        run = session.get(PipelineRun, run_id)
        if not run:
            raise ValueError(f"Pipeline run {run_id} not found")

        run.state = state
        if sub_state is not None:
            run.sub_state = sub_state
        if percent_complete is not None:
            run.percent_complete = percent_complete
        if started_at is not None and run.started_at is None:
            run.started_at = started_at
        if metadata:
            snapshot = dict(run.metadata_snapshot or {})
            snapshot.update(metadata)
            run.metadata_snapshot = snapshot
        if state in {RunState.SUCCESS, RunState.PARTIAL_SUCCESS, RunState.FAILED, RunState.CANCELLED}:
            run.completed_at = datetime.utcnow()
        session.add(run)


def record_run_error(
    *,
    run_id: str,
    phase: Optional[str],
    source: Optional[str],
    message: str,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    with session_scope() as session:
        error = RunError(
            run_id=run_id,
            phase=phase,
            source=source,
            message=message,
            detail=detail,
        )
        session.add(error)


# ---------------------------------------------------------------------------
# Finalise run results
# ---------------------------------------------------------------------------
def finalize_pipeline_run(
    *,
    run_id: Optional[str],
    mode: str,
    sources: Sequence[Dict[str, Any]],
    fetched: Sequence[FetchedEntry],
    ingested: Sequence[IngestedEntry],
    validated: Sequence[ValidatedEntry],
    augmented: Optional[Sequence[AugmentedEntry]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    valid_count = sum(1 for entry in validated if entry.valid)
    invalid_count = len(validated) - valid_count
    augmented_count = len(augmented or [])

    type_breakdown = Counter(
        _coerce_indicator_type(entry.entry_type).value for entry in validated
    )
    error_breakdown = Counter(
        entry.error.code for entry in validated if entry.error is not None
    )

    metadata_snapshot: Dict[str, Any] = {
        "source_count": len(sources),
        "entry_type_counts": dict(type_breakdown),
        "validation_error_counts": dict(error_breakdown),
    }
    if metadata:
        metadata_snapshot.update(metadata)

    source_lookup = {src.get("name"): src for src in sources if src.get("name")}

    with session_scope() as session:
        if run_id:
            run = session.get(PipelineRun, run_id)
        else:
            run = PipelineRun(mode=mode, metadata_snapshot={})
            session.add(run)
            session.flush()
            run_id = run.id

        if not run:
            raise ValueError(f"Pipeline run {run_id} not found")

        run.metadata_snapshot = {**(run.metadata_snapshot or {}), **metadata_snapshot}
        run.total_fetched = len(fetched)
        run.total_ingested = len(ingested)
        run.total_valid = valid_count
        run.total_invalid = invalid_count
        run.total_augmented = augmented_count
        run.state = RunState.SUCCESS if invalid_count == 0 else RunState.PARTIAL_SUCCESS
        run.completed_at = datetime.utcnow()
        run.percent_complete = 100.0
        run.sub_state = None

        feed_cache: Dict[str, Feed] = {}

        def resolve_feed(feed_name: Optional[str]) -> Optional[Feed]:
            if not feed_name:
                return None
            if feed_name in feed_cache:
                return feed_cache[feed_name]

            record = session.exec(select(Feed).where(Feed.name == feed_name)).one_or_none()
            if record:
                feed_cache[feed_name] = record
                return record

            cfg = source_lookup.get(feed_name, {})
            record = Feed(
                name=feed_name,
                source_url=cfg.get("location"),
                source_type=cfg.get("type"),
                description=cfg.get("description"),
            )
            session.add(record)
            session.flush()
            feed_cache[feed_name] = record
            return record

        indicator_lookup: Dict[Tuple[str, str, str], Indicator] = {}

        for entry in validated:
            entry_type = _coerce_indicator_type(entry.entry_type)
            key = (entry.source or "", entry.normalized, entry_type.value)
            if key in indicator_lookup:
                continue

            feed = resolve_feed(entry.source)
            indicator = Indicator(
                run_id=run.id,
                feed_id=feed.id if feed else None,
                source_name=entry.source or "",
                original=entry.original,
                normalized=entry.normalized,
                entry_type=entry_type,
                is_valid=entry.valid,
                error_code=entry.error.code if entry.error else None,
                error_message=entry.error.message if entry.error else None,
                error_hint=entry.error.hint if entry.error else None,
                meta=entry.meta or {},
            )
            session.add(indicator)
            session.flush()

            indicator_lookup[key] = indicator

        if augmented:
            for entry in augmented:
                key = (
                    entry.source or "",
                    entry.normalized,
                    _coerce_indicator_type(entry.entry_type).value,
                )
                indicator = indicator_lookup.get(key)
                if not indicator:
                    continue

                existing_aug = session.exec(
                    select(AugmentedIndicator).where(AugmentedIndicator.indicator_id == indicator.id)
                ).one_or_none()

                if existing_aug:
                    existing_aug.augmented_value = entry.augmented
                    existing_aug.changes = list(entry.changes)
                    existing_aug.created_at = datetime.utcnow()
                    session.add(existing_aug)
                else:
                    session.add(
                        AugmentedIndicator(
                            indicator_id=indicator.id,
                            augmented_value=entry.augmented,
                            changes=list(entry.changes),
                        )
                    )

        # Register artifacts for the run (logical URLs for the hosted feeds)
        existing_artifacts = {
            artifact.artifact_type: artifact
            for artifact in session.exec(select(Artifact).where(Artifact.run_id == run.id))
        }

        type_to_artifact = {
            IndicatorType.IPV4: ArtifactType.IPV4,
            IndicatorType.IPV6: ArtifactType.IPV6,
            IndicatorType.CIDR: ArtifactType.CIDR,
            IndicatorType.FQDN: ArtifactType.FQDN,
            IndicatorType.URL: ArtifactType.URL,
        }

        for indicator_type, artifact_type in type_to_artifact.items():
            if type_breakdown.get(indicator_type.value):
                location = f"/edl/{indicator_type.value}"
                artifact = existing_artifacts.get(artifact_type)
                if artifact:
                    artifact.location = location
                    artifact.status = ArtifactStatus.COMPLETE
                    artifact.updated_at = datetime.utcnow()
                else:
                    session.add(
                        Artifact(
                            run_id=run.id,
                            artifact_type=artifact_type,
                            location=location,
                            status=ArtifactStatus.COMPLETE,
                        )
                    )

        session.add(run)
        logger.info("DB persistence succeeded | run_id=%s state=%s", run.id, run.state.value)
        return run.id


def _coerce_indicator_type(entry_type: Any) -> IndicatorType:
    if isinstance(entry_type, IndicatorType):
        return entry_type
    value = getattr(entry_type, "value", entry_type)
    try:
        return IndicatorType(str(value))
    except ValueError:
        return IndicatorType.UNKNOWN


__all__ = [
    "create_profile",
    "create_profile_config",
    "create_pipeline_run_record",
    "update_run_state",
    "record_run_error",
    "finalize_pipeline_run",
]

