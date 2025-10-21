"""
Helpers for persisting pipeline results into the relational database.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Optional, Sequence, Tuple

from sqlmodel import Session, select

from models.augmentation_model import AugmentedEntry
from models.schemas import FetchedEntry, IngestedEntry, ValidatedEntry

from db.models import AugmentedIndicator, Feed, Indicator, IndicatorType, PipelineRun
from db.session import session_scope

logger = logging.getLogger("db.persistence")

def persist_pipeline_results(
    *,
    mode: str,
    sources: Sequence[Dict[str, Any]],
    fetched: Sequence[FetchedEntry],
    ingested: Sequence[IngestedEntry],
    validated: Sequence[ValidatedEntry],
    augmented: Optional[Sequence[AugmentedEntry]] = None,
    profile_id: Optional[str] = None,
    run_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Persist a pipeline execution into the database.

    Returns:
        The primary-key (UUID) of the recorded PipelineRun.
    """

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
    if profile_id:
        metadata_snapshot.setdefault("config_profile_id", profile_id)
    if run_metadata:
        metadata_snapshot.update(run_metadata)

    source_lookup = {src.get("name"): src for src in sources if src.get("name")}

    logger.info(
        "DB persistence request | mode=%s profile=%s fetched=%d ingested=%d validated=%d invalid=%d augmented=%d",
        mode,
        profile_id,
        len(fetched),
        len(ingested),
        len(validated),
        invalid_count,
        augmented_count,
    )

    with session_scope() as session:
        run = PipelineRun(
            mode=mode,
            profile_id=profile_id,
            total_fetched=len(fetched),
            total_ingested=len(ingested),
            total_valid=valid_count,
            total_invalid=invalid_count,
            total_augmented=augmented_count,
            metadata_snapshot=metadata_snapshot,
        )
        session.add(run)
        session.flush()  # Assign primary key

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

        run.completed_at = datetime.utcnow()
        session.add(run)

        # session_scope commits and closes
        logger.info("DB persistence succeeded | run_id=%s profile=%s", run.id, profile_id)
        return run.id


def _coerce_indicator_type(entry_type: Any) -> IndicatorType:
    if isinstance(entry_type, IndicatorType):
        return entry_type
    value = getattr(entry_type, "value", entry_type)
    try:
        return IndicatorType(str(value))
    except ValueError:
        return IndicatorType.UNKNOWN
