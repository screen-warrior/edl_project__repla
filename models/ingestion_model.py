#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingestion service for the EDL pipeline.

Responsibilities:
- Convert raw fetched lines into structured `IngestedEntry` models
- Apply lightweight normalization (trim whitespace, drop BOMs)
- Preserve inline comments as metadata for downstream stages
- Emit metrics detailing how many lines were processed/skipped
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from models.schemas import FetchedEntry, IngestedEntry
from utils.logger import get_logger, log_metric


def _strip_inline_comment(value: str) -> Tuple[str, Optional[str]]:
    """
    Extract inline comments denoted by ' #'.
    Mirrors behaviour expected by the validation layer, allowing us to
    surface the comment via metadata while keeping the base entry clean.
    """
    if "#" not in value:
        return value, None
    idx = value.find(" #")
    if idx == -1:
        return value, None
    return value[:idx].rstrip(), value[idx + 1 :].strip()


def _normalize_entry(value: str) -> str:
    """Trim surrounding whitespace and strip UTF-8 BOM if present."""
    normalized = value.strip()
    if normalized.startswith("\ufeff"):
        normalized = normalized.lstrip("\ufeff")
    return normalized


class EDLIngestionService:
    """
    Service that transforms raw fetched output into structured ingestion models.
    """

    def __init__(self, log_level: str = "INFO") -> None:
        self.logger = get_logger("ingestion.service", log_level, "ingestion.log")

    def ingest(self, fetched_entries: Iterable[FetchedEntry]) -> List[IngestedEntry]:
        """
        Convert a sequence of `FetchedEntry` instances into `IngestedEntry`.
        """
        results: List[IngestedEntry] = []
        total = 0
        skipped_empty = 0

        for record in fetched_entries:
            total += 1
            cleaned = _normalize_entry(record.raw)
            if not cleaned:
                skipped_empty += 1
                continue

            base_entry, comment = _strip_inline_comment(cleaned)
            metadata: Dict[str, Any] = dict(record.metadata)
            if comment:
                metadata["comment"] = comment
            if "raw" not in metadata:
                metadata["raw"] = record.raw

            ingested = IngestedEntry(
                source=record.source,
                entry=base_entry,
                line_number=record.line_number,
                metadata=metadata,
            )
            results.append(ingested)

        processed = len(results)
        self.logger.info(
            "Ingestion complete | processed=%s skipped_empty=%s", processed, skipped_empty
        )
        log_metric(self.logger, "entries_ingested_total", processed, stage="ingest")
        log_metric(self.logger, "entries_ingested_skipped_empty", skipped_empty, stage="ingest")
        return results

    def ingest_source(
        self,
        source: str,
        lines: Iterable[str],
        *,
        base_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[IngestedEntry]:
        """
        Helper to ingest raw lines for a single source without having to
        manufacture `FetchedEntry` objects (useful for tests).
        """
        metadata = base_metadata or {}
        fetched = [
            FetchedEntry(
                source=source,
                raw=line,
                line_number=index,
                metadata=metadata,
            )
            for index, line in enumerate(lines, start=1)
        ]
        return self.ingest(fetched)
