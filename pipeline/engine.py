# engine.py
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import yaml

from pipeline.ingestor import EDLIngestor
from models.ingestion_model import EDLIngestionService
from models.schemas import FetchedEntry, IngestedEntry, ValidatedEntry
from models.validation_model import EDLValidator
from models.augmentation_model import EDL_Augmentor, AugmentedEntry
from utils.logger import get_logger, log_metric, log_stage
from utils.config_loader import load_config
from db.models import RunState
from db.persistence import finalize_pipeline_run, update_run_state


# ----------------------------------------------------------------------
# Structured execution payload
# ----------------------------------------------------------------------
@dataclass
class PipelineExecutionResult:
    fetched: List[FetchedEntry]
    ingested: List[IngestedEntry]
    validated: List[ValidatedEntry]
    augmented: Optional[List[AugmentedEntry]]


# ----------------------------------------------------------------------
# Config Manager
# ----------------------------------------------------------------------
class EngineConfig:
    @staticmethod
    def load_sources(path: str, logger) -> List[Dict[str, str]]:
        cfg = load_config(path, logger)
        sources = cfg.get("sources", [])
        if not isinstance(sources, list):
            raise ValueError(f"Invalid sources.yaml at {path}")
        logger.info("Loaded %d sources from %s", len(sources), path)
        return sources

    @staticmethod
    def load_augmentor(path: str, logger) -> Dict:
        cfg = load_config(path, logger)
        logger.info("Loaded augmentor configuration from %s", path)
        return cfg

    @staticmethod
    def parse_sources_yaml(content: str, logger) -> List[Dict[str, str]]:
        data = yaml.safe_load(content) or {}
        sources = data.get("sources", []) if isinstance(data, dict) else data
        if not isinstance(sources, list):
            raise ValueError("Invalid sources YAML payload")
        logger.info("Loaded %d sources from inline payload", len(sources))
        return [dict(src) for src in sources]

    @staticmethod
    def parse_augmentor_yaml(content: Optional[str], logger) -> Dict:
        if not content:
            return {}
        data = yaml.safe_load(content) or {}
        if not isinstance(data, dict):
            raise ValueError("Invalid augmentor YAML payload")
        logger.info("Loaded augmentor configuration from inline payload")
        return data


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------
class Orchestrator:
    """
    Orchestrates the pipeline:
      fetch -> ingest -> validate -> (optional augmentation) -> json output
    """

    def __init__(
        self,
        sources: List[Dict[str, str]],
        mode: str = "validate",
        augmentor_cfg: Optional[Dict] = None,
        timeout: int = 15,
        log_level: str = "INFO",
        persist_to_db: bool = False,
        proxy: Optional[str] = None,
        use_proxy: bool = False,
        run_id: Optional[str] = None,
        profile_id: Optional[str] = None,
        manual_entries: Optional[List[Dict[str, Any]]] = None,
    ):
        self.logger = get_logger("engine.orchestrator", log_level, "engine.log")
        self.sources_cfg = [dict(src) for src in sources]
        self.fetcher = EDLIngestor(
            sources=self.sources_cfg,
            timeout=timeout,
            log_level=log_level,
            proxy=proxy,
            use_proxy=use_proxy,
        )
        self.persist_to_db = persist_to_db
        self.ingestor = EDLIngestionService(log_level=log_level)
        self.validator = EDLValidator(log_level=log_level)
        self.augmentor_cfg = augmentor_cfg or {}
        self.mode = mode
        self.log_level = log_level
        self.profile_id = profile_id
        self.run_id = run_id
        self.last_run_id: Optional[str] = run_id
        self.manual_entries = manual_entries or []
        self.manual_entries_count: int = 0
        if proxy and use_proxy:
            self.logger.info("Proxy enabled for fetch stage: %s", proxy)
        elif use_proxy and not proxy:
            self.logger.info("Proxy enabled via environment configuration.")
        elif not use_proxy and proxy:
            self.logger.debug("Proxy value supplied but disabled via flag.")

    def _transition(self, state: RunState, sub_state: Optional[str], percent: float, *, started_at: Optional[datetime] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        if not self.run_id:
            return
        update_run_state(
            self.run_id,
            state=state,
            sub_state=sub_state,
            percent_complete=percent,
            started_at=started_at,
            metadata=metadata,
        )

    async def run(self) -> PipelineExecutionResult:
        self.logger.info("Starting pipeline run in '%s' mode...", self.mode)
        self.logger.info(
            "Pipeline persistence flag is %s", "ENABLED" if self.persist_to_db else "DISABLED"
        )

        self._transition(RunState.RUNNING, "fetch", 0.0, started_at=datetime.utcnow())

        # Step 1: Fetch
        with log_stage(self.logger, "fetch"):
            fetched: List[FetchedEntry] = await self.fetcher.run()

        if self.manual_entries:
            manual_results: List[FetchedEntry] = []
            for index, item in enumerate(self.manual_entries, start=1):
                raw_value = str(item.get("value", "")).strip()
                if not raw_value:
                    continue
                metadata = dict(item.get("metadata") or {})
                metadata.setdefault("source_type", "manual")
                metadata.setdefault("manual", True)
                if "submitted_at" in item:
                    metadata.setdefault("submitted_at", item["submitted_at"])
                if "submitted_by" in item:
                    metadata.setdefault("submitted_by", item["submitted_by"])
                declared_type = item.get("type")
                if declared_type:
                    metadata.setdefault("declared_type", declared_type)
                manual_results.append(
                    FetchedEntry(
                        source=item.get("source") or "manual",
                        raw=raw_value,
                        line_number=index,
                        metadata=metadata,
                    )
                )
            if manual_results:
                fetched.extend(manual_results)
                self.manual_entries_count = len(manual_results)
                self.logger.info("Appended %d manual entries to fetched set.", len(manual_results))
                log_metric(self.logger, "manual_entries_fetched_total", len(manual_results), stage="fetch", source="manual")
            else:
                self.logger.debug("Manual submission contained only empty values; nothing appended.")

        if not fetched:
            self.logger.warning("No entries fetched from configured sources.")

        log_metric(self.logger, "engine_fetch_total", len(fetched), stage="fetch")
        self._transition(RunState.RUNNING, "ingest", 25.0)

        # Step 2: Ingest
        with log_stage(self.logger, "ingest"):
            ingested: List[IngestedEntry] = self.ingestor.ingest(fetched)
        if not ingested:
            self.logger.warning("No entries ingested after processing fetched data.")

        log_metric(self.logger, "engine_ingested_total", len(ingested), stage="ingest")
        self._transition(RunState.RUNNING, "validate", 55.0)

        # Step 3: Validate
        with log_stage(self.logger, "validate"):
            validated: List[ValidatedEntry] = self.validator.validate_entries(ingested)
        if not validated:
            self.logger.warning("Validation produced no entries.")

        log_metric(self.logger, "engine_validated_total", len(validated), stage="validate")

        augmented: Optional[List[AugmentedEntry]] = None
        if self.mode == "augment":
            self._transition(RunState.RUNNING, "augment", 75.0)
            with log_stage(self.logger, "augment"):
                augmentor = EDL_Augmentor(cfg=self.augmentor_cfg, log_level=self.log_level)
                augmented = augmentor.augment_entries(validated)
                self.logger.info("Augmentation complete: %d entries processed", len(augmented))
        else:
            self.logger.info("Validation-only mode complete: %d entries processed", len(validated))

        self._transition(RunState.RUNNING, "finalize", 90.0)

        return PipelineExecutionResult(
            fetched=fetched,
            ingested=ingested,
            validated=validated,
            augmented=augmented,
        )


# ----------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------
def run_pipeline(
    sources: List[Dict[str, str]],
    output: str,
    mode: str = "validate",
    augmentor_cfg: Optional[Dict] = None,
    timeout: int = 15,
    log_level: str = "INFO",
    persist_to_db: bool = False,
    proxy: Optional[str] = None,
    use_proxy: bool = False,
    profile_id: Optional[str] = None,
    profile_config_id: Optional[str] = None,
    run_id: Optional[str] = None,
    manual_submission: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Run the pipeline synchronously."""

    metadata = {"sources": [dict(src) for src in sources]}
    if proxy and use_proxy:
        metadata["proxy"] = proxy
    metadata["proxy_enabled"] = use_proxy
    manual_entries = None
    manual_summary: Optional[Dict[str, Any]] = None
    if manual_submission:
        manual_entries = manual_submission.get("entries") or []
        manual_summary = {
            "entry_count": len(manual_entries),
            "source": manual_submission.get("source"),
            "notes": manual_submission.get("notes"),
            "submitted_at": manual_submission.get("submitted_at"),
            "submitted_by": manual_submission.get("submitted_by"),
        }
        metadata["manual_submission"] = manual_summary

    if persist_to_db and run_id is None:
        raise ValueError("run_id must be provided when persist_to_db is True")

    orchestrator = Orchestrator(
        sources=sources,
        mode=mode,
        augmentor_cfg=augmentor_cfg,
        timeout=timeout,
        log_level=log_level,
        persist_to_db=persist_to_db,
        proxy=proxy,
        use_proxy=use_proxy,
        run_id=run_id,
        profile_id=profile_id,
        manual_entries=manual_entries,
    )

    execution: PipelineExecutionResult
    try:
        execution = asyncio.run(orchestrator.run())
    except Exception as exc:  # noqa: BLE001
        logger = get_logger("engine.runner", log_level, "engine.log")
        logger.exception("Pipeline execution failed")
        if persist_to_db and run_id:
            update_run_state(
                run_id,
                state=RunState.FAILED,
                sub_state="error",
                percent_complete=100.0,
                metadata={"exception": str(exc)},
            )
        raise

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if execution.augmented is not None and mode == "augment":
        output_entries: Sequence[Union[AugmentedEntry, ValidatedEntry]] = execution.augmented
    else:
        output_entries = execution.validated

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([r.model_dump() for r in output_entries], f, indent=2, ensure_ascii=False)

    logger = get_logger("engine.runner", log_level, "engine.log")
    logger.info("Pipeline results written to %s", out_path)

    if persist_to_db:
        try:
            finalize_metadata = {"output_path": str(out_path)}
            if manual_summary:
                finalize_metadata["manual_submission"] = manual_summary
            finalize_pipeline_run(
                run_id=run_id,
                mode=mode,
                sources=sources,
                fetched=execution.fetched,
                ingested=execution.ingested,
                validated=execution.validated,
                augmented=execution.augmented,
                metadata=finalize_metadata,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to finalize pipeline run in database")
            if run_id:
                update_run_state(
                    run_id,
                    state=RunState.FAILED,
                    sub_state="finalize",
                    percent_complete=100.0,
                    metadata={"exception": str(exc)},
                )
            raise

    return run_id or orchestrator.last_run_id
