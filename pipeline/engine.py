# engine.py
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import yaml

from pipeline.ingestor import EDLIngestor
from models.ingestion_model import EDLIngestionService
from models.schemas import EntryType, FetchedEntry, IngestedEntry, ValidatedEntry
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
    manual: List[ValidatedEntry] = field(default_factory=list)


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

    @staticmethod
    def parse_rules_yaml(content: Optional[str], logger) -> Dict[str, List[Dict[str, Any]]]:
        rules: Dict[str, List[Dict[str, Any]]] = {
            "manual_assertions": [],
            "exclusions": [],
        }
        if not content:
            return rules

        data = yaml.safe_load(content) or {}
        if not isinstance(data, dict):
            raise ValueError("Invalid rules YAML payload")

        manual_entries = []
        for raw in data.get("manual_assertions", []):
            if isinstance(raw, str):
                manual_entries.append({"value": raw})
            elif isinstance(raw, dict):
                if "value" not in raw:
                    raise ValueError("Manual assertion entry missing 'value'")
                manual_entries.append(
                    {
                        "value": str(raw["value"]),
                        "type": raw.get("type"),
                        "metadata": dict(raw.get("metadata") or {}),
                    }
                )
            else:
                raise ValueError("Manual assertion entries must be strings or objects with a 'value'")

        exclusion_entries = []
        for raw in data.get("exclusions", []):
            if isinstance(raw, str):
                exclusion_entries.append({"value": raw})
            elif isinstance(raw, dict):
                if "value" not in raw:
                    raise ValueError("Exclusion entry missing 'value'")
                exclusion_entries.append({"value": str(raw["value"])})
            else:
                raise ValueError("Exclusion entries must be strings or objects with a 'value'")

        rules["manual_assertions"] = manual_entries
        rules["exclusions"] = exclusion_entries
        logger.info(
            "Loaded rules configuration (%d manual assertions, %d exclusions)",
            len(manual_entries),
            len(exclusion_entries),
        )
        return rules


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
        rules: Optional[Dict[str, List[Dict[str, Any]]]] = None,
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
        rules_config = rules or {"manual_assertions": [], "exclusions": []}
        self.manual_assertions: List[Dict[str, Any]] = []
        for entry in rules_config.get("manual_assertions", []):
            value = entry.get("value") if isinstance(entry, dict) else None
            if value is None:
                continue
            manual_entry = {
                "value": str(value),
                "entry_type": self._coerce_entry_type(entry.get("type") if isinstance(entry, dict) else None),
                "metadata": dict(entry.get("metadata") or {}) if isinstance(entry, dict) else {},
            }
            self.manual_assertions.append(manual_entry)
        self.exclusion_values = {
            self._normalize_value(exclusion.get("value"))
            for exclusion in rules_config.get("exclusions", [])
            if isinstance(exclusion, dict) and exclusion.get("value")
        }
        self.manual_entries_added: List[ValidatedEntry] = []
        self.manual_entries_skipped_due_to_exclusion = 0
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

    @staticmethod
    def _normalize_value(value: Optional[str]) -> str:
        return value.strip().lower() if value else ""

    @staticmethod
    def _coerce_entry_type(raw: Optional[str]) -> EntryType:
        if not raw:
            return EntryType.UNKNOWN
        candidate = str(raw).lower()
        try:
            return EntryType(candidate)
        except ValueError:
            return EntryType.UNKNOWN

    async def run(self) -> PipelineExecutionResult:
        self.logger.info("Starting pipeline run in '%s' mode...", self.mode)
        self.logger.info(
            "Pipeline persistence flag is %s", "ENABLED" if self.persist_to_db else "DISABLED"
        )

        self._transition(RunState.RUNNING, "fetch", 0.0, started_at=datetime.utcnow())

        # Step 1: Fetch
        with log_stage(self.logger, "fetch"):
            fetched: List[FetchedEntry] = await self.fetcher.run()

        if self.exclusion_values:
            filtered_fetched = [
                entry
                for entry in fetched
                if self._normalize_value(entry.raw) not in self.exclusion_values
            ]
            removed_fetch = len(fetched) - len(filtered_fetched)
            if removed_fetch:
                self.logger.info("Exclusion rules removed %d fetched entries", removed_fetch)
            fetched = filtered_fetched

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

        if self.exclusion_values and validated:
            filtered_validated = [
                entry
                for entry in validated
                if self._normalize_value(entry.normalized) not in self.exclusion_values
            ]
            removed_validated = len(validated) - len(filtered_validated)
            if removed_validated:
                self.logger.info("Exclusion rules removed %d validated entries", removed_validated)
            validated = filtered_validated

        manual_validated_entries: List[ValidatedEntry] = []
        if self.manual_assertions:
            for entry in self.manual_assertions:
                raw_value = entry["value"]
                if self._normalize_value(raw_value) in self.exclusion_values:
                    self.manual_entries_skipped_due_to_exclusion += 1
                    self.logger.info("Manual assertion '%s' skipped due to exclusion rule", raw_value)
                    continue
                meta = dict(entry.get("metadata") or {})
                source_label = meta.pop("source", "manual")
                meta.setdefault("manual", True)
                manual_validated_entries.append(
                    ValidatedEntry(
                        source=source_label,
                        original=raw_value,
                        entry_type=entry["entry_type"],
                        error=None,
                        normalized=raw_value,
                        meta=meta,
                    )
                )
            if manual_validated_entries:
                self.logger.info("Prepared %d manual assertions from rules.yaml", len(manual_validated_entries))
        self.manual_entries_added = manual_validated_entries

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

        if manual_validated_entries:
            validated.extend(manual_validated_entries)

        self._transition(RunState.RUNNING, "finalize", 90.0)

        return PipelineExecutionResult(
            fetched=fetched,
            ingested=ingested,
            validated=validated,
            augmented=augmented,
            manual=manual_validated_entries,
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
    rules: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Optional[str]:
    """Run the pipeline synchronously."""

    logger = get_logger("engine.runner", log_level, "engine.log")

    metadata = {"sources": [dict(src) for src in sources]}
    if proxy and use_proxy:
        metadata["proxy"] = proxy
    metadata["proxy_enabled"] = use_proxy
    if rules:
        metadata["rules_configured"] = {
            "manual_assertions": len(rules.get("manual_assertions", [])),
            "exclusions": len(rules.get("exclusions", [])),
        }

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
        rules=rules,
    )

    execution: PipelineExecutionResult
    try:
        execution = asyncio.run(orchestrator.run())
    except Exception as exc:  # noqa: BLE001
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

    manual_for_output = execution.manual or []
    if manual_for_output:
        logger.info("Rules contributed %d manual assertions", len(manual_for_output))
    if orchestrator.manual_entries_skipped_due_to_exclusion:
        logger.info(
            "Rules skipped %d manual assertions via exclusions",
            orchestrator.manual_entries_skipped_due_to_exclusion,
        )

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if execution.augmented is not None and mode == "augment":
        output_entries: List[Union[AugmentedEntry, ValidatedEntry]] = list(execution.augmented)
        if manual_for_output:
            for manual_entry in manual_for_output:
                output_entries.append(
                    AugmentedEntry(
                        **manual_entry.model_dump(),
                        augmented=manual_entry.normalized,
                        changes=["manual_assertion"],
                    )
                )
    else:
        output_entries = execution.validated

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([r.model_dump() for r in output_entries], f, indent=2, ensure_ascii=False)

    logger.info("Pipeline results written to %s", out_path)

    rules_summary: Optional[Dict[str, Any]] = None
    if rules is not None:
        rules_summary = {
            "manual_assertions_configured": len(rules.get("manual_assertions", [])),
            "manual_assertions_applied": len(manual_for_output),
            "manual_assertions_skipped": orchestrator.manual_entries_skipped_due_to_exclusion,
            "exclusions_configured": len(rules.get("exclusions", [])),
        }

    if persist_to_db:
        try:
            finalize_metadata = {
                "output_path": str(out_path),
                "sources": metadata["sources"],
                "proxy_enabled": metadata["proxy_enabled"],
            }
            if proxy and use_proxy:
                finalize_metadata["proxy"] = proxy
            if rules_summary:
                finalize_metadata["rules"] = rules_summary
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
