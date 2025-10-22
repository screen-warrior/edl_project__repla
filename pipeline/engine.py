# engine.py
from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
from collections import Counter
from typing import Any, Dict, List, Optional, Union

from pipeline.ingestor import EDLIngestor
from models.ingestion_model import EDLIngestionService
from models.schemas import FetchedEntry, IngestedEntry, ValidatedEntry
from models.validation_model import EDLValidator
from models.augmentation_model import EDL_Augmentor, AugmentedEntry   # <- new augmentor
from utils.logger import get_logger, log_metric, log_stage
from utils.config_loader import load_config


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


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------
class Orchestrator:
    """
    Orchestrates the pipeline:
      fetch -> ingest -> validate -> (optional augmentation) -> json output
    """

    def __init__(self, sources: List[Dict[str, str]], mode: str = "validate",
                 augmentor_cfg: Optional[Dict] = None, timeout: int = 15,
                 log_level: str = "INFO", persist_to_db: bool = False,
                 profile_id: Optional[str] = None, proxy: Optional[Union[str, Dict[str, str]]] = None):
        self.logger = get_logger("engine.orchestrator", log_level, "engine.log")
        self.sources_cfg = [dict(src) for src in sources]
        proxy = proxy or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
        self.fetcher = EDLIngestor(sources=self.sources_cfg, timeout=timeout, log_level=log_level, proxy=proxy)
        self.persist_to_db = persist_to_db
        self.ingestor = EDLIngestionService(log_level=log_level)
        self.validator = EDLValidator(log_level=log_level)
        self.augmentor_cfg = augmentor_cfg or {}
        self.mode = mode
        self.log_level = log_level
        self.profile_id = profile_id
        self.proxy = proxy
        self.last_run_id: Optional[str] = None

    async def run(self) -> Union[List[ValidatedEntry], List[AugmentedEntry]]:
        self.logger.info("Starting pipeline run in '%s' mode...", self.mode)
        self.logger.info(
            "Pipeline persistence flag is %s", "ENABLED" if self.persist_to_db else "DISABLED"
        )

        # Step 1: Fetch
        with log_stage(self.logger, "fetch"):
            fetched: List[FetchedEntry] = await self.fetcher.run()
        if not fetched:
            self.logger.warning("No entries fetched; exiting early.")
            return []

        log_metric(self.logger, "engine_fetch_total", len(fetched), stage="fetch")

        # Step 2: Ingest
        with log_stage(self.logger, "ingest"):
            ingested: List[IngestedEntry] = self.ingestor.ingest(fetched)
        if not ingested:
            self.logger.warning("No entries ingested; exiting early.")
            return []

        log_metric(self.logger, "engine_ingested_total", len(ingested), stage="ingest")

        # Step 3: Validate
        with log_stage(self.logger, "validate"):
            validated: List[ValidatedEntry] = self.validator.validate_entries(ingested)
        if not validated:
            self.logger.warning("No validated entries; exiting early.")
            return []

        log_metric(self.logger, "engine_validated_total", len(validated), stage="validate")

        type_counter = Counter(
            getattr(entry.entry_type, "value", str(entry.entry_type)) for entry in validated
        )
        error_counter = Counter(
            entry.error.code for entry in validated if entry.error is not None
        )

        augmented: Optional[List[AugmentedEntry]] = None
        if self.mode == "augment":
            with log_stage(self.logger, "augment"):
                augmentor = EDL_Augmentor(cfg=self.augmentor_cfg, log_level=self.log_level)
                augmented = augmentor.augment_entries(validated)
                self.logger.info("Augmentation complete: %d entries processed", len(augmented))
            results: Union[List[ValidatedEntry], List[AugmentedEntry]] = augmented
        else:
            self.logger.info("Validation-only mode complete: %d entries processed", len(validated))
            results = validated

        if self.persist_to_db:
            try:
                from db.persistence import persist_pipeline_results

                metadata_snapshot: Dict[str, Any] = {
                    "log_level": self.log_level,
                    "source_summaries": [
                        {
                            "name": src.get("name"),
                            "type": src.get("type"),
                            "location": src.get("location"),
                        }
                        for src in self.sources_cfg
                    ],
                    "entry_type_counts": dict(type_counter),
                    "validation_error_counts": dict(error_counter),
                }

                self.logger.info(
                    "Persisting pipeline run | mode=%s fetched=%d ingested=%d validated=%d augmented=%d",
                    self.mode,
                    len(fetched),
                    len(ingested),
                    len(validated),
                    len(augmented) if augmented else 0,
                )
                self.logger.debug("Pipeline persistence metadata: %s", metadata_snapshot)

                run_id = persist_pipeline_results(
                    mode=self.mode,
                    sources=self.sources_cfg,
                    fetched=fetched,
                    ingested=ingested,
                    validated=validated,
                    augmented=augmented,
                    profile_id=self.profile_id,
                    run_metadata=metadata_snapshot,
                )
                self.logger.info("Persisted pipeline run %s to database", run_id)
                self.last_run_id = run_id
            except Exception:
                self.logger.exception("Failed to persist pipeline run to database")

        return results


# ----------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------
def run_pipeline(sources: List[Dict[str, str]], output: str,
                 mode: str = "validate", augmentor_cfg: Optional[Dict] = None,
                 timeout: int = 15, log_level: str = "INFO",
                 persist_to_db: bool = False, profile_id: Optional[str] = None,
                 proxy: Optional[Union[str, Dict[str, str]]] = None) -> Optional[str]:
    """
    Convenience wrapper for CLI entrypoint.
    Saves either validated or augmented results depending on mode.
    """
    orchestrator = Orchestrator(
        sources=sources, mode=mode, augmentor_cfg=augmentor_cfg,
        timeout=timeout, log_level=log_level, persist_to_db=persist_to_db,
        profile_id=profile_id, proxy=proxy
    )

    results = asyncio.run(orchestrator.run())
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([r.model_dump() for r in results], f, indent=2, ensure_ascii=False)

    logger = get_logger("engine.runner", log_level, "engine.log")
    logger.info("Pipeline results written to %s", out_path)
    return getattr(orchestrator, "last_run_id", None)
