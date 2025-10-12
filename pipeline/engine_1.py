# engine.py
from __future__ import annotations
import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional, Union

from pipeline.fetcher_2 import EDLFetcher
from models.pydantic_models import IngestedEntry, EntryType
from models.validation_model import EDLValidator, ValidatedEntry
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
      fetch -> validation -> (optional augmentation) -> json output
    """

    def __init__(self, sources: List[Dict[str, str]], mode: str = "validate",
                 augmentor_cfg: Optional[Dict] = None, timeout: int = 15,
                 log_level: str = "INFO"):
        self.logger = get_logger("engine.orchestrator", log_level, "engine.log")
        self.fetcher = EDLFetcher(sources=sources, timeout=timeout, log_level=log_level)
        self.validator = EDLValidator(log_level=log_level)
        self.augmentor_cfg = augmentor_cfg or {}
        self.mode = mode
        self.log_level = log_level

    async def run(self) -> Union[List[ValidatedEntry], List[AugmentedEntry]]:
        self.logger.info("Starting pipeline run in '%s' mode...", self.mode)

        # Step 1: Fetch
        with log_stage(self.logger, "fetch"):
            ingested: List[IngestedEntry] = await self.fetcher.run()
        if not ingested:
            self.logger.warning("No entries fetched; exiting early.")
            return []

        # Step 2: Validate
        with log_stage(self.logger, "validate"):
            validated: List[ValidatedEntry] = self.validator.validate_entries(ingested)
        if not validated:
            self.logger.warning("No validated entries; exiting early.")
            return []

        # Step 3: Augment (if enabled)
        if self.mode == "augment":
            with log_stage(self.logger, "augment"):
                augmentor = EDL_Augmentor(cfg=self.augmentor_cfg, log_level=self.log_level)
                augmented: List[AugmentedEntry] = augmentor.augment_entries(validated)
                self.logger.info("Augmentation complete: %d entries processed", len(augmented))
                return augmented

        # Otherwise return validated only
        self.logger.info("Validation-only mode complete: %d entries processed", len(validated))
        return validated


# ----------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------
def run_pipeline(sources: List[Dict[str, str]], output: str,
                 mode: str = "validate", augmentor_cfg: Optional[Dict] = None,
                 timeout: int = 15, log_level: str = "INFO") -> None:
    """
    Convenience wrapper for CLI entrypoint.
    Saves either validated or augmented results depending on mode.
    """
    orchestrator = Orchestrator(
        sources=sources, mode=mode, augmentor_cfg=augmentor_cfg,
        timeout=timeout, log_level=log_level
    )

    results = asyncio.run(orchestrator.run())
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([r.model_dump() for r in results], f, indent=2, ensure_ascii=False)

    logger = get_logger("engine.runner", log_level, "engine.log")
    logger.info("Pipeline results written to %s", out_path)
