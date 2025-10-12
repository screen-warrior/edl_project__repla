#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Engine (Orchestrator) - Enterprise Mode

- Loads sources.yaml and augmentor_config.yaml
- Fetches raw EDLs from multiple sources (URL or file)
- Converts them into IngestedEntry objects
- Validates them into ValidatedEntry objects
- Augments them into AugmentedEntry objects
- Outputs JSON file
- Separate logging for engine module (logs/engine.log)
"""

import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
import json
from pathlib import Path
from typing import Dict, List

import yaml

from pipeline.fetcher import EDLFetcher
from pipeline.validators import EDLValidator
from pipeline.augmenters import RunAugmentor
from models.models import IngestedEntry, ValidatedEntry, AugmentedEntry


# ----------------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------------
def setup_module_logger(name: str, log_level: str, log_file: str) -> logging.Logger:
    """Create a logger for this module with console + file handler."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    fh = TimedRotatingFileHandler(log_path, when="midnight", backupCount=7, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------
class Orchestrator:
    def __init__(self, sources: List[Dict[str, str]], augmentor_cfg: Dict, timeout: int = 15, log_level: str = "INFO"):
        self.logger = setup_module_logger("engine.orchestrator", log_level, "engine.log")
        self.fetcher = EDLFetcher(sources=sources, timeout=timeout)
        self.validator = EDLValidator()
        self.augmentor = RunAugmentor(augmentor_cfg)

    async def run(self) -> List[AugmentedEntry]:
        self.logger.info("Starting pipeline run...")

        # Step 1: Fetch -> List[IngestedEntry]
        raw_entries: List[IngestedEntry] = await self.fetcher.run()
        self.logger.info("Fetched %d entries in total", len(raw_entries))

        # Step 2: Validate -> List[ValidatedEntry]
        validated: List[ValidatedEntry] = self.validator.validate_entries(raw_entries)
        self.logger.info("Validated %d entries", len(validated))

        # Step 3: Augment -> List[AugmentedEntry]
        augmented: List[AugmentedEntry] = self.augmentor.augment_entries(validated)
        self.logger.info("Augmentation complete. Total %d entries.", len(augmented))

        return augmented


# ----------------------------------------------------------------------
# Main Entry
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch, validate, and augment EDL entries.")
    parser.add_argument("--sources", required=True, help="Path to sources.yaml")
    parser.add_argument("--config", required=True, help="Path to augmentor_config.yaml")
    parser.add_argument(
        "--output",
        default="test_output_data/augmentor_output.json",
        help="Path to JSON output file",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    logger = setup_module_logger("engine", args.log_level, "engine.log")
    logger.info("Loading configuration...")

    # Load sources.yaml
    with open(args.sources, "r", encoding="utf-8") as f:
        sources_cfg = yaml.safe_load(f) or {}

    sources = sources_cfg.get("sources", [])
    logger.info("Loaded %d sources from %s", len(sources), args.sources)

    # Load augmentor_config.yaml
    with open(args.config, "r", encoding="utf-8") as f:
        augmentor_cfg = yaml.safe_load(f) or {}
    logger.info("Loaded augmentor configuration from %s", args.config)

    # Instantiate orchestrator
    orchestrator = Orchestrator(sources=sources, augmentor_cfg=augmentor_cfg, log_level=args.log_level)

    # ✅ Run pipeline
    result: List[AugmentedEntry] = asyncio.run(orchestrator.run())

    # ✅ Save to JSON
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump([r.dict() for r in result], f, indent=2, ensure_ascii=False)

    logger.info("Augmented entries written to %s", out_path)

    # ✅ Preview console
    print(json.dumps([r.dict() for r in result], indent=2))
