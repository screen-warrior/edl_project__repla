#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDL Fetcher (Enterprise, Pydantic-Driven)

- Downloads raw EDLs from configured sources
- Supports both remote URLs and local files
- Directly returns ValidatedEntry objects (auto-classified/normalized via Pydantic)
- Logs all fetch activity to logs/fetcher.log
"""

import asyncio
import aiohttp
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import List, Dict

from models.models import ValidatedEntry


# ----------------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------------
def setup_module_logger(name: str, log_level: str, log_file: str) -> logging.Logger:
    """
    Create a logger with console + file handler (rotated daily).
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    # File
    fh = TimedRotatingFileHandler(log_path, when="midnight", backupCount=7, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


# ----------------------------------------------------------------------
# Fetcher Class
# ----------------------------------------------------------------------
class EDLFetcher:
    """
    Fetcher that ingests from URLs or local files,
    and directly returns ValidatedEntry objects.
    """

    def __init__(self, sources: List[Dict[str, str]], timeout: int = 15, log_level: str = "INFO"):
        self.sources = sources
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.logger = setup_module_logger("fetcher", log_level, "fetcher.log")

    async def _fetch_url(self, session: aiohttp.ClientSession, name: str, url: str) -> List[ValidatedEntry]:
        """
        Fetch entries from a URL and auto-validate them via ValidatedEntry.
        """
        try:
            self.logger.debug("Fetching URL: %s -> %s", name, url)
            async with session.get(url) as resp:
                if resp.status != 200:
                    self.logger.warning("%s returned HTTP %s", name, resp.status)
                    return []
                text = await resp.text()
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                self.logger.info("Fetched %d lines from %s", len(lines), name)
                return [ValidatedEntry(source=name, original=ln) for ln in lines]
        except Exception as e:
            self.logger.error("Failed to fetch URL %s: %s", name, e)
            return []

    def _fetch_file(self, name: str, path: str) -> List[ValidatedEntry]:
        """
        Read entries from a local file and auto-validate them via ValidatedEntry.
        """
        try:
            self.logger.debug("Reading file: %s -> %s", name, path)
            with open(path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            self.logger.info("Read %d lines from file %s", len(lines), path)
            return [ValidatedEntry(source=name, original=ln) for ln in lines]
        except Exception as e:
            self.logger.error("Failed to read file %s: %s", path, e)
            return []

    async def run(self) -> List[ValidatedEntry]:
        """
        Run fetch for all sources (URL + file).
        Returns flat list of ValidatedEntry.
        """
        self.logger.info("Starting fetch for %d sources", len(self.sources))
        results: List[ValidatedEntry] = []

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = []
            for src in self.sources:
                src_type = src.get("type", "url")
                name = src["name"]
                location = src["location"]

                if src_type == "url":
                    tasks.append(self._fetch_url(session, name, location))
                elif src_type == "file":
                    results.extend(self._fetch_file(name, location))
                else:
                    self.logger.warning("Unknown source type for %s: %s", name, src_type)

            if tasks:
                url_results = await asyncio.gather(*tasks, return_exceptions=False)
                for batch in url_results:
                    results.extend(batch)

        self.logger.info("Completed fetch. Total entries: %d", len(results))
        return results
