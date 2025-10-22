#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDL Ingestor (Enterprise, Pydantic-Driven)

- Downloads raw EDLs from configured sources
- Supports both remote URLs and local files
- Returns raw `FetchedEntry` objects ready for ingestion
- Logs all ingestion fetch activity to logs/ingestor.log
"""

import asyncio
import aiohttp
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Union

from models.schemas import FetchedEntry
from utils.logger import log_metric


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
# Ingestor Class
# ----------------------------------------------------------------------
class EDLIngestor:
    """
    Fetch raw entries from configured sources (URLs and files).
    Returns raw `FetchedEntry` instances ready for ingestion.
    """

    def __init__(
        self,
        sources: List[Dict[str, str]],
        timeout: int = 15,
        log_level: str = "INFO",
        proxy: Optional[Union[str, Dict[str, str]]] = None,
    ):
        self.sources = sources
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.logger = setup_module_logger("ingestor", log_level, "ingestor.log")
        if isinstance(proxy, dict):
            proxy_url = proxy.get("https") or proxy.get("http")
        else:
            proxy_url = proxy
        env_proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
        self.proxy = proxy_url or env_proxy
        if self.proxy:
            self.logger.info("Using proxy for ingestion: %s", self.proxy)

    async def _fetch_url(self, session: aiohttp.ClientSession, name: str, url: str) -> List[FetchedEntry]:
        """Fetch raw entries from a URL."""
        try:
            self.logger.debug("Fetching URL: %s -> %s", name, url)
            kwargs = {"proxy": self.proxy} if self.proxy else {}
            async with session.get(url, **kwargs) as resp:
                if resp.status != 200:
                    self.logger.warning("%s returned HTTP %s", name, resp.status)
                    return []
                text = await resp.text()
                lines = text.splitlines()
                entries = [
                    FetchedEntry(
                        source=name,
                        raw=line,
                        line_number=index,
                        metadata={"source_type": "url", "url": url},
                    )
                    for index, line in enumerate(lines, start=1)
                ]
                non_empty = sum(1 for e in entries if e.raw.strip())
                self.logger.info("Fetched %d lines (%d non-empty) from %s", len(entries), non_empty, name)
                log_metric(self.logger, "entries_fetched_total", len(entries), stage="fetch", source=name)
                return entries
        except Exception as e:
            self.logger.error("Failed to fetch URL %s: %s", name, e)
            return []

    def _fetch_file(self, name: str, path: str) -> List[FetchedEntry]:
        """Read raw entries from a local file."""
        try:
            self.logger.debug("Reading file: %s -> %s", name, path)
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            entries = [
                FetchedEntry(
                    source=name,
                    raw=line.rstrip("\r\n"),
                    line_number=index,
                    metadata={"source_type": "file", "path": path},
                )
                for index, line in enumerate(lines, start=1)
            ]
            non_empty = sum(1 for e in entries if e.raw.strip())
            self.logger.info("Read %d lines (%d non-empty) from file %s", len(entries), non_empty, path)
            log_metric(self.logger, "entries_fetched_total", len(entries), stage="fetch", source=name)
            return entries
        except Exception as e:
            self.logger.error("Failed to read file %s: %s", path, e)
            return []

    async def run(self) -> List[FetchedEntry]:
        """
        Run fetch for all sources (URL + file).
        Returns flat list of FetchedEntry instances.
        """
        self.logger.info("Starting fetch for %d sources", len(self.sources))
        results: List[FetchedEntry] = []

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

        non_empty_total = sum(1 for entry in results if entry.raw.strip())
        log_metric(self.logger, "entries_fetched_total", len(results), stage="fetch", source="__aggregate__")
        log_metric(self.logger, "entries_fetched_non_empty", non_empty_total, stage="fetch", source="__aggregate__")
        self.logger.info("Completed fetch. Total entries: %d (non-empty=%d)", len(results), non_empty_total)
        return results
