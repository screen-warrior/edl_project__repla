#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDL Fetcher

- Downloads raw EDLs from configured sources
- Runs requests concurrently with aiohttp
- Logs fetch activity to logs/fetcher.log
"""

import asyncio
import aiohttp
from typing import List, Dict

from models.pydantic_models import IngestedEntry
from utils.logger import get_logger, log_metric, log_stage


# ----------------------------------------------------------------------
# Fetcher
# ----------------------------------------------------------------------
class EDLFetcher:
    """
    Fetch-only component.
    Supports both URLs and local text files.
    Returns a flat list of IngestedEntry objects.
    """

    def __init__(self, sources: List[Dict[str, str]], timeout: int = 15, log_level: str = "INFO"):
        self.sources = sources
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.logger = get_logger("fetcher", log_level, "fetcher.log")

    async def _fetch_url(self, session: aiohttp.ClientSession, name: str, url: str) -> List[IngestedEntry]:
        try:
            self.logger.debug("Fetching URL: %s -> %s", name, url)
            async with session.get(url) as resp:
                if resp.status != 200:
                    self.logger.warning("%s returned HTTP %s", name, resp.status)
                    return []
                text = await resp.text()
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                self.logger.info("Fetched %d lines from %s", len(lines), name)
                log_metric(self.logger, "lines_fetched", len(lines), stage="fetch", extra={"source": name})
                return [IngestedEntry(source=name, entry=ln) for ln in lines]
        except Exception as e:
            self.logger.error("Failed to fetch URL %s: %s", name, e, exc_info=True)
            return []

    def _fetch_file(self, name: str, path: str) -> List[IngestedEntry]:
        try:
            self.logger.debug("Reading file: %s -> %s", name, path)
            with open(path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            self.logger.info("Read %d lines from file %s", len(lines), path)
            log_metric(self.logger, "lines_fetched", len(lines), stage="fetch", extra={"source": name})
            return [IngestedEntry(source=name, entry=ln) for ln in lines]
        except Exception as e:
            self.logger.error("Failed to read file %s: %s", path, e, exc_info=True)
            return []

    async def run(self) -> List[IngestedEntry]:
        self.logger.info("Starting fetch for %d sources", len(self.sources))
        ingested: List[IngestedEntry] = []

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = []
            for src in self.sources:
                src_type = src.get("type", "url")
                name = src["name"]
                location = src["location"]

                if src_type == "url":
                    tasks.append(self._fetch_url(session, name, location))
                elif src_type == "file":
                    ingested.extend(self._fetch_file(name, location))
                else:
                    self.logger.warning("Unknown source type for %s: %s", name, src_type)

            if tasks:
                with log_stage(self.logger, "url_fetch_batch"):
                    results = await asyncio.gather(*tasks, return_exceptions=False)
                    for res in results:
                        ingested.extend(res)

        self.logger.info("Completed fetch. Total entries: %d", len(ingested))
        log_metric(self.logger, "entries_fetched_total", len(ingested), stage="fetch")
        return ingested
