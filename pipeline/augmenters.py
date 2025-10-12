#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDL Augmentor (enterprise-ready, with logging)

- Uses YAML config to apply augmentation to validated entries
- Works directly with ValidatedEntry (Pydantic) objects
- Expands model with augmented + changes fields
- Logs all augmentation activity to logs/augmentor.log
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List

import idna
from urllib.parse import urlparse, urlunparse
from pydantic import Field

from pipeline.validators import ValidatedEntry, EntryType
from logging.handlers import TimedRotatingFileHandler
from models.models import ValidatedEntry, AugmentedEntry, EntryType

# ----------------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------------
def setup_module_logger(name: str, log_level: str, log_file: str) -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    # File handler (rotates daily)
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
# AugmentedEntry Model
# ----------------------------------------------------------------------
# class AugmentedEntry(ValidatedEntry):
#     augmented: str = Field(..., description="Final augmented value")
#     changes: List[str] = Field(default_factory=list, description="List of applied augmentations")
#

# ----------------------------------------------------------------------
# Augmentor Class
# ----------------------------------------------------------------------
class RunAugmentor:
    def __init__(self, config: Dict, log_level: str = "INFO"):
        self.url_cfg = (config or {}).get("url_augmenter", {})
        self.fqdn_cfg = (config or {}).get("fqdn_augmenter", {})
        self.ipv4_cfg = (config or {}).get("ipv4_augmenter", {})
        self.ipv6_cfg = (config or {}).get("ipv6_augmenter", {})
        self.cidr_cfg = (config or {}).get("cidr_augmenter", {})

        self.logger = setup_module_logger("augmentor", log_level, "augmentor.log")

    # ----------------- URL Augmentations -----------------
    def run_url(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("Starting URL augmentation for: %s", entry.normalized)
        changes: List[str] = []
        p = urlparse(entry.normalized if "://" in entry.normalized else f"http://{entry.normalized}")

        if self.url_cfg.get("strip_query_fragments", {}).get("enabled", False):
            before, after = urlunparse(p), urlunparse(p._replace(query="", fragment=""))
            if before != after:
                changes.append("strip_query_fragments")
                self.logger.info("Applied strip_query_fragments to: %s", entry.normalized)
            p = urlparse(after)

        if self.url_cfg.get("enforce_https", {}).get("enabled", False) and p.scheme == "http":
            p = p._replace(scheme="https")
            changes.append("enforce_https")
            self.logger.info("Enforced HTTPS on: %s", entry.normalized)

        if self.url_cfg.get("enforce_http", {}).get("enabled", False) and p.scheme == "https":
            p = p._replace(scheme="http")
            changes.append("enforce_http")
            self.logger.info("Enforced HTTP on: %s", entry.normalized)

        if self.url_cfg.get("remove_port", {}).get("enabled", False) and p.hostname:
            before = urlunparse(p)
            new_netloc = p.hostname
            p = p._replace(netloc=new_netloc)
            after = urlunparse(p)
            if before != after:
                changes.append("remove_port")
                self.logger.info("Removed port from: %s", entry.normalized)

        if self.url_cfg.get("handle_idna", {}).get("enabled", False) and p.hostname:
            try:
                enc = idna.encode(p.hostname).decode("ascii")
                p = p._replace(netloc=enc)
                changes.append("handle_idna")
                self.logger.info("Applied IDNA encoding to: %s", entry.normalized)
            except Exception as e:
                self.logger.warning("IDNA encoding failed for %s: %s", entry.normalized, e)

        if self.url_cfg.get("normalize_slashes", {}).get("enabled", False):
            norm_path = re.sub(r"/+", "/", p.path or "/")
            before = urlunparse(p)
            p = p._replace(path=norm_path)
            after = urlunparse(p)
            if before != after:
                changes.append("normalize_slashes")
                self.logger.info("Normalized slashes in: %s", entry.normalized)

        return AugmentedEntry(**entry.dict(), augmented=urlunparse(p), changes=changes)

    # ----------------- FQDN Augmentations -----------------
    def run_fqdn(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("Starting FQDN augmentation for: %s", entry.normalized)
        s = entry.normalized
        changes: List[str] = []

        if self.fqdn_cfg.get("remove_port", {}).get("enabled", False) and ":" in s:
            before = s
            h, _, tail = s.rpartition(":")
            if tail.isdigit():
                s = h
            if before != s:
                changes.append("remove_port")
                self.logger.info("Removed port from FQDN: %s", entry.normalized)

        if self.fqdn_cfg.get("wildcard_normalization", {}).get("enabled", False) and s.startswith("*."):
            before = s
            base = s[2:]
            s = f"*.{base.lower()}"
            if s != before:
                changes.append("wildcard_normalization")
                self.logger.info("Normalized wildcard in FQDN: %s", entry.normalized)

        if self.fqdn_cfg.get("convert_unicode", {}).get("enabled", False):
            try:
                enc = idna.encode(s).decode("ascii")
                if enc != s:
                    changes.append("convert_unicode")
                    self.logger.info("Converted Unicode FQDN to ASCII: %s -> %s", s, enc)
                s = enc
            except Exception as e:
                self.logger.warning("Unicode conversion failed for %s: %s", s, e)

        return AugmentedEntry(**entry.dict(), augmented=s, changes=changes)

    # ----------------- IPs and CIDR -----------------
    def run_ipv4(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("IPv4 augmentation placeholder for: %s", entry.normalized)
        return AugmentedEntry(**entry.dict(), augmented=entry.normalized, changes=[])

    def run_ipv6(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("IPv6 augmentation placeholder for: %s", entry.normalized)
        return AugmentedEntry(**entry.dict(), augmented=entry.normalized, changes=[])

    def run_cidr(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("CIDR augmentation placeholder for: %s", entry.normalized)
        return AugmentedEntry(**entry.dict(), augmented=entry.normalized, changes=[])

    # ----------------- Public API -----------------
    def augment(self, entry: ValidatedEntry) -> AugmentedEntry:
        self.logger.debug("Augmenting entry: %s (%s)", entry.original, entry.entry_type)
        if entry.entry_type == EntryType.URL:
            return self.run_url(entry)
        elif entry.entry_type == EntryType.FQDN:
            return self.run_fqdn(entry)
        elif entry.entry_type == EntryType.IPV4:
            return self.run_ipv4(entry)
        elif entry.entry_type == EntryType.IPV6:
            return self.run_ipv6(entry)
        elif entry.entry_type == EntryType.CIDR:
            return self.run_cidr(entry)
        else:
            self.logger.warning("Unsupported entry type for augmentation: %s", entry.entry_type)
            return AugmentedEntry(**entry.dict(), augmented=entry.normalized, changes=[])

    def augment_entries(self, entries: List[ValidatedEntry]) -> List[AugmentedEntry]:
        self.logger.info("Starting augmentation of %d entries", len(entries))
        results = [self.augment(e) for e in entries]
        self.logger.info("Finished augmentation: %d entries processed", len(results))
        return results
