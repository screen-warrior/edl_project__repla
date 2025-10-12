#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EDL Validator with Pydantic (enterprise-ready)

- Validates ipv4, ipv6, cidr, ip_range, fqdn, url
- Logs all validation activity to logs/validator.log
"""

from __future__ import annotations

import ipaddress
import logging
import re
from pathlib import Path
from typing import List, Optional

import idna
import regex
from urllib.parse import urlsplit, urlunsplit
from logging.handlers import TimedRotatingFileHandler
from models.models import ValidatedEntry, EntryType, IngestedEntry


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

    # File handler
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
# Regex for Unicode FQDN validation
# ----------------------------------------------------------------------
_LABEL_UNICODE = r"(?:[\p{L}\p{N}](?:[\p{L}\p{N}-]{0,61}[\p{L}\p{N}])?)"
_FQDN_UNICODE_RE = regex.compile(
    rf"^(?=.{1,253}$){_LABEL_UNICODE}(?:\.{_LABEL_UNICODE})+$",
    flags=regex.UNICODE
)


# ----------------------------------------------------------------------
# Validator
# ----------------------------------------------------------------------
class EDLValidator:
    def __init__(self, log_level: str = "INFO"):
        self.logger = setup_module_logger("validator", log_level, "validator.log")

    def _is_ipv4(self, entry: str) -> Optional[str]:
        try:
            return str(ipaddress.IPv4Address(entry))
        except ValueError:
            return None

    def _is_ipv6(self, entry: str) -> Optional[str]:
        try:
            return str(ipaddress.IPv6Address(entry))
        except ValueError:
            return None

    def _is_cidr(self, entry: str) -> Optional[str]:
        try:
            return ipaddress.ip_network(entry, strict=False).with_prefixlen
        except ValueError:
            return None

    def _is_ip_range(self, entry: str) -> Optional[str]:
        if "-" not in entry:
            return None
        try:
            start_raw, end_raw = [p.strip() for p in entry.split("-")]
            start = ipaddress.ip_address(start_raw)
            end = ipaddress.ip_address(end_raw)
            if start.version != end.version or int(start) > int(end):
                return None
            return f"{start}-{end}"
        except ValueError:
            return None

    def _is_valid_fqdn(self, entry: str) -> Optional[str]:
        s = entry.strip().rstrip("./")
        if not s or "://" in s:
            return None
        try:
            ipaddress.ip_address(s)
            return None
        except ValueError:
            pass
        host = s.lower()
        if host.startswith("*."):
            base = host[2:]
            if "." not in base:
                return None
            return f"*.{base}"
        if "." not in host:
            return None
        return host

    def _is_valid_url(self, entry: str) -> Optional[str]:
        try:
            sp = urlsplit(entry)
            if sp.scheme.lower() not in ("http", "https") or not sp.netloc:
                return None
            host = sp.hostname
            if not host:
                return None
            try:
                ipaddress.ip_address(host)
                host_norm = host
            except ValueError:
                host_norm = idna.encode(host.lower()).decode("ascii")
            display_host = f"[{host_norm}]" if ":" in host_norm and not self._looks_like_ipv4(host_norm) else host_norm
            port_part = f":{sp.port}" if sp.port else ""
            netloc = f"{display_host}{port_part}"
            sp_norm = sp._replace(scheme=sp.scheme.lower(), netloc=netloc)
            return urlunsplit(sp_norm)
        except Exception:
            return None

    @staticmethod
    def _looks_like_ipv4(host: str) -> bool:
        return bool(re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host))

    # ---------------- Public API ----------------
    def validate_entry(self, ingested: IngestedEntry) -> ValidatedEntry:
        entry = ingested.entry.strip()
        source = ingested.source
        self.logger.debug("Validating entry from %s: %s", source, entry)

        if norm := self._is_ipv4(entry):
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.IPV4, normalized=norm)
        if norm := self._is_ipv6(entry):
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.IPV6, normalized=norm)
        if norm := self._is_cidr(entry):
            print(f"the if result is: {ValidatedEntry(source=source, original=entry, entry_type=EntryType.CIDR, normalized=norm)}")
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.CIDR, normalized=norm)
        if norm := self._is_ip_range(entry):
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.IP_RANGE, normalized=norm)
        if norm := self._is_valid_fqdn(entry):
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.FQDN, normalized=norm)
        if norm := self._is_valid_url(entry):
            return ValidatedEntry(source=source, original=entry, entry_type=EntryType.URL, normalized=norm)

        self.logger.warning("Entry invalid/unknown: %s", entry)
        return ValidatedEntry(
            source=source,
            original=entry,
            entry_type=EntryType.UNKNOWN,
            normalized=entry,
            error="Unknown entry type"  # ✅ no valid=False, error drives .valid
        )

    def validate_entries(self, ingested_entries: List[IngestedEntry]) -> List[ValidatedEntry]:
        self.logger.info("Starting validation of %d entries", len(ingested_entries))
        results = [self.validate_entry(e) for e in ingested_entries]
        valid_count = sum(1 for r in results if r.valid)
        self.logger.info("Finished validation (%d valid, %d invalid)", valid_count, len(ingested_entries) - valid_count)
        return results
