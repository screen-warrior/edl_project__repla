#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validation Models & Service (Enterprise-Ready, Validation Only)

Responsibilities:
- Detect entry type: ipv4, ipv6, ipv4_with_port, ipv6_with_port, cidr, ip_range, fqdn, url
- Validate strictly (scheme, port, IDNA, format, wildcards)
- Report structured errors (code, message, hint)
- NO normalization, NO augmentation. Normalization happens later in a different stage.
"""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
import ipaddress
import re
from urllib.parse import urlsplit

import idna
import regex
from pydantic import BaseModel, Field


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
def _setup_logger(name: str, log_level: str = "INFO", log_file: str = "validator.log") -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)

    fh = TimedRotatingFileHandler(log_dir / log_file, when="midnight", backupCount=7, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)
    return logger


logger = _setup_logger("validator")


# ----------------------------------------------------------------------
# Enums & Models
# ----------------------------------------------------------------------
class EntryType(str, Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    IPV4_WITH_PORT = "ipv4_with_port"
    IPV6_WITH_PORT = "ipv6_with_port"
    CIDR = "cidr"
    IP_RANGE = "ip_range"
    FQDN = "fqdn"
    URL = "url"
    UNKNOWN = "unknown"


class IngestedEntry(BaseModel):
    source: str = Field(..., description="Name of the feed this entry came from")
    entry: str = Field(..., description="Raw string entry fetched from the source")


class ValidationErrorDetail(BaseModel):
    code: str = Field(..., description="Stable machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    hint: Optional[str] = Field(None, description="Optional remediation hint")


class ValidatedEntry(BaseModel):
    source: Optional[str] = Field(None, description="Source feed name or URL")
    original: str = Field(..., description="Original raw entry string")
    entry_type: EntryType = Field(default=EntryType.UNKNOWN, description="Detected type of entry")
    error: Optional[ValidationErrorDetail] = Field(None, description="If invalid, structured error")

    # In validation-only mode, we keep the original exactly as-is.
    normalized: str = Field(..., description="Always equal to original; no normalization here")

    meta: Dict[str, Any] = Field(default_factory=dict, description="Parsed metadata (host, port, scheme, etc.)")

    @property
    def valid(self) -> bool:
        return self.error is None


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
_ASCII_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_PORT_RE = re.compile(r"^[0-9]{1,5}$")


def _strip_inline_comment(s: str) -> Tuple[str, Optional[str]]:
    if "#" not in s:
        return s, None
    idx = s.find(" #")
    if idx == -1:
        return s, None
    return s[:idx].rstrip(), s[idx + 1:].strip()


def _is_valid_port(port_text: str) -> bool:
    if not _PORT_RE.match(port_text):
        return False
    port = int(port_text)
    return 1 <= port <= 65535


def _try_ipv4(addr: str) -> bool:
    try:
        ipaddress.IPv4Address(addr)
        return True
    except ValueError:
        return False


def _try_ipv6(addr: str) -> bool:
    actual = addr.split("%", 1)[0] if "%" in addr else addr
    try:
        ipaddress.IPv6Address(actual)
        return True
    except ValueError:
        return False


def _split_host_port(text: str) -> Tuple[str, Optional[str], Optional[ValidationErrorDetail]]:
    """
    Split "host[:port]" (IPv4, hostname) or "[IPv6]:port".
    IMPORTANT: Do NOT split unbracketed IPv6 literals by colon.
    Returns: (host_without_brackets_or_trailing_slashes, port_str_or_None, error_or_None)
    """
    s = text.strip()
    if not s:
        return "", None, ValidationErrorDetail(code="empty", message="Empty host", hint=None)

    # Remove trailing slashes (so "example.com/" can pass as host)
    if s.endswith("/"):
        s = s.rstrip("/")

    # Bracketed IPv6 literal
    if s.startswith("["):
        if "]" not in s:
            return s, None, ValidationErrorDetail(code="ipv6_bracket", message="Missing closing bracket", hint="Use [addr]")
        host_literal, _, tail = s[1:].partition("]")
        host_no_brackets = host_literal
        port = None
        if tail.startswith(":"):
            port = tail[1:]
        elif tail != "":
            return s, None, ValidationErrorDetail(code="ipv6_bracket", message="Invalid bracketed literal syntax", hint="Use [addr]:port")

        if not _try_ipv6(host_no_brackets):
            return s, None, ValidationErrorDetail(code="ipv6_invalid", message="Invalid IPv6 literal", hint=None)

        if port is not None and not _is_valid_port(port):
            return s, None, ValidationErrorDetail(code="port_invalid", message="Invalid port", hint="Port 1-65535")

        return host_no_brackets, port, None

    # More than one colon -> likely unbracketed IPv6; don't split as host:port
    colon_count = s.count(":")
    if colon_count > 1:
        return s, None, None

    # Exactly one colon -> host:port
    if colon_count == 1:
        host, _, port = s.rpartition(":")
        if not host:
            return s, None, ValidationErrorDetail(code="host_missing", message="Host missing before port", hint=None)
        if not _is_valid_port(port):
            return s, None, ValidationErrorDetail(code="port_invalid", message="Invalid port", hint="Port 1-65535")
        return host, port, None

    # No colon -> just host
    return s, None, None


def _validate_fqdn_labels_idna_per_label(base: str) -> bool:
    """
    Robust FQDN check:
    - Split into labels
    - Accept punycode labels (xn--*) that already match ASCII rules
    - For non-ASCII labels, IDNA-encode per-label and check ASCII shape
    """
    if not base or len(base) > 253:
        return False
    labels = base.split(".")
    if len(labels) < 2:
        return False
    for lab in labels:
        if not lab:
            return False
        # fast-path: already ASCII & puny-style OK?
        if lab.isascii() and _ASCII_LABEL_RE.match(lab):
            continue
        # else try IDNA per-label
        try:
            puny = idna.alabel(lab).decode("ascii")
        except Exception:
            return False
        if not _ASCII_LABEL_RE.match(puny):
            return False
    return True


def _validate_fqdn(host_text: str) -> Tuple[bool, Optional[ValidationErrorDetail], str, bool]:
    """
    Validate FQDN (Unicode + IDNA), with support for:
      - A single leading wildcard label: "*."
      - Optional trailing slash(es)
    Rejects when a path is present (i.e., a slash that isn't just trailing).

    Returns: (ok, error, cleaned_host, had_trailing_slash)
    cleaned_host has trailing slashes and final dot removed.
    """
    raw = host_text.strip()
    if not raw:
        return False, ValidationErrorDetail(code="fqdn_empty", message="Empty FQDN", hint=None), raw, False

    # Allow ONLY trailing slashes; anything else means a path and is invalid as FQDN
    if "/" in raw:
        first_slash = raw.find("/")
        tail = raw[first_slash:]
        if not all(ch == "/" for ch in tail):
            return False, ValidationErrorDetail(
                code="fqdn_path",
                message="FQDN contains a path but no scheme",
                hint="Use http(s):// if this is a URL, or remove the path for a plain FQDN"
            ), raw, False

    had_trailing_slash = raw.endswith("/")
    host = raw.rstrip("/").rstrip(".")

    if not host:
        return False, ValidationErrorDetail(code="fqdn_empty", message="Empty FQDN", hint=None), raw, had_trailing_slash

    # Wildcard handling
    is_wildcard = host.startswith("*.")  # only allowed at leftmost
    base = host[2:] if is_wildcard else host

    # Basic IDNA/shape on a per-label basis
    if not _validate_fqdn_labels_idna_per_label(base):
        return False, ValidationErrorDetail(
            code="fqdn_idna",
            message="Invalid Unicode/Punycode FQDN",
            hint="Check Unicode characters, hyphens, and label lengths"
        ), base, had_trailing_slash

    # Wildcard allowed only as a single leading label
    if is_wildcard and ("*" in base):
        return False, ValidationErrorDetail(
            code="fqdn_wildcard",
            message="Invalid wildcard placement",
            hint="Only a single leading '*.' label is allowed"
        ), base, had_trailing_slash

    return True, None, host, had_trailing_slash


# ----------------------------------------------------------------------
# Validator Service
# ----------------------------------------------------------------------
class EDLValidator:
    def __init__(self, log_level: str = "INFO"):
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    def validate_entries(self, ingested_entries: List[IngestedEntry]) -> List[ValidatedEntry]:
        results: List[ValidatedEntry] = []
        for e in ingested_entries:
            results.append(self.validate_entry(e))
        valid = sum(1 for r in results if r.valid)
        invalid = len(results) - valid
        counts: Dict[str, int] = {}
        for r in results:
            counts[r.entry_type.value] = counts.get(r.entry_type.value, 0) + 1
        logger.info(
            "Validation complete: %d total (%d valid, %d invalid), breakdown=%s",
            len(results), valid, invalid, counts
        )
        return results

    def validate_entry(self, ingested: IngestedEntry) -> ValidatedEntry:
        raw = ingested.entry.strip()
        body, comment = _strip_inline_comment(raw)
        if not body:
            return self._invalid(ingested, "empty", "Empty line", None)

        # --- URL ---
        if "://" in body:
            sp = urlsplit(body)
            if sp.scheme.lower() not in ("http", "https"):
                return self._invalid(ingested, "url_scheme", "Unsupported URL scheme", "Only http/https are allowed")
            if not sp.netloc:
                return self._invalid(ingested, "url_netloc", "URL missing host", None)

            host = sp.hostname
            if not host:
                return self._invalid(ingested, "url_host", "URL host cannot be parsed", None)

            # Host can be IPv4, IPv6, or FQDN
            if not (_try_ipv4(host) or _try_ipv6(host)):
                ok, err, _, _ = _validate_fqdn(host)
                if not ok:
                    return self._invalid(ingested, err.code, err.message, err.hint)

            if sp.port is not None and not (1 <= int(sp.port) <= 65535):
                return self._invalid(ingested, "url_port", "Invalid URL port", "Port 1-65535")

            return ValidatedEntry(
                source=ingested.source, original=ingested.entry,
                entry_type=EntryType.URL, error=None,
                normalized=ingested.entry,
                meta={
                    "kind": "url", "scheme": sp.scheme.lower(), "host": host,
                    "port": sp.port, "path": sp.path, "query": sp.query,
                    "fragment": sp.fragment, "comment": comment
                }
            )

        # --- CIDR ---
        if "/" in body:
            try:
                net = ipaddress.ip_network(body, strict=False)
                return ValidatedEntry(
                    source=ingested.source, original=ingested.entry,
                    entry_type=EntryType.CIDR, error=None,
                    normalized=ingested.entry,
                    meta={
                        "kind": "cidr", "version": net.version,
                        "network": str(net.network_address),
                        "prefixlen": net.prefixlen, "comment": comment
                    }
                )
            except ValueError:
                pass  # not a CIDR; continue

        # --- IP Range ---
        if "-" in body:
            parts = body.split("-")
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()
                try:
                    ip_l, ip_r = ipaddress.ip_address(left), ipaddress.ip_address(right)
                    if ip_l.version != ip_r.version:
                        return self._invalid(ingested, "range_mixed_versions", "IP range mixes IPv4 and IPv6", None)
                    if int(ip_l) > int(ip_r):
                        return self._invalid(ingested, "range_order", "IP range start is greater than end", None)
                    return ValidatedEntry(
                        source=ingested.source, original=ingested.entry,
                        entry_type=EntryType.IP_RANGE, error=None,
                        normalized=ingested.entry,
                        meta={"kind": "ip_range", "version": ip_l.version, "start": left, "end": right, "comment": comment}
                    )
                except ValueError:
                    pass  # fall through if not strict IPs

        # --- Pure IP or host[:port] ---
        host, port, split_err = _split_host_port(body)
        if split_err:
            return self._invalid(ingested, split_err.code, split_err.message, split_err.hint)

        # IPv4 / IPv4:port
        if _try_ipv4(host):
            if port:
                return ValidatedEntry(
                    source=ingested.source, original=ingested.entry,
                    entry_type=EntryType.IPV4_WITH_PORT, error=None,
                    normalized=ingested.entry,
                    meta={"kind": "ipv4_with_port", "host": host, "port": int(port), "comment": comment}
                )
            return ValidatedEntry(
                source=ingested.source, original=ingested.entry,
                entry_type=EntryType.IPV4, error=None,
                normalized=ingested.entry,
                meta={"kind": "ipv4", "comment": comment}
            )

        # IPv6 / IPv6:port (unbracketed IPv6 never split as host:port above)
        if _try_ipv6(host):
            if port:
                return ValidatedEntry(
                    source=ingested.source, original=ingested.entry,
                    entry_type=EntryType.IPV6_WITH_PORT, error=None,
                    normalized=ingested.entry,
                    meta={"kind": "ipv6_with_port", "host": host, "port": int(port), "comment": comment}
                )
            zone = host.split("%", 1)[1] if "%" in host else None
            return ValidatedEntry(
                source=ingested.source, original=ingested.entry,
                entry_type=EntryType.IPV6, error=None,
                normalized=ingested.entry,
                meta={"kind": "ipv6", "zone": zone, "comment": comment}
            )

        # --- FQDN (with optional wildcard and optional trailing slash) ---
        trailing_slash_flag = body.rstrip().endswith("/")
        ok, fqdn_err, cleaned_host, _ = _validate_fqdn(host)
        if not ok:
            return self._invalid(ingested, fqdn_err.code, fqdn_err.message, fqdn_err.hint)

        return ValidatedEntry(
            source=ingested.source, original=ingested.entry,
            entry_type=EntryType.FQDN, error=None,
            normalized=ingested.entry,
            meta={
                "kind": "fqdn",
                "host": cleaned_host,
                "port": int(port) if port else None,
                "trailing_slash": trailing_slash_flag,
                "comment": comment
            }
        )

    # ---- Internals ----
    @staticmethod
    def _invalid(ingested: IngestedEntry, code: str, message: str, hint: Optional[str]) -> ValidatedEntry:
        logger.debug("Invalid entry: %s | %s (%s)", ingested.entry, code, message)
        return ValidatedEntry(
            source=ingested.source, original=ingested.entry,
            entry_type=EntryType.UNKNOWN,
            error=ValidationErrorDetail(code=code, message=message, hint=hint),
            normalized=ingested.entry, meta={}
        )
