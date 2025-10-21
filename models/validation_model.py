#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validation Models & Service (Enterprise-Ready, Validation Only)

Responsibilities:
- Detect entry type: ipv4, ipv6, ipv4_with_port, ipv6_with_port, cidr, ip_range, fqdn, url
- Validate strictly (scheme, port, IDNA per-label, wildcard placement, trailing slashes)
- Report structured errors (code, message, hint)
- NO normalization, NO augmentation. Normalization happens later in a different stage.
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
import ipaddress
import re
from urllib.parse import urlsplit

import idna

from models.schemas import (
    EntryType,
    IngestedEntry,
    ValidationErrorDetail,
    ValidatedEntry,
)
from utils.logger import get_logger, log_metric, log_stage


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
_ASCII_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_PORT_RE = re.compile(r"^[0-9]{1,5}$")

_FOUR_PART_NUMERIC_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_IPV4_ISH_RE = re.compile(r"^\d+(?:\.\d+){1,}$")
_POTENTIAL_CIDR_RE = re.compile(r"^[^/]+/\d{1,3}$")


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
    core = addr.split("%", 1)[0] if "%" in addr else addr
    try:
        ipaddress.IPv6Address(core)
        return True
    except ValueError:
        return False


def _split_host_port(text: str) -> Tuple[str, Optional[str], Optional[ValidationErrorDetail]]:
    s = text.strip()
    if not s:
        return "", None, ValidationErrorDetail(code="empty", message="Empty host", hint=None)

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

    colon_count = s.count(":")
    if colon_count > 1:
        return s, None, None

    if colon_count == 1:
        host, _, port = s.rpartition(":")
        if not host:
            return s, None, ValidationErrorDetail(code="host_missing", message="Host missing before port", hint=None)
        if not _is_valid_port(port):
            return s, None, ValidationErrorDetail(code="port_invalid", message="Invalid port", hint="Port 1-65535")
        return host, port, None

    return s, None, None


def _merge_meta(base: Dict[str, Any], extra: Dict[str, Any], comment: Optional[str]) -> Dict[str, Any]:
    """
    Merge ingestion metadata with validation-specific metadata while
    preserving (or clearing) the comment as appropriate.
    """
    combined = dict(base)
    combined.update(extra)
    if comment is not None:
        combined["comment"] = comment
    else:
        combined.pop("comment", None)
    return combined


def _validate_fqdn_labels_idna_per_label(base: str) -> bool:
    if not base or len(base) > 253:
        return False

    labels = base.split(".")
    if len(labels) < 2:
        return False

    for lab in labels:
        if not lab:
            return False
        if lab.isascii():
            if not _ASCII_LABEL_RE.match(lab):
                return False
            continue
        try:
            puny = idna.alabel(lab).decode("ascii")
        except Exception:
            return False
        if not _ASCII_LABEL_RE.match(puny):
            return False
    return True


def _validate_fqdn(host_text: str) -> Tuple[bool, Optional[ValidationErrorDetail], str, bool]:
    raw = host_text.strip()
    if not raw:
        return False, ValidationErrorDetail(code="fqdn_empty", message="Empty FQDN", hint=None), raw, False

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

    is_wildcard = host.startswith("*.")
    base = host[2:] if is_wildcard else host

    if ("*" in base) or (not is_wildcard and host.startswith("*")):
        return False, ValidationErrorDetail(
            code="fqdn_wildcard",
            message="Invalid wildcard placement",
            hint="Only a single leading '*.' label is allowed"
        ), base, had_trailing_slash

    if not _validate_fqdn_labels_idna_per_label(base):
        return False, ValidationErrorDetail(
            code="fqdn_idna",
            message="Invalid Unicode/Punycode FQDN",
            hint="Check Unicode characters, hyphens, and label lengths"
        ), base, had_trailing_slash

    return True, None, host, had_trailing_slash


# ----------------------------------------------------------------------
# Validator Service
# ----------------------------------------------------------------------
class EDLValidator:
    def __init__(self, log_level: str = "INFO"):
        self.logger = get_logger("validator", log_level, "validator.log")

    def validate_entries(self, ingested_entries: List[IngestedEntry]) -> List[ValidatedEntry]:
        with log_stage(self.logger, "validate_batch"):
            results: List[ValidatedEntry] = []
            for e in ingested_entries:
                self.logger.debug("Validating entry: %s (source=%s)", e.entry, e.source)
                results.append(self.validate_entry(e))

            valid = sum(1 for r in results if r.valid)
            invalid = len(results) - valid
            counts: Dict[str, int] = {}
            for r in results:
                counts[r.entry_type.value] = counts.get(r.entry_type.value, 0) + 1

            self.logger.info(
                "Validation complete: %d total (%d valid, %d invalid), breakdown=%s",
                len(results), valid, invalid, counts
            )
            log_metric(self.logger, "validation_total", len(results), stage="validate")
            log_metric(self.logger, "validation_valid", valid, stage="validate")
            log_metric(self.logger, "validation_invalid", invalid, stage="validate")

            return results

    def validate_entry(self, ingested: IngestedEntry) -> ValidatedEntry:
        base_meta = dict(ingested.metadata)
        if ingested.line_number is not None:
            base_meta.setdefault("line_number", ingested.line_number)

        original_value = base_meta.get("raw", ingested.entry)
        raw = ingested.entry.strip()
        body, comment_inline = _strip_inline_comment(raw)
        comment = base_meta.get("comment") or comment_inline

        if not body:
            return self._invalid(ingested, "empty", "Empty line", None, comment)

        # --- URL ---
        if "://" in body:
            sp = urlsplit(body)
            scheme = sp.scheme.lower()
            if scheme not in ("http", "https"):
                return self._invalid(
                    ingested,
                    "url_scheme",
                    "Unsupported URL scheme",
                    "Only http/https are allowed",
                    comment,
                )
            if not sp.netloc:
                return self._invalid(ingested, "url_netloc", "URL missing host", None, comment)

            host = sp.hostname
            if not host:
                return self._invalid(ingested, "url_host", "URL host cannot be parsed", None, comment)

            if not (_try_ipv4(host) or _try_ipv6(host)):
                ok, err, _, _ = _validate_fqdn(host)
                if not ok:
                    return self._invalid(ingested, err.code, err.message, err.hint, comment)

            try:
                port = sp.port
            except ValueError:
                return self._invalid(
                    ingested,
                    "url_port",
                    "Invalid URL port",
                    "Port must be between 1 and 65535",
                    comment,
                )

            if port is not None and not (1 <= int(port) <= 65535):
                return self._invalid(ingested, "url_port", "Invalid URL port", "Port 1-65535", comment)

            meta = _merge_meta(
                base_meta,
                {
                    "kind": "url",
                    "scheme": scheme,
                    "host": host,
                    "port": port,
                    "path": sp.path,
                    "query": sp.query,
                    "fragment": sp.fragment,
                },
                comment,
            )
            return ValidatedEntry(
                source=ingested.source,
                original=original_value,
                entry_type=EntryType.URL,
                error=None,
                normalized=body,
                meta=meta,
            )

        # --- CIDR ---
        if "/" in body:
            try:
                net = ipaddress.ip_network(body, strict=False)
                meta = _merge_meta(
                    base_meta,
                    {
                        "kind": "cidr",
                        "version": net.version,
                        "network": str(net.network_address),
                        "prefixlen": net.prefixlen,
                    },
                    comment,
                )
                return ValidatedEntry(
                    source=ingested.source,
                    original=original_value,
                    entry_type=EntryType.CIDR,
                    error=None,
                    normalized=body,
                    meta=meta,
                )
            except ValueError:
                if _POTENTIAL_CIDR_RE.match(body):
                    return self._invalid(
                        ingested,
                        "cidr_invalid",
                        "Invalid CIDR notation",
                        "Ensure address and prefix length are correct (e.g., 192.0.2.0/24)",
                        comment,
                    )

        # --- IP Range ---
        if "-" in body:
            parts = body.split("-")
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()
                try:
                    ip_l, ip_r = ipaddress.ip_address(left), ipaddress.ip_address(right)
                    if ip_l.version != ip_r.version:
                        return self._invalid(ingested, "range_mixed_versions", "IP range mixes IPv4 and IPv6", None, comment)
                    if int(ip_l) > int(ip_r):
                        return self._invalid(ingested, "range_order", "IP range start is greater than end", None, comment)
                    meta = _merge_meta(
                        base_meta,
                        {
                            "kind": "ip_range",
                            "version": ip_l.version,
                            "start": left,
                            "end": right,
                        },
                        comment,
                    )
                    return ValidatedEntry(
                        source=ingested.source,
                        original=original_value,
                        entry_type=EntryType.IP_RANGE,
                        error=None,
                        normalized=body,
                        meta=meta,
                    )
                except ValueError:
                    pass

        # --- Pure IP or host[:port] ---
        host, port, split_err = _split_host_port(body)
        if split_err:
            return self._invalid(ingested, split_err.code, split_err.message, split_err.hint)

        if _try_ipv4(host):
            if port:
                meta = _merge_meta(
                    base_meta,
                    {"kind": "ipv4_with_port", "host": host, "port": int(port)},
                    comment,
                )
                return ValidatedEntry(
                    source=ingested.source,
                    original=original_value,
                    entry_type=EntryType.IPV4_WITH_PORT,
                    error=None,
                    normalized=body,
                    meta=meta,
                )
            meta = _merge_meta(base_meta, {"kind": "ipv4"}, comment)
            return ValidatedEntry(
                source=ingested.source,
                original=original_value,
                entry_type=EntryType.IPV4,
                error=None,
                normalized=body,
                meta=meta,
            )

        if _try_ipv6(host):
            if port:
                meta = _merge_meta(
                    base_meta,
                    {"kind": "ipv6_with_port", "host": host, "port": int(port)},
                    comment,
                )
                return ValidatedEntry(
                    source=ingested.source,
                    original=original_value,
                    entry_type=EntryType.IPV6_WITH_PORT,
                    error=None,
                    normalized=body,
                    meta=meta,
                )
            zone = host.split("%", 1)[1] if "%" in host else None
            meta = _merge_meta(
                base_meta,
                {"kind": "ipv6", "zone": zone},
                comment,
            )
            return ValidatedEntry(
                source=ingested.source,
                original=original_value,
                entry_type=EntryType.IPV6,
                error=None,
                normalized=body,
                meta=meta,
            )

        if _FOUR_PART_NUMERIC_RE.match(host) or _IPV4_ISH_RE.match(host):
            return self._invalid(
                ingested,
                "ip_invalid",
                "Invalid IPv4 address",
                "IPv4 must have 4 octets 0-255 (e.g., 203.0.113.10)",
                comment,
            )

        ok, fqdn_err, cleaned_host, trailing_slash = _validate_fqdn(host)
        if not ok:
            return self._invalid(ingested, fqdn_err.code, fqdn_err.message, fqdn_err.hint, comment)

        meta = _merge_meta(
            base_meta,
            {
                "kind": "fqdn",
                "host": cleaned_host,
                "port": int(port) if port else None,
                "trailing_slash": trailing_slash,
            },
            comment,
        )
        return ValidatedEntry(
            source=ingested.source,
            original=original_value,
            entry_type=EntryType.FQDN,
            error=None,
            normalized=body,
            meta=meta,
        )

    def _invalid(
        self,
        ingested: IngestedEntry,
        code: str,
        message: str,
        hint: Optional[str],
        comment: Optional[str] = None,
    ) -> ValidatedEntry:
        self.logger.warning(
            "Invalid entry detected | entry=%s | code=%s | message=%s | hint=%s",
            ingested.entry, code, message, hint
        )
        base_meta = dict(ingested.metadata)
        if comment is not None:
            base_meta["comment"] = comment
        if ingested.line_number is not None:
            base_meta.setdefault("line_number", ingested.line_number)
        original_value = base_meta.get("raw", ingested.entry)
        return ValidatedEntry(
            source=ingested.source, original=original_value,
            entry_type=EntryType.UNKNOWN,
            error=ValidationErrorDetail(code=code, message=message, hint=hint),
            normalized=ingested.entry,
            meta=base_meta,
        )
