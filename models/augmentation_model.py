#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Augmentor Models & Service (Enterprise-Ready)

Responsibilities:
- Take validated entries and apply augmentation per augmentor_config.yaml
- Each EDL type (URL, FQDN, IPv4, IPv6, CIDR) has its own augmentation pipeline
- Pydantic models ensure strict schema and predictable output
- Structured logging, metrics, and stage timers included
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

from models.schemas import ValidatedEntry, EntryType
from utils.logger import get_logger, log_stage, log_metric


# ----------------------------------------------------------------------
# Pydantic Models (Config Representation)
# ----------------------------------------------------------------------
class ToggleOption(BaseModel):
    enabled: bool = Field(False, description="Toggle to enable/disable this option")


class PortOption(BaseModel):
    enabled: bool = Field(False)
    ports: List[int] = Field(default_factory=list)


class UrlAugmenterConfig(BaseModel):
    strip_query_fragments: ToggleOption = ToggleOption()
    enforce_https: ToggleOption = ToggleOption()
    enforce_http: ToggleOption = ToggleOption()
    remove_port: ToggleOption = ToggleOption()
    remove_specific_port: PortOption = PortOption()
    handle_idna: ToggleOption = ToggleOption()
    normalize_slashes: ToggleOption = ToggleOption()


class FqdnAugmenterConfig(BaseModel):
    remove_port: ToggleOption = ToggleOption()
    remove_specific_port: PortOption = PortOption()
    wildcard_normalization: ToggleOption = ToggleOption()
    convert_unicode: ToggleOption = ToggleOption()
    strip_path: ToggleOption = ToggleOption(enabled=True)


class Ipv4AugmenterConfig(BaseModel):
    remove_port: ToggleOption = ToggleOption()
    remove_exact_port: PortOption = PortOption()


class Ipv6AugmenterConfig(BaseModel):
    remove_port: ToggleOption = ToggleOption()
    remove_exact_port: PortOption = PortOption()


class CidrAugmenterConfig(BaseModel):
    normalize: ToggleOption = ToggleOption(enabled=True)


class AugmentorConfig(BaseModel):
    url_augmenter: UrlAugmenterConfig = UrlAugmenterConfig()
    fqdn_augmenter: FqdnAugmenterConfig = FqdnAugmenterConfig()
    ipv4_augmenter: Ipv4AugmenterConfig = Ipv4AugmenterConfig()
    ipv6_augmenter: Ipv6AugmenterConfig = Ipv6AugmenterConfig()
    cidr_augmenter: CidrAugmenterConfig = CidrAugmenterConfig()


# ----------------------------------------------------------------------
# Augmented Entry Model
# ----------------------------------------------------------------------
class AugmentedEntry(ValidatedEntry):
    augmented: str = Field(..., description="Augmented value after transformations")
    changes: List[str] = Field(default_factory=list, description="List of applied augmentations")


# ----------------------------------------------------------------------
# Augmentor Service
# ----------------------------------------------------------------------
class EDL_Augmentor:
    def __init__(self, cfg: Dict[str, Any], log_level: str = "INFO"):
        self.logger = get_logger("augmentor", log_level, "augmentor.log")
        self.cfg = AugmentorConfig.model_validate(cfg)

    def augment_entries(self, validated: List[ValidatedEntry]) -> List[AugmentedEntry]:
        with log_stage(self.logger, "augment_batch"):
            results: List[AugmentedEntry] = []
            for entry in validated:
                if not entry.valid:
                    # Pass through invalid entries unchanged
                    results.append(
                        AugmentedEntry(**entry.model_dump(), augmented=entry.normalized, changes=["invalid_pass_through"])
                    )
                    continue

                augmented = self.augment_entry(entry)
                results.append(augmented)

            log_metric(self.logger, "entries_augmented_total", len(results), stage="augment")
            return results

    def augment_entry(self, entry: ValidatedEntry) -> AugmentedEntry:
        changes = []
        new_value = entry.normalized

        if entry.entry_type == EntryType.URL:
            new_value, changes = self._augment_url(new_value, entry.meta)
        elif entry.entry_type == EntryType.FQDN:
            new_value, changes = self._augment_fqdn(new_value, entry.meta)
        elif entry.entry_type == EntryType.IPV4 or entry.entry_type == EntryType.IPV4_WITH_PORT:
            new_value, changes = self._augment_ipv4(new_value, entry.meta)
        elif entry.entry_type == EntryType.IPV6 or entry.entry_type == EntryType.IPV6_WITH_PORT:
            new_value, changes = self._augment_ipv6(new_value, entry.meta)
        elif entry.entry_type == EntryType.CIDR:
            new_value, changes = self._augment_cidr(new_value, entry.meta)

        self.logger.debug("Augmented entry: %s -> %s (changes=%s)", entry.original, new_value, changes)
        return AugmentedEntry(**entry.model_dump(), augmented=new_value, changes=changes)

    # ------------------------------------------------------------------
    # URL Augmentation
    # ------------------------------------------------------------------
    def _augment_url(self, value: str, meta: Dict[str, Any]) -> tuple[str, List[str]]:
        changes = []
        sp = urlsplit(value)
        scheme, netloc, path, query, fragment = sp

        cfg = self.cfg.url_augmenter

        if cfg.strip_query_fragments.enabled:
            if query or fragment:
                changes.append("strip_query_fragments")
            query, fragment = "", ""

        if cfg.enforce_https.enabled and scheme != "https":
            scheme = "https"
            changes.append("enforce_https")

        if cfg.enforce_http.enabled and scheme != "http":
            scheme = "http"
            changes.append("enforce_http")

        if cfg.remove_port.enabled and ":" in netloc:
            host = netloc.split(":")[0]
            netloc = host
            changes.append("remove_port")

        if cfg.remove_specific_port.enabled and ":" in netloc:
            host, port = netloc.split(":")
            if int(port) in cfg.remove_specific_port.ports:
                netloc = host
                changes.append(f"remove_specific_port:{port}")

        if cfg.normalize_slashes.enabled and path and "//" in path:
            path = re.sub("/{2,}", "/", path)
            changes.append("normalize_slashes")

        new_url = urlunsplit((scheme, netloc, path, query, fragment))
        return new_url, changes

    # ------------------------------------------------------------------
    # FQDN Augmentation
    # ------------------------------------------------------------------
    def _augment_fqdn(self, value: str, meta: Dict[str, Any]) -> tuple[str, List[str]]:
        changes = []
        cfg = self.cfg.fqdn_augmenter

        host = value
        if cfg.remove_port.enabled and ":" in host:
            host = host.split(":")[0]
            changes.append("remove_port")

        if cfg.remove_specific_port.enabled and ":" in host:
            base, port = host.split(":")
            if int(port) in cfg.remove_specific_port.ports:
                host = base
                changes.append(f"remove_specific_port:{port}")

        if cfg.wildcard_normalization.enabled and host.startswith("*."):
            # Normalize to base domain
            host = host[2:]
            changes.append("wildcard_normalization")

        # Unicode → punycode
        if cfg.convert_unicode.enabled:
            try:
                import idna
                host = idna.encode(host).decode("ascii")
                changes.append("convert_unicode")
            except Exception:
                self.logger.warning("Failed unicode conversion for host=%s", host)

        if cfg.strip_path.enabled and "/" in host:
            base = host.split("/", 1)[0]
            if base != host:
                host = base
                changes.append("strip_path")
            elif host.endswith("/"):
                host = host.rstrip("/")
                changes.append("strip_path")

        return host, changes

    # ------------------------------------------------------------------
    # IPv4 Augmentation
    # ------------------------------------------------------------------
    def _augment_ipv4(self, value: str, meta: Dict[str, Any]) -> tuple[str, List[str]]:
        changes = []
        cfg = self.cfg.ipv4_augmenter

        if cfg.remove_port.enabled and ":" in value:
            value = value.split(":")[0]
            changes.append("remove_port")

        if cfg.remove_exact_port.enabled and ":" in value:
            base, port = value.split(":")
            if int(port) == cfg.remove_exact_port.port:
                value = base
                changes.append(f"remove_exact_port:{port}")

        return value, changes

    # ------------------------------------------------------------------
    # IPv6 Augmentation
    # ------------------------------------------------------------------
    def _augment_ipv6(self, value: str, meta: Dict[str, Any]) -> tuple[str, List[str]]:
        changes = []
        cfg = self.cfg.ipv6_augmenter

        if cfg.remove_port.enabled and "]:" in value:
            value = value.split("]:")[0] + "]"
            changes.append("remove_port")

        if cfg.remove_exact_port.enabled and "]:" in value:
            base, port = value.split("]:")
            port = port.strip()
            if int(port) == cfg.remove_exact_port.port:
                value = base + "]"
                changes.append(f"remove_exact_port:{port}")

        return value, changes

    # ------------------------------------------------------------------
    # CIDR Augmentation
    # ------------------------------------------------------------------
    def _augment_cidr(self, value: str, meta: Dict[str, Any]) -> tuple[str, List[str]]:
        changes = []
        cfg = self.cfg.cidr_augmenter
        if cfg.normalize.enabled:
            try:
                import ipaddress
                net = ipaddress.ip_network(value, strict=False)
                normalized = str(net)
                if normalized != value:
                    value = normalized
                    changes.append("normalize")
            except Exception:
                self.logger.warning("Failed CIDR normalization for value=%s", value)
        return value, changes
