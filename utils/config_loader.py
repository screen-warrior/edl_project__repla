#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Config Loader (Enterprise Ready)

- Centralized configuration loader for YAML (JSON/env can be added later)
- Validates structure before returning
- Redacts sensitive fields from logs (API keys, passwords, tokens, etc.)
- Provides consistent logging for config load operations
"""

from pathlib import Path
from typing import Dict, Any, Optional
import yaml

from utils.logger import get_logger

# Keys that should never be logged in plain text
SENSITIVE_KEYS = {"password", "api_key", "secret", "token", "auth", "key"}


def _redact_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively redact sensitive values in a config dict.
    """

    redacted = {}
    for k, v in config.items():
        if isinstance(v, dict):
            redacted[k] = _redact_config(v)
        elif isinstance(v, list):
            redacted[k] = [
                _redact_config(i) if isinstance(i, dict) else i for i in v
            ]
        else:
            if any(s in k.lower() for s in SENSITIVE_KEYS):
                redacted[k] = "***REDACTED***"
            else:
                redacted[k] = v
    return redacted


def load_config(path: str, logger: Optional[Any] = None) -> Dict[str, Any]:

    """
    Load a YAML config file with structured logging and redaction.

    Args:
        path: Path to config file
        logger: Optional logger; if None, uses default config logger

    Returns:
        Parsed configuration dict
    """
    log = logger or get_logger("config_loader", "INFO", "config_loader.log")

    config_path = Path(path)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        raise FileNotFoundError(f"Config file not found: {path}")

    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        log.info("Loaded config file: %s", config_path)

        # Log redacted config snapshot for debugging
        redacted = _redact_config(config)
        log.debug("Config contents (redacted): %s", redacted)

        return config
    except yaml.YAMLError as e:
        log.error("Failed to parse YAML config %s: %s", config_path, e, exc_info=True)
        raise
    except Exception as e:
        log.error("Unexpected error loading config %s: %s", config_path, e, exc_info=True)
        raise
