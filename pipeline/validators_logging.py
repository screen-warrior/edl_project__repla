#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Structured logging helpers for the EDL validator.

- JSON logs for easy ingestion (Splunk/ELK/CloudWatch)
- Context fields (run_id, input/output paths)
- Both console and rotating file handler (optional)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach extras safely
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            base.update(record.extra)
        # Include exception info if present
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)


def _make_console_handler(level: int) -> logging.Handler:
    h = logging.StreamHandler(stream=sys.stdout)
    h.setLevel(level)
    h.setFormatter(JSONFormatter())
    return h


def _make_rotating_file_handler(path: str, level: int) -> logging.Handler:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    h = RotatingFileHandler(path, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    h.setLevel(level)
    h.setFormatter(JSONFormatter())
    return h


def get_run_id() -> str:
    return str(uuid.uuid4())


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Create a JSON logger. Idempotent (won't add duplicate handlers).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers on reloads
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(_make_console_handler(level))

    if log_file and not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(_make_rotating_file_handler(log_file, level))

    # Library friendliness: don't propagate to root if using our own handlers
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """
    Emit a structured event with arbitrary fields.
    """
    logger.log(level, event, extra={"extra": fields})
