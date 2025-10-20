"""
SQLModel session utilities for the EDL pipeline.

Provides an engine factory, retry-aware transactional scope, and database
initialisation helpers backed by SQLite (override via DATABASE_URL if needed).
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy.exc import OperationalError
from sqlmodel import Session, SQLModel, create_engine

logger = logging.getLogger("db.session")


# ---------------------------------------------------------------------------
# Engine / configuration
# ---------------------------------------------------------------------------
def _build_database_url() -> str:
    """
    Compute the database URL, defaulting to a local SQLite file.
    """

    if url := os.getenv("DATABASE_URL"):
        return url
    if path := os.getenv("SQLITE_PATH"):
        # Accept bare file paths for convenience.
        if not path.startswith("sqlite"):
            return f"sqlite:///{path}"
        return path

    return "sqlite:///./edl_pipeline.db"


DATABASE_URL = _build_database_url()
ECHO = bool(int(os.getenv("SQL_ECHO", "0")))
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
RETRY_DELAY = float(os.getenv("DB_RETRY_DELAY", "2.0"))

engine_kwargs = {
    "echo": ECHO,
    "pool_pre_ping": True,
    "pool_size": POOL_SIZE,
    "max_overflow": MAX_OVERFLOW,
    "pool_recycle": POOL_RECYCLE,
}

if DATABASE_URL.startswith("sqlite"):
    engine_kwargs.pop("pool_size", None)
    engine_kwargs.pop("max_overflow", None)
    engine_kwargs.pop("pool_recycle", None)
    engine_kwargs["connect_args"] = {"check_same_thread": False}

ENGINE = create_engine(DATABASE_URL, **engine_kwargs)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------
@contextmanager
def session_scope(
    retries: int = 3,
    retry_delay: Optional[float] = None,
) -> Generator[Session, None, None]:
    """
    Provide a transactional scope with simple retry handling.
    """

    delay = retry_delay if retry_delay is not None else RETRY_DELAY
    attempt = 0

    while True:
        session = Session(ENGINE)
        try:
            yield session
            session.commit()
            break
        except OperationalError as exc:
            session.rollback()
            attempt += 1
            if attempt >= retries:
                logger.exception("Database operation failed after %s retries.", retries)
                raise
            logger.warning(
                "OperationalError during DB session (attempt %s/%s): %s",
                attempt,
                retries,
                exc,
            )
            time.sleep(delay)
        except Exception:
            session.rollback()
            logger.exception("Unhandled error during DB session; rolling back.")
            raise
        finally:
            session.close()


def get_session() -> Session:
    """
    Convenience helper to obtain a raw Session (caller must close it).
    """

    return Session(ENGINE)


def init_db(drop_existing: bool = False) -> None:
    """
    Initialise the database schema using SQLModel metadata.
    """

    from db import models  # Local import to avoid circular dependency

    if drop_existing:
        SQLModel.metadata.drop_all(ENGINE)
    SQLModel.metadata.create_all(ENGINE)


__all__ = [
    "ENGINE",
    "session_scope",
    "get_session",
    "init_db",
]
