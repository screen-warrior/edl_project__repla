import logging
import uuid
import gzip
import shutil
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from time import perf_counter


# ----------------------------------------------------------------------
# Custom Filters
# ----------------------------------------------------------------------
class CorrelationFilter(logging.Filter):
    """Attach correlation/run ID to every log record."""

    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


class RedactFilter(logging.Filter):
    """Redact sensitive information like API keys, tokens."""

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, "msg") and isinstance(record.msg, str):
            record.msg = (
                record.msg.replace("API_KEY=", "API_KEY=***")
                .replace("Authorization:", "Authorization: ***")
            )
        return True


# ----------------------------------------------------------------------
# Log rotation with gzip compression
# ----------------------------------------------------------------------
def _rotator(source, dest):
    with open(source, "rb") as sf, gzip.open(dest + ".gz", "wb") as df:
        shutil.copyfileobj(sf, df)
    Path(source).unlink()


def _namer(name):
    return name


# ----------------------------------------------------------------------
# Logger Setup
# ----------------------------------------------------------------------
def get_logger(
    name: str,
    log_level: str = "INFO",
    log_file: str = "app.log",
    run_id: str = None,
    retention_days: int = 30,
) -> logging.Logger:
    """
    Create or retrieve a logger with enterprise features:
    - Console + rotating file (daily, compress, retain N days)
    - Correlation ID (run_id)
    - Redaction filter
    """

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Correlation ID
    run_id = run_id or str(uuid.uuid4())
    corr_filter = CorrelationFilter(run_id)
    redact_filter = RedactFilter()

    # Formatter with run_id
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | run_id=%(run_id)s | "
        "%(filename)s:%(lineno)d | %(message)s"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)
    ch.addFilter(corr_filter)
    ch.addFilter(redact_filter)

    # File handler (rotates daily, keeps retention_days, compresses old logs)
    fh = TimedRotatingFileHandler(
        log_dir / log_file, when="midnight", backupCount=retention_days, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    fh.rotator = _rotator
    fh.namer = _namer
    fh.addFilter(corr_filter)
    fh.addFilter(redact_filter)

    # Avoid duplicate handlers
    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


# ----------------------------------------------------------------------
# Metrics Logging Helper
# ----------------------------------------------------------------------
def log_metric(logger: logging.Logger, name: str, value: int, **labels):
    """Log a structured metric in a consistent format."""
    label_str = " ".join(f"{k}={v}" for k, v in labels.items())
    logger.info("METRIC | %s=%s %s", name, value, label_str)


# ----------------------------------------------------------------------
# Stage Timing Context Manager
# ----------------------------------------------------------------------
class log_stage:
    """Context manager for timing a pipeline stage."""

    def __init__(self, logger: logging.Logger, stage: str):
        self.logger = logger
        self.stage = stage

    def __enter__(self):
        self.start = perf_counter()
        self.logger.debug("Stage '%s' started", self.stage)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = perf_counter() - self.start
        self.logger.info("Stage '%s' completed in %.2fs", self.stage, duration)
