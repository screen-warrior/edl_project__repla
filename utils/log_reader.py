from __future__ import annotations

import gzip
from pathlib import Path
from typing import Iterable, List

LOG_DIR = Path("logs")
LOG_FILENAME_PREFIX = "engine.log"


def _iterate_log_files() -> Iterable[Path]:
    if not LOG_DIR.exists():
        return []
    return sorted(LOG_DIR.glob(f"{LOG_FILENAME_PREFIX}*"))


def read_run_log(run_id: str, limit: int | None = None) -> List[str]:
    run_token = f"run_id={run_id}"
    matched: List[str] = []
    for path in _iterate_log_files():
        try:
            if path.suffix == ".gz":
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
                    for line in fh:
                        if run_token in line:
                            matched.append(line.rstrip("\n"))
            else:
                with path.open("r", encoding="utf-8", errors="ignore") as fh:
                    for line in fh:
                        if run_token in line:
                            matched.append(line.rstrip("\n"))
        except FileNotFoundError:
            continue
    if limit is not None and limit >= 0:
        return matched[-limit:]
    return matched
