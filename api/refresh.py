from __future__ import annotations

import threading
from typing import Callable, Optional


class RefreshScheduler:
    """Lightweight scaffolding for future automated refresh jobs."""

    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self, interval_minutes: float, task: Callable[[], None]) -> None:
        if interval_minutes <= 0:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()

        def runner() -> None:
            while not self._stop_event.wait(interval_minutes * 60):
                task()

        self._thread = threading.Thread(target=runner, daemon=True, name="edl-refresh-scheduler")
        self._thread.start()

    def stop(self) -> None:
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(timeout=2.0)
        self._thread = None
        self._stop_event.clear()


__all__ = ["RefreshScheduler"]
