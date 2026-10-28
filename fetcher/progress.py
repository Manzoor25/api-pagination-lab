"""Print a one-line progress report after every page.

Learners should be able to see, without opening a profiler, whether a run is
healthy: pages, records, request rate, retries, elapsed time, and a rough ETA.
"""

from __future__ import annotations

import threading
import time


class Progress:
    """Thread-safe progress printer. Parallel strategies share one of these."""

    def __init__(self, strategy: str, total_hint: int | None = None) -> None:
        self.strategy = strategy
        self.total_hint = total_hint
        self.pages = 0
        self.records = 0
        self.requests = 0
        self.retries = 0
        self.errors = 0
        self.started = time.perf_counter()
        self._lock = threading.Lock()

    def set_total(self, total: int | None) -> None:
        if total is not None:
            self.total_hint = total

    def note_retry(self, count: int = 1) -> None:
        with self._lock:
            self.retries += count

    def note_error(self, count: int = 1) -> None:
        with self._lock:
            self.errors += count

    def tick(self, page_records: int, requests: int = 1, detail: str = "") -> None:
        """Record one completed page and print the current pace."""
        with self._lock:
            self.pages += 1
            self.records += page_records
            self.requests += requests
            elapsed = time.perf_counter() - self.started
            rps = self.requests / elapsed if elapsed > 0 else 0.0
            eta = self._eta(elapsed)
            extra = f" {detail}" if detail else ""
            print(
                f"[{self.strategy}] page={self.pages} records={self.records:,} "
                f"requests={self.requests} retries={self.retries} "
                f"elapsed={elapsed:.1f}s rps={rps:.2f} eta={eta}{extra}",
                flush=True,
            )

    def _eta(self, elapsed: float) -> str:
        total = self.total_hint
        if not total or self.records <= 0 or self.records >= total:
            return "done" if total and self.records >= total else "unknown"
        remaining = elapsed / self.records * (total - self.records)
        return _format_seconds(remaining)

    @property
    def elapsed(self) -> float:
        return time.perf_counter() - self.started


def _format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{sec:02d}s"
    if minutes:
        return f"{minutes}m{sec:02d}s"
    return f"{sec}s"
