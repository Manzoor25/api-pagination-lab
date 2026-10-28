"""Retries, backoff, and a client-side token bucket.

What this file teaches
----------------------
A 429 or a 503 is not a reason to abort a multi-hour pull, and it is also not
a reason to immediately hammer the API again. The safe pattern is:

1. Retry only retryable failures (429, 500, 502, 503, 504, timeouts, connection drops).
2. Wait at least as long as the server's Retry-After header.
3. Otherwise wait with exponential backoff plus jitter, so many clients do not
   wake up on the same millisecond and cause a second outage.
4. Optionally take a token from a local bucket *before* the request, so you
   never exceed a rate you already know about.

Do not retry 400. A bad page size will never succeed if you try it again.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import TypeVar

T = TypeVar("T")

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass
class AttemptResult:
    """The outcome of one HTTP attempt, stripped of the HTTP library.

    Tests pass these in directly. The real client adapts httpx responses into
    the same shape so the retry policy can be tested without a server.
    """

    status_code: int
    headers: dict[str, str]
    body: T
    retryable_error: str | None = None


class TokenBucket:
    """A lock-protected token bucket. One token = permission to send one request."""

    def __init__(self, rate_per_second: float, capacity: float | None = None) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive; skip the bucket to disable it")
        self.rate = rate_per_second
        # A burst of one second is enough. A huge burst just moves the 429s later.
        self.capacity = capacity if capacity is not None else max(1.0, rate_per_second)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available. Parallel tasks share this bucket."""
        while True:
            with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            time.sleep(wait)


def retry_after_seconds(headers: dict[str, str], now: float | None = None) -> float | None:
    """Read Retry-After as either delta-seconds or an HTTP date."""
    raw = None
    for key, value in headers.items():
        if key.lower() == "retry-after":
            raw = value.strip()
            break
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw).timestamp()
    except (TypeError, ValueError, IndexError):
        return None
    current = time.time() if now is None else now
    return max(0.0, when - current)


def backoff_seconds(attempt: int, base: float = 0.25, cap: float = 30.0, rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter.

    attempt is 0 for the first retry. Jitter is uniform between 0 and the cap
    for this attempt, which is the "full jitter" pattern from the AWS
    Architecture Blog. It spreads retries out instead of synchronizing them.
    """
    chooser = rng or random
    ceiling = min(cap, base * (2**attempt))
    return chooser.uniform(0.0, ceiling)


def call_with_retry(
    do_request: Callable[[], AttemptResult],
    max_retries: int = 6,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, float, str], None] | None = None,
    bucket: TokenBucket | None = None,
) -> tuple[AttemptResult, int]:
    """Call do_request until it succeeds or retries are exhausted.

    Returns (result, retry_count). Raises RuntimeError if every attempt failed
    with a retryable error. A non-retryable status (400, 401, 404, ...) is
    returned immediately so the caller can show the real error.
    """
    last: AttemptResult | None = None
    for attempt in range(max_retries + 1):
        if bucket is not None:
            bucket.acquire()
        result = do_request()
        last = result
        retryable = result.retryable_error is not None or result.status_code in RETRYABLE_STATUS
        if not retryable:
            return result, attempt
        if attempt >= max_retries:
            break
        hinted = retry_after_seconds(result.headers)
        delay = hinted if hinted is not None else backoff_seconds(attempt, rng=rng)
        reason = result.retryable_error or f"HTTP {result.status_code}"
        if on_retry is not None:
            on_retry(attempt + 1, delay, reason)
        sleeper(delay)
    assert last is not None
    detail = last.retryable_error or f"HTTP {last.status_code}"
    raise RuntimeError(f"giving up after {max_retries} retries ({detail})")
