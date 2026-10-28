"""Unit tests for the safety kit: retry, checkpoint, dedup, missing ids."""

from __future__ import annotations

from pathlib import Path

import pytest

from fetcher.checkpoint import Checkpoint
from fetcher.dedup import count_missing, dedup_records, find_duplicate_ids, missing_ranges
from fetcher.retry import (
    AttemptResult,
    TokenBucket,
    backoff_seconds,
    call_with_retry,
    retry_after_seconds,
)
from fetcher.strategies import plan_date_partitions, plan_id_partitions


def test_retry_stops_on_400_and_does_not_sleep() -> None:
    calls = {"n": 0}
    sleeps: list[float] = []

    def do_request() -> AttemptResult:
        calls["n"] += 1
        return AttemptResult(400, {}, {"detail": "no"})

    result, retries = call_with_retry(do_request, max_retries=5, sleeper=sleeps.append)
    assert result.status_code == 400
    assert retries == 0
    assert calls["n"] == 1
    assert sleeps == []


def test_retry_honors_retry_after_then_succeeds() -> None:
    calls = {"n": 0}
    sleeps: list[float] = []

    def do_request() -> AttemptResult:
        calls["n"] += 1
        if calls["n"] < 3:
            return AttemptResult(429, {"Retry-After": "2"}, None)
        return AttemptResult(200, {}, {"ok": True})

    result, retries = call_with_retry(do_request, max_retries=5, sleeper=sleeps.append)
    assert result.status_code == 200
    assert retries == 2
    assert sleeps == [2.0, 2.0]


def test_retry_gives_up_on_repeated_503() -> None:
    def do_request() -> AttemptResult:
        return AttemptResult(503, {}, None)

    with pytest.raises(RuntimeError):
        call_with_retry(do_request, max_retries=2, sleeper=lambda _s: None)


def test_retry_on_timeout_error_string() -> None:
    calls = {"n": 0}

    def do_request() -> AttemptResult:
        calls["n"] += 1
        if calls["n"] == 1:
            return AttemptResult(0, {}, None, retryable_error="timeout: timed out")
        return AttemptResult(200, {}, {"ok": True})

    result, retries = call_with_retry(do_request, max_retries=3, sleeper=lambda _s: None)
    assert result.status_code == 200
    assert retries == 1


def test_retry_after_http_date() -> None:
    # A date in the past means "do not wait".
    assert retry_after_seconds({"Retry-After": "0.5"}) == 0.5
    waited = retry_after_seconds({"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}, now=2_000_000_000)
    assert waited == 0.0


def test_backoff_grows_and_stays_within_cap() -> None:
    import random

    rng = random.Random(1)
    first = backoff_seconds(0, base=0.25, cap=30, rng=rng)
    later = backoff_seconds(8, base=0.25, cap=2, rng=rng)
    assert 0 <= first <= 0.25
    assert 0 <= later <= 2


def test_token_bucket_blocks_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = TokenBucket(rate_per_second=1000, capacity=1)
    bucket.acquire()
    slept: list[float] = []
    monkeypatch.setattr("fetcher.retry.time.sleep", slept.append)
    monkeypatch.setattr("fetcher.retry.time.monotonic", lambda: 0.0)
    # Force an empty bucket and a frozen clock so acquire would spin. We only
    # check the first sleep by raising from sleep.
    bucket.tokens = 0
    bucket.updated = 0

    def stop(_seconds: float) -> None:
        raise RuntimeError("slept")

    monkeypatch.setattr("fetcher.retry.time.sleep", stop)
    with pytest.raises(RuntimeError, match="slept"):
        bucket.acquire()


def test_checkpoint_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    saved = Checkpoint(
        strategy="resilient",
        last_id=500,
        pages=4,
        records=400,
        requests=5,
        retries=1,
        run_dir=str(tmp_path),
    )
    saved.save(path)
    loaded = Checkpoint.load(path)
    assert loaded is not None
    assert loaded.last_id == 500
    assert loaded.records == 400
    assert Checkpoint.load(tmp_path / "missing.json") is None


def test_dedup_keeps_newest_updated_at() -> None:
    rows = [
        {"id": 1, "updated_at": "2024-01-01T00:00:00Z", "amount": 1},
        {"id": 1, "updated_at": "2024-06-01T00:00:00Z", "amount": 2},
        {"id": 2, "updated_at": "2024-01-01T00:00:00Z", "amount": 3},
    ]
    kept = {row["id"]: row for row in dedup_records(rows)}
    assert kept[1]["amount"] == 2
    assert kept[2]["amount"] == 3
    assert find_duplicate_ids(rows) == [1]


def test_missing_ranges() -> None:
    gaps = missing_ranges([1, 2, 5, 6], 1, 7)
    assert gaps == [(3, 4), (7, 7)]
    assert count_missing(gaps) == 3
    assert missing_ranges([1, 2, 3], 1, 3) == []


def test_partition_plans_cover_without_overlap() -> None:
    ranges = plan_id_partitions(1, 40, 4)
    assert ranges[0][0] == 1
    assert ranges[-1][1] == 40
    covered = []
    for start, end in ranges:
        covered.extend(range(start, end + 1))
    assert covered == list(range(1, 41))

    dates = plan_date_partitions("2024-01-01T00:00:00Z", "2024-01-01T00:00:09Z", 3)
    assert dates[0][0] == "2024-01-01T00:00:00Z"
    assert dates[-1][1] == "2024-01-01T00:00:09Z"
    # No shared second, no gap: next start is previous end + 1 second.
    for ( _start, end), (nxt, _nxt_end) in zip(dates, dates[1:]):
        assert nxt > end
