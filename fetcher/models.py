"""Small data objects shared by every strategy.

A Page is the fetcher's view of one API response, after the adapter has
unwrapped whatever envelope that API uses. Strategies should not care whether
the wire format was FDIC's `data[].data` or the mock API's flat list.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Page:
    """One page of records, plus the hints the next request needs."""

    records: list[dict]
    total: int | None
    has_more: bool
    next_cursor: str | None = None
    next_offset: int | None = None
    last_id: int | None = None
    request_ms: float = 0.0
    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    raw_bytes: int = 0


@dataclass
class RunResult:
    """What one strategy run accomplished. Benchmark and validate read this."""

    strategy: str
    pages: int
    records: int
    requests: int
    retries: int
    errors: int
    elapsed_s: float
    run_dir: str
    parquet_path: str = ""
    manifest_path: str = ""
    duplicates: int = 0
    missing: int = 0
    notes: str = ""
    resumed_from_id: int = 0
    checksum: str = ""


@dataclass
class ClientConfig:
    """Knobs for a fetch run. Defaults match the mock API's page cap."""

    base_url: str = "http://127.0.0.1:8000"
    page_size: int = 1000
    timeout_s: float = 30.0
    max_retries: int = 6
    concurrency: int = 8
    partitions: int = 8
    # 0 means "do not client-limit". The server may still return 429.
    rate_limit_rps: float = 0.0
    run_id: str = ""
    out_dir: Path = Path("data/landing")
    fields: str | None = None
    # Used by the automated crash test. Production resumes from the last
    # checkpoint even if the process is killed, so this is only a convenience.
    crash_after_pages: int | None = None
    partition_by: str = "id"
    adapter: str = "mock"
    # Tests inject httpx.ASGITransport so strategies can run without a port.
    transport: object | None = None
