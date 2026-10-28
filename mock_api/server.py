"""A local API that behaves like a painful enterprise REST API.

What this file teaches
----------------------
Teams usually meet pagination because the first call came back with 1,000
rows and then stopped. That cap is deliberate. A single response that holds
every row can blow server memory, run past the gateway timeout, and starve
every other caller. This server refuses oversized pages, then offers every
common way to walk the rest of the data:

- /offset and /pages     easy, but slow at depth, and wrong if rows move
- /cursor                opaque token, consistent, cannot be parallelized
- /keyset                "id greater than the last id I saw", stays fast
- /linkheader            same walk, but the next URL is in a Link header
- /export                one file, which usually beats paging

Every painful behavior is a flag so you can turn it on when you want to see
it and off when you want a clean measurement. Defaults match the story in
the README: 1,000-row pages, 300 ms of latency, a deep-offset penalty, a
rate limit, and a small chance of 500/503.

Run it from the repo root:

    python -m mock_api.server
    python -m mock_api.server --records 5000 --latency-ms 20 --failure-rate 0
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import hmac
import json
import os
import random
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from mock_api.seed import connect, seed_database

ALLOWED_FIELDS = ("id", "updated_at", "category", "amount", "payload")
ISO_PREFIX = "YYYY-MM-DD"


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else float(raw)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else int(raw)


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    """Every behavior the lab needs to switch on or off."""

    db_path: str = "data/mock.db"
    records: int = 1_000_000
    seed: int = 42
    host: str = "127.0.0.1"
    port: int = 8000
    max_page_size: int = 1000
    latency_ms: float = 300.0
    # Extra delay added to offset/page queries: (offset / 1000) * this value.
    # This simulates a database that must walk and discard skipped rows.
    deep_offset_ms_per_1k: float = 25.0
    deep_offset_cap_ms: float = 2_000.0
    rate_limit_rps: float = 10.0
    failure_rate: float = 0.02
    timeout_rate: float = 0.005
    timeout_hang_s: float = 15.0
    live_data: bool = False
    live_every: int = 1
    admin: bool = True
    cursor_secret: str = "lab-cursor-secret"
    force_seed: bool = False

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            db_path=os.environ.get("MOCK_DB", "data/mock.db"),
            records=env_int("MOCK_RECORDS", 1_000_000),
            seed=env_int("MOCK_SEED", 42),
            host=os.environ.get("MOCK_HOST", "127.0.0.1"),
            port=env_int("MOCK_PORT", 8000),
            max_page_size=env_int("MOCK_MAX_PAGE_SIZE", 1000),
            latency_ms=env_float("MOCK_LATENCY_MS", 300.0),
            deep_offset_ms_per_1k=env_float("MOCK_DEEP_OFFSET_MS_PER_1K", 25.0),
            deep_offset_cap_ms=env_float("MOCK_DEEP_OFFSET_CAP_MS", 2_000.0),
            rate_limit_rps=env_float("MOCK_RATE_LIMIT_RPS", 10.0),
            failure_rate=env_float("MOCK_FAILURE_RATE", 0.02),
            timeout_rate=env_float("MOCK_TIMEOUT_RATE", 0.005),
            timeout_hang_s=env_float("MOCK_TIMEOUT_HANG_S", 15.0),
            live_data=env_flag("MOCK_LIVE_DATA", False),
            live_every=env_int("MOCK_LIVE_EVERY", 1),
            admin=env_flag("MOCK_ADMIN", True),
            cursor_secret=os.environ.get("MOCK_CURSOR_SECRET", "lab-cursor-secret"),
        )


class RateLimitExceeded(Exception):
    def __init__(self, retry_after: float, limit: float) -> None:
        self.retry_after = retry_after
        self.limit = limit


class ServerLimiter:
    """Token bucket per client. rate_limit_rps <= 0 disables the limit."""

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.capacity = max(1.0, rate) if rate > 0 else 0.0
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, client_id: str) -> None:
        if self.rate <= 0:
            return
        now = time.monotonic()
        with self._lock:
            tokens, updated = self._buckets.get(client_id, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - updated) * self.rate)
            if tokens < 1.0:
                retry_after = (1.0 - tokens) / self.rate
                self._buckets[client_id] = (tokens, now)
                raise RateLimitExceeded(retry_after, self.rate)
            self._buckets[client_id] = (tokens - 1.0, now)


def encode_cursor(last_id: int, secret: str) -> str:
    """Opaque, signed token. Clients should treat it as a blob, not a number.

    Signing is the reason a cursor walk cannot be partitioned: you cannot
    invent the token for id=500000. Keyset pagination exposes that id on
    purpose. Cursor pagination hides it on purpose.
    """
    payload = json.dumps({"v": 1, "last_id": last_id}, separators=(",", ":")).encode()
    signature = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()[:20]
    body = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return f"{body}.{signature}"


def decode_cursor(token: str, secret: str) -> int:
    try:
        body, signature = token.split(".", 1)
        padded = body + "=" * (-len(body) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="cursor could not be decoded") from exc
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()[:20]
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(
            status_code=400,
            detail=(
                "cursor signature mismatch. Cursors are opaque; do not forge the "
                "next page. Use /keyset if you need to start at a chosen id."
            ),
        )
    try:
        return int(json.loads(payload)["last_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="cursor payload is invalid") from exc


def _validate_iso(value: str | None, name: str) -> str | None:
    """Reject anything that is not a date or an ISO-8601 UTC timestamp.

    Filter values are interpolated into SQL only as bound parameters, but
    validating them keeps a typo from silently matching zero rows.
    """
    if value is None or value == "":
        return None
    if len(value) == 10:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"{name} must look like {ISO_PREFIX}") from exc
        return value
    if len(value) == 20 and value.endswith("Z"):
        try:
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=f"{name} must look like 2024-01-31T00:00:00Z"
            ) from exc
        return value
    raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ")


def _parse_fields(raw: str | None) -> list[str] | None:
    if raw is None or raw.strip() == "":
        return None
    fields = [part.strip() for part in raw.split(",") if part.strip()]
    unknown = [name for name in fields if name not in ALLOWED_FIELDS]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"unknown fields {unknown}; allowed: {', '.join(ALLOWED_FIELDS)}",
        )
    # id is always returned. Without it the client cannot do keyset pagination
    # or dedup, so hiding it would manufacture data loss.
    if "id" not in fields:
        fields.insert(0, "id")
    return fields


def _project(row: sqlite3.Row, fields: list[str] | None) -> dict:
    full = {
        "id": row["id"],
        "updated_at": row["updated_at"],
        "category": row["category"],
        "amount": row["amount"],
        "payload": row["payload"],
    }
    if fields is None:
        return full
    return {name: full[name] for name in fields}


def create_app(settings: Settings) -> FastAPI:
    """Build the app. Tests call this with a tiny database and zero latency."""
    app = FastAPI(
        title="Pagination Lab Mock API",
        description=(
            "A teaching API. It limits page size, adds latency, rate-limits, "
            "and occasionally fails, so you can practice pulling data safely."
        ),
        version="1.0.0",
    )
    app.state.settings = settings
    app.state.limiter = ServerLimiter(settings.rate_limit_rps)
    app.state.write_lock = threading.Lock()
    app.state.live_requests = 0
    app.state.stats = {"requests": 0, "rate_limited": 0, "failures": 0, "timeouts": 0}
    app.state.jobs: dict[str, dict] = {}
    app.state.flaky_hits: dict[str, int] = {}
    app.state.export_dir = Path(settings.db_path).parent / "exports"
    app.state.export_dir.mkdir(parents=True, exist_ok=True)

    # One connection per thread. SQLite connections are not happy when shared.
    local = threading.local()

    def db() -> sqlite3.Connection:
        conn = getattr(local, "conn", None)
        if conn is None:
            conn = connect(settings.db_path)
            local.conn = conn
        return conn

    def cached_total() -> int:
        row = db().execute("SELECT COUNT(*) AS n FROM records").fetchone()
        return int(row["n"])

    app.state.total = cached_total()

    def client_id(request: Request) -> str:
        return request.headers.get("x-client-id") or (
            request.client.host if request.client else "unknown"
        )

    def gate(request: Request, response: Response, offset: int = 0, deep: bool = False) -> None:
        """Apply rate limit, injected failures, and artificial latency."""
        app.state.stats["requests"] += 1
        try:
            app.state.limiter.check(client_id(request))
        except RateLimitExceeded as exc:
            app.state.stats["rate_limited"] += 1
            retry_after = max(1, ceil(exc.retry_after))
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limited",
                    "message": (
                        "Too many requests. Adding more threads will not help "
                        "until the limit itself is higher."
                    ),
                    "retry_after": retry_after,
                    "limit_rps": exc.limit,
                },
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(int(exc.limit)),
                    "X-RateLimit-Remaining": "0",
                },
            ) from exc
        roll = random.random()
        if roll < settings.failure_rate:
            app.state.stats["failures"] += 1
            code = 500 if roll < settings.failure_rate / 2 else 503
            raise HTTPException(
                status_code=code,
                detail={
                    "error": "injected_failure",
                    "message": "Random upstream failure. A production pull retries this.",
                },
            )
        if random.random() < settings.timeout_rate:
            app.state.stats["timeouts"] += 1
            time.sleep(settings.timeout_hang_s)
        delay_ms = settings.latency_ms
        if deep and offset > 0 and settings.deep_offset_ms_per_1k > 0:
            extra = (offset / 1000.0) * settings.deep_offset_ms_per_1k
            delay_ms += min(settings.deep_offset_cap_ms, extra)
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        response.headers["X-Query-Delay-Ms"] = str(int(delay_ms))
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_rps)
        response.headers["X-Total-Count"] = str(app.state.total)

    def require_limit(limit: int | None) -> tuple[int, bool]:
        """Return (page_size, truncated_because_caller_asked_for_everything)."""
        if limit is None:
            return settings.max_page_size, True
        if limit < 1:
            raise HTTPException(status_code=400, detail="limit must be >= 1")
        if limit > settings.max_page_size:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "page_size_exceeded",
                    "message": (
                        f"This API refuses to return more than {settings.max_page_size} "
                        "records in one response."
                    ),
                    "reasons": [
                        "Encoding a huge JSON array can exhaust server memory.",
                        "The gateway will time out before a giant body finishes.",
                        "One client downloading everything starves other users.",
                        "A dropped connection forces the entire giant response to be retried.",
                    ],
                    "max_page_size": settings.max_page_size,
                    "hint": "Page with /offset, /cursor, or /keyset, or start a bulk job at POST /export.",
                },
            )
        return limit, False

    def maybe_drift(offset: int) -> None:
        """Move and delete rows so offset pagination skips and repeats.

        Offset means "skip N rows in whatever order the table has right now".
        If a not-yet-seen row jumps backward (its updated_at becomes old), it
        lands in the part of the list the client already consumed. The next
        offset then repeats a row and never sees the one that jumped. Keyset
        and cursor walks keyed by id do not use that moving order, so the same
        mutation does not skip them.
        """
        if not settings.live_data or offset <= 0:
            return
        app.state.live_requests += 1
        if app.state.live_requests % max(1, settings.live_every) != 0:
            return
        conn = db()
        with app.state.write_lock:
            mover = conn.execute(
                "SELECT id FROM records ORDER BY updated_at, id LIMIT 1 OFFSET ?",
                (offset + 5,),
            ).fetchone()
            if mover is not None:
                conn.execute(
                    "UPDATE records SET updated_at = ? WHERE id = ?",
                    ("2000-01-01T00:00:00Z", mover["id"]),
                )
                conn.execute(
                    "INSERT INTO drift_events (ts, action, record_id, detail) VALUES (?, ?, ?, ?)",
                    (
                        utcnow(),
                        "move_backward",
                        mover["id"],
                        f"updated_at moved to the past while the client was at offset {offset}",
                    ),
                )
            victim = conn.execute(
                "SELECT id FROM records ORDER BY id LIMIT 1 OFFSET ?",
                (offset + 2,),
            ).fetchone()
            if victim is not None:
                conn.execute("DELETE FROM records WHERE id = ?", (victim["id"],))
                conn.execute(
                    "INSERT INTO drift_events (ts, action, record_id, detail) VALUES (?, ?, ?, ?)",
                    (
                        utcnow(),
                        "delete",
                        victim["id"],
                        f"deleted while the client was at offset {offset}",
                    ),
                )
                app.state.total = max(0, app.state.total - 1)
            conn.commit()

    def fetch_rows(
        limit: int,
        *,
        offset: int | None = None,
        after_id: int | None = None,
        end_id: int | None = None,
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        order: str = "id",
    ) -> tuple[list[sqlite3.Row], bool]:
        """Return up to `limit` rows and whether another row exists after them.

        Fetching limit+1 is the stopping condition that does not trust a total
        count. Counts lie when rows are inserted during the pull.
        """
        where: list[str] = []
        params: list[object] = []
        if after_id is not None:
            where.append("id > ?")
            params.append(after_id)
        if end_id is not None:
            where.append("id <= ?")
            params.append(end_id)
        since = _validate_iso(updated_since, "updated_since")
        start = _validate_iso(start_date, "start_date")
        end = _validate_iso(end_date, "end_date")
        # A date-only updated_since means "that whole day starts here".
        if since and len(since) == 10:
            since = since + "T00:00:00Z"
        if start and len(start) == 10:
            start = start + "T00:00:00Z"
        if end and len(end) == 10:
            end = end + "T23:59:59Z"
        lower = since or start
        if lower:
            where.append("updated_at >= ?")
            params.append(lower)
        if end:
            where.append("updated_at <= ?")
            params.append(end)
        sql = "SELECT id, updated_at, category, amount, payload FROM records"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at, id" if order == "updated_at" else " ORDER BY id"
        # Ask for one extra row. Its presence is has_more. Do not return it.
        if offset is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit + 1, offset])
        else:
            sql += " LIMIT ?"
            params.append(limit + 1)
        rows = list(db().execute(sql, params).fetchall())
        has_more = len(rows) > limit
        return rows[:limit], has_more

    def filtered_total(**filters: str | None) -> int:
        if not any(filters.values()):
            return int(app.state.total)
        where: list[str] = []
        params: list[object] = []
        since = _validate_iso(filters.get("updated_since"), "updated_since")
        start = _validate_iso(filters.get("start_date"), "start_date")
        end = _validate_iso(filters.get("end_date"), "end_date")
        if since and len(since) == 10:
            since = since + "T00:00:00Z"
        if start and len(start) == 10:
            start = start + "T00:00:00Z"
        if end and len(end) == 10:
            end = end + "T23:59:59Z"
        lower = since or start
        if lower:
            where.append("updated_at >= ?")
            params.append(lower)
        if end:
            where.append("updated_at <= ?")
            params.append(end)
        if filters.get("end_id") is not None:
            where.append("id <= ?")
            params.append(int(filters["end_id"]))
        sql = "SELECT COUNT(*) AS n FROM records"
        if where:
            sql += " WHERE " + " AND ".join(where)
        return int(db().execute(sql, params).fetchone()["n"])

    def envelope(
        rows: list[sqlite3.Row],
        *,
        limit: int,
        total: int,
        has_more: bool,
        fields: list[str] | None,
        ordering: str,
        truncated: bool = False,
        next_cursor: str | None = None,
        next_offset: int | None = None,
    ) -> dict:
        data = [_project(row, fields) for row in rows]
        return {
            "data": data,
            "page_size": limit,
            "returned": len(data),
            "total": total,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "next_offset": next_offset,
            "ordering": ordering,
            "live_data": settings.live_data,
            "truncated": truncated,
        }

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        # FastAPI's default handler drops headers passed via HTTPException in
        # some versions. Setting them explicitly keeps Retry-After visible.
        headers = dict(exc.headers or {})
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=headers)

    @app.get("/")
    def root() -> dict:
        return {
            "service": "pagination-lab-mock-api",
            "records": app.state.total,
            "max_page_size": settings.max_page_size,
            "docs": "/docs",
            "endpoints": [
                "/count",
                "/offset",
                "/pages",
                "/cursor",
                "/keyset",
                "/linkheader",
                "/export",
                "/health",
                "/stats",
            ],
            "why_page_size_is_capped": (
                "Server memory, gateway timeouts, fairness, and the cost of retrying a huge body."
            ),
        }

    @app.get("/health")
    def health() -> dict:
        return {
            "status": "ok",
            "server_time": utcnow(),
            "records": app.state.total,
            "config": {
                "max_page_size": settings.max_page_size,
                "latency_ms": settings.latency_ms,
                "deep_offset_ms_per_1k": settings.deep_offset_ms_per_1k,
                "rate_limit_rps": settings.rate_limit_rps,
                "failure_rate": settings.failure_rate,
                "timeout_rate": settings.timeout_rate,
                "live_data": settings.live_data,
            },
        }

    @app.get("/stats")
    def stats() -> dict:
        return dict(app.state.stats)

    @app.get("/count")
    def count(
        request: Request,
        response: Response,
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        gate(request, response)
        total = filtered_total(
            updated_since=updated_since, start_date=start_date, end_date=end_date
        )
        return {"total": total, "server_time": utcnow()}

    @app.get("/offset")
    def offset_page(
        request: Request,
        response: Response,
        limit: int | None = Query(default=None),
        offset: int = Query(default=0, ge=0),
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | None = None,
    ) -> dict:
        page_size, truncated = require_limit(limit)
        # Live mode sorts by a mutable column so you can watch offset drift.
        # The normal path sorts by id, which is what makes deep OFFSET slow
        # and, absent deletes, stable.
        ordering = "updated_at" if settings.live_data else "id"
        gate(request, response, offset=offset, deep=True)
        # Drift after the gate so a 429 does not mutate rows the client never saw.
        if settings.live_data:
            maybe_drift(offset)
        rows, has_more = fetch_rows(
            page_size,
            offset=offset,
            updated_since=updated_since,
            start_date=start_date,
            end_date=end_date,
            order=ordering,
        )
        total = filtered_total(
            updated_since=updated_since, start_date=start_date, end_date=end_date
        )
        next_offset = offset + len(rows) if has_more else None
        return envelope(
            rows,
            limit=page_size,
            total=total,
            has_more=has_more,
            fields=_parse_fields(fields),
            ordering=ordering,
            truncated=truncated,
            next_offset=next_offset,
        )

    @app.get("/pages")
    def page_number(
        request: Request,
        response: Response,
        page: int = Query(default=1, ge=1),
        page_size: int | None = Query(default=None),
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | None = None,
    ) -> dict:
        """Page numbers are offset pagination wearing a friendlier name.

        page 1 is the first page. Some APIs use page 0. That off-by-one is a
        classic way to skip or duplicate a whole page, so this server rejects
        page 0 and documents the choice.
        """
        size, truncated = require_limit(page_size)
        offset = (page - 1) * size
        ordering = "updated_at" if settings.live_data else "id"
        gate(request, response, offset=offset, deep=True)
        if settings.live_data:
            maybe_drift(offset)
        rows, has_more = fetch_rows(
            size,
            offset=offset,
            updated_since=updated_since,
            start_date=start_date,
            end_date=end_date,
            order=ordering,
        )
        total = filtered_total(
            updated_since=updated_since, start_date=start_date, end_date=end_date
        )
        return envelope(
            rows,
            limit=size,
            total=total,
            has_more=has_more,
            fields=_parse_fields(fields),
            ordering=ordering,
            truncated=truncated,
            next_offset=offset + len(rows) if has_more else None,
        )

    @app.get("/cursor")
    def cursor_page(
        request: Request,
        response: Response,
        limit: int | None = Query(default=None),
        cursor: str | None = None,
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | None = None,
    ) -> dict:
        page_size, truncated = require_limit(limit)
        after_id = decode_cursor(cursor, settings.cursor_secret) if cursor else 0
        gate(request, response)
        rows, has_more = fetch_rows(
            page_size,
            after_id=after_id,
            updated_since=updated_since,
            start_date=start_date,
            end_date=end_date,
            order="id",
        )
        next_cursor = encode_cursor(int(rows[-1]["id"]), settings.cursor_secret) if has_more and rows else None
        total = filtered_total(
            updated_since=updated_since, start_date=start_date, end_date=end_date
        )
        return envelope(
            rows,
            limit=page_size,
            total=total,
            has_more=has_more,
            fields=_parse_fields(fields),
            ordering="id",
            truncated=truncated,
            next_cursor=next_cursor,
        )

    @app.get("/keyset")
    def keyset_page(
        request: Request,
        response: Response,
        limit: int | None = Query(default=None),
        after_id: int = Query(default=0, ge=0),
        end_id: int | None = Query(default=None),
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | None = None,
    ) -> dict:
        """Seek to the first id greater than after_id. No skipped rows.

        The database can satisfy `WHERE id > ? ORDER BY id LIMIT ?` with the
        primary-key index. OFFSET 900000 cannot: it still walks those 900000
        index entries and throws them away. That is why keyset stays fast at
        any depth, and why it can be split into id ranges and run in parallel.
        """
        page_size, truncated = require_limit(limit)
        gate(request, response)
        rows, has_more = fetch_rows(
            page_size,
            after_id=after_id,
            end_id=end_id,
            updated_since=updated_since,
            start_date=start_date,
            end_date=end_date,
            order="id",
        )
        total = filtered_total(
            updated_since=updated_since,
            start_date=start_date,
            end_date=end_date,
            end_id=str(end_id) if end_id is not None else None,
        )
        return envelope(
            rows,
            limit=page_size,
            total=total,
            has_more=has_more,
            fields=_parse_fields(fields),
            ordering="id",
            truncated=truncated,
        )

    @app.get("/linkheader")
    def link_header_page(
        request: Request,
        response: Response,
        limit: int | None = Query(default=None),
        cursor: str | None = None,
        fields: str | None = None,
    ) -> dict:
        """RFC 5988: the next page is a URL in the Link header, not the body.

        GitHub's API made this style famous. Clients that only look at JSON
        will silently stop after page 1, which is its own kind of data loss.
        """
        page_size, truncated = require_limit(limit)
        after_id = decode_cursor(cursor, settings.cursor_secret) if cursor else 0
        gate(request, response)
        rows, has_more = fetch_rows(page_size, after_id=after_id, order="id")
        links = [f'<{request.base_url}linkheader?limit={page_size}>; rel="first"']
        next_cursor = None
        if has_more and rows:
            next_cursor = encode_cursor(int(rows[-1]["id"]), settings.cursor_secret)
            links.append(
                f'<{request.base_url}linkheader?limit={page_size}&cursor={next_cursor}>; rel="next"'
            )
        response.headers["Link"] = ", ".join(links)
        return envelope(
            rows,
            limit=page_size,
            total=int(app.state.total),
            has_more=has_more,
            fields=_parse_fields(fields),
            ordering="id",
            truncated=truncated,
            next_cursor=next_cursor,
        )

    @app.get("/flaky")
    def flaky(request: Request, response: Response, fail_times: int = 2) -> dict:
        """Fail the first N calls for this client, then succeed. For retry drills."""
        gate(request, response)
        key = client_id(request)
        app.state.flaky_hits[key] = app.state.flaky_hits.get(key, 0) + 1
        seen = app.state.flaky_hits[key]
        if seen <= fail_times:
            raise HTTPException(
                status_code=503,
                detail={"error": "flaky", "attempt": seen, "fail_times": fail_times},
            )
        return {"ok": True, "attempt": seen}

    @app.get("/bounds")
    def bounds(request: Request, response: Response) -> dict:
        """Min/max id and updated_at. Partitioned pulls use this to cut ranges."""
        gate(request, response)
        row = db().execute(
            "SELECT MIN(id) AS min_id, MAX(id) AS max_id, "
            "MIN(updated_at) AS min_updated, MAX(updated_at) AS max_updated FROM records"
        ).fetchone()
        return {
            "min_id": row["min_id"],
            "max_id": row["max_id"],
            "min_updated": row["min_updated"],
            "max_updated": row["max_updated"],
            "total": app.state.total,
        }

    @app.get("/drift")
    def drift() -> dict:
        rows = db().execute(
            "SELECT id, ts, action, record_id, detail FROM drift_events ORDER BY id"
        ).fetchall()
        return {
            "events": [dict(row) for row in rows],
            "total": app.state.total,
        }

    @app.post("/admin/touch")
    def touch(n: int = Query(default=100, ge=1, le=100_000)) -> dict:
        """Bump updated_at on N rows so the next incremental load has work to do."""
        if not settings.admin:
            raise HTTPException(status_code=404, detail="admin endpoints are disabled")
        now = utcnow()
        conn = db()
        with app.state.write_lock:
            ids = [
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM records ORDER BY id DESC LIMIT ?", (n,)
                ).fetchall()
            ]
            conn.executemany(
                "UPDATE records SET updated_at = ?, amount = amount + 1 WHERE id = ?",
                [(now, record_id) for record_id in ids],
            )
            conn.commit()
        return {"touched": len(ids), "updated_at": now}

    def _export_iter(conn: sqlite3.Connection, filters: dict):
        where: list[str] = []
        params: list[object] = []
        since = filters.get("updated_since")
        start = filters.get("start_date")
        end = filters.get("end_date")
        lower = since or start
        if lower:
            where.append("updated_at >= ?")
            params.append(lower if "T" in lower else lower + "T00:00:00Z")
        if end:
            where.append("updated_at <= ?")
            params.append(end if "T" in end else end + "T23:59:59Z")
        sql = "SELECT id, updated_at, category, amount, payload FROM records"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id"
        yield from conn.execute(sql, params)

    def _run_export_stream(job_id: str, filters: dict, fields: list[str] | None) -> None:
        job = app.state.jobs[job_id]
        job["status"] = "running"
        path = app.state.export_dir / f"{job_id}.ndjson.gz"
        conn = connect(settings.db_path)
        try:
            written = 0
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                for row in _export_iter(conn, filters):
                    handle.write(json.dumps(_project(row, fields), separators=(",", ":")) + "\n")
                    written += 1
            job["records"] = written
            job["status"] = "complete"
            job["file"] = str(path)
        except Exception as exc:
            # The poll endpoint is how the client sees this. Swallowing it
            # here would hide a failed job behind a thread that just died.
            job["status"] = "failed"
            job["error"] = str(exc)
        finally:
            conn.close()

    @app.post("/export")
    def start_export(
        request: Request,
        response: Response,
        updated_since: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | None = None,
    ) -> dict:
        """Start a bulk file. Poll /export/{id}, then download the gzipped NDJSON.

        This is usually the fastest honest option. The server walks the table
        once and writes a file. You pay latency for a handful of calls, not
        for every page.
        """
        gate(request, response)
        job_id = uuid.uuid4().hex
        selected = _parse_fields(fields)
        filters = {
            "updated_since": _validate_iso(updated_since, "updated_since"),
            "start_date": _validate_iso(start_date, "start_date"),
            "end_date": _validate_iso(end_date, "end_date"),
        }
        app.state.jobs[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "records": 0,
            "error": None,
            "file": None,
            "created_at": utcnow(),
        }
        threading.Thread(
            target=_run_export_stream,
            args=(job_id, filters, selected),
            daemon=True,
        ).start()
        return {"job_id": job_id, "status": "pending", "poll": f"/export/{job_id}"}

    @app.get("/export/{job_id}")
    def export_status(job_id: str, request: Request, response: Response) -> dict:
        gate(request, response)
        job = app.state.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown export job")
        return {
            "job_id": job_id,
            "status": job["status"],
            "records": job["records"],
            "error": job["error"],
            "download": f"/export/{job_id}/download" if job["status"] == "complete" else None,
        }

    @app.get("/export/{job_id}/download")
    def export_download(job_id: str, request: Request, response: Response):
        gate(request, response)
        job = app.state.jobs.get(job_id)
        if job is None or job["status"] != "complete" or not job.get("file"):
            raise HTTPException(status_code=409, detail="export is not ready")
        return FileResponse(
            job["file"],
            media_type="application/gzip",
            filename=f"{job_id}.ndjson.gz",
        )

    from fastapi.middleware.gzip import GZipMiddleware

    # Compress large JSON pages when the client sends Accept-Encoding: gzip.
    # httpx does that by default. Field selection plus gzip is how you shrink
    # a pull without skipping rows.
    app.add_middleware(GZipMiddleware, minimum_size=800)
    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the pagination-lab mock API")
    parser.add_argument("--db", default=None)
    parser.add_argument("--records", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--max-page-size", type=int, default=None)
    parser.add_argument("--latency-ms", type=float, default=None)
    parser.add_argument("--deep-offset-ms-per-1k", type=float, default=None)
    parser.add_argument("--deep-offset-cap-ms", type=float, default=None)
    parser.add_argument("--rate-limit-rps", type=float, default=None)
    parser.add_argument("--failure-rate", type=float, default=None)
    parser.add_argument("--timeout-rate", type=float, default=None)
    parser.add_argument("--timeout-hang-s", type=float, default=None)
    parser.add_argument("--live-every", type=int, default=None)
    parser.add_argument("--live-data", action="store_true")
    parser.add_argument("--no-admin", action="store_true")
    parser.add_argument("--force-seed", action="store_true")
    return parser


def settings_from_args(argv: list[str] | None = None) -> Settings:
    settings = Settings.from_env()
    args = build_parser().parse_args(argv)
    mapping = {
        "db_path": args.db,
        "records": args.records,
        "seed": args.seed,
        "host": args.host,
        "port": args.port,
        "max_page_size": args.max_page_size,
        "latency_ms": args.latency_ms,
        "deep_offset_ms_per_1k": args.deep_offset_ms_per_1k,
        "deep_offset_cap_ms": args.deep_offset_cap_ms,
        "rate_limit_rps": args.rate_limit_rps,
        "failure_rate": args.failure_rate,
        "timeout_rate": args.timeout_rate,
        "timeout_hang_s": args.timeout_hang_s,
        "live_every": args.live_every,
    }
    for name, value in mapping.items():
        if value is not None:
            setattr(settings, name, value)
    if args.live_data:
        settings.live_data = True
    if args.no_admin:
        settings.admin = False
    if args.force_seed:
        settings.force_seed = True
    return settings


def main(argv: list[str] | None = None) -> None:
    settings = settings_from_args(argv)
    seed_database(
        settings.db_path,
        count=settings.records,
        seed=settings.seed,
        force=settings.force_seed,
    )
    pages = ceil(settings.records / settings.max_page_size)
    sequential_s = pages * (settings.latency_ms / 1000.0)
    print(
        f"[api] {settings.records:,} records, page cap {settings.max_page_size}, "
        f"latency {settings.latency_ms} ms, rate limit {settings.rate_limit_rps} rps"
    )
    print(
        f"[api] a sequential pull is about {pages:,} requests, "
        f"~{sequential_s:.0f}s before deep-offset penalty and retries"
    )
    print(f"[api] listening on http://{settings.host}:{settings.port}  (docs at /docs)")
    app = create_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")


if __name__ == "__main__":
    main()
