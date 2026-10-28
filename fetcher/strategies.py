"""One function per way of pulling the dataset.

Read these in order: naive, offset, cursor, keyset, then the faster and safer
variants. Each function streams pages to disk through a LandingZone. None of
them accumulate the full dataset in a list.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path

import httpx

from fetcher.checkpoint import Checkpoint
from fetcher.client import MockClient
from fetcher.landing import LandingZone
from fetcher.models import ClientConfig, Page, RunResult
from fetcher.progress import Progress
from fetcher.retry import RETRYABLE_STATUS, TokenBucket, backoff_seconds, retry_after_seconds


def _run_dir(config: ClientConfig, strategy: str) -> Path:
    run_id = config.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return Path(config.out_dir) / strategy / run_id


def _finish(
    strategy: str,
    progress: Progress,
    landing: LandingZone,
    client: MockClient,
    notes: str = "",
    resumed_from_id: int = 0,
) -> RunResult:
    parquet, parquet_rows = landing.compact_parquet(dedup=True)
    elapsed = progress.elapsed
    manifest = landing.write_manifest(
        {
            "strategy": strategy,
            "elapsed_s": round(elapsed, 3),
            "pages": progress.pages,
            "records": progress.records,
            "parquet_records": parquet_rows,
            "requests": client.requests,
            "retries": client.retries,
            "errors": progress.errors,
            "notes": notes,
            "resumed_from_id": resumed_from_id,
        }
    )
    print(
        f"[{strategy}] finished records={progress.records:,} pages={progress.pages} "
        f"retries={client.retries} elapsed={elapsed:.1f}s parquet={parquet}",
        flush=True,
    )
    return RunResult(
        strategy=strategy,
        pages=progress.pages,
        records=progress.records,
        requests=client.requests,
        retries=client.retries,
        errors=progress.errors,
        elapsed_s=elapsed,
        run_dir=str(landing.run_dir),
        parquet_path=str(parquet),
        manifest_path=str(manifest),
        notes=notes,
        resumed_from_id=resumed_from_id,
        checksum=landing.checksum,
    )


def _tick(progress: Progress, page: Page, detail: str = "") -> None:
    if page.total is not None:
        progress.set_total(page.total)
    progress.tick(len(page.records), detail=detail)


def run_naive(config: ClientConfig) -> RunResult:
    """Ask for every row in one call. The server refuses, which is the point."""
    progress = Progress("naive")
    landing = LandingZone(_run_dir(config, "naive"))
    client = MockClient(config, progress)
    notes = []
    try:
        print(
            "[naive] requesting limit=10000000. APIs reject this because a single "
            "response can exhaust memory, hit a gateway timeout, and unfairly hog the server.",
            flush=True,
        )
        response = client.get_raw("/offset", {"limit": 10_000_000})
        client.requests += 1
        body = response.json()
        print(f"[naive] HTTP {response.status_code}: {json.dumps(body)[:700]}", flush=True)
        notes.append(f"huge page rejected with HTTP {response.status_code}")
        # The call their teammates actually made: no explicit huge limit, just
        # "give me the data", which silently stops at the page cap.
        page = client.fetch_offset(0, config.page_size)
        _tick(progress, page, detail="truncated_at_page_cap" if page.records else "empty")
        landing.write_page("offset_0", page.records)
        total = page.total or 0
        lost_pct = (100.0 * (total - len(page.records)) / total) if total else 0.0
        notes.append(
            f"one allowed page returned {len(page.records):,} of {total:,} records. "
            f"Stopping here loses the other {lost_pct:.1f}%."
        )
        print(f"[naive] {notes[-1]}", flush=True)
        return _finish("naive", progress, landing, client, notes=" ".join(notes))
    finally:
        client.close()


def run_offset(config: ClientConfig, filters: dict | None = None) -> RunResult:
    """Classic offset loop. Simple, and wrong when rows move under you.

    Stopping condition: the server says has_more is false, or the page is empty.
    Do not stop only because offset >= total. A total counted at the start is
    already stale if the table is live.
    """
    progress = Progress("offset")
    landing = LandingZone(_run_dir(config, "offset"))
    client = MockClient(config, progress)
    filters = filters or {}
    try:
        offset = 0
        while True:
            page = client.fetch_offset(offset, config.page_size, **filters)
            landing.write_page(f"offset_{offset:09d}", page.records)
            _tick(progress, page, detail=f"offset={offset}")
            if not page.records or not page.has_more:
                break
            # Advance by rows received, not by the requested page size. A short
            # page means the server is done. Advancing by page_size anyway can
            # skip a row when the page was short for any other reason.
            offset += len(page.records)
        return _finish("offset", progress, landing, client)
    finally:
        client.close()


def run_cursor(config: ClientConfig, filters: dict | None = None) -> RunResult:
    """Follow the opaque next token. Page N cannot start until page N-1 returns.

    That dependency is why cursor pagination cannot be fanned out across
    threads. You do not know the next token, and a signed token cannot be forged.
    """
    progress = Progress("cursor")
    landing = LandingZone(_run_dir(config, "cursor"))
    client = MockClient(config, progress)
    filters = filters or {}
    try:
        cursor = None
        page_index = 0
        while True:
            page = client.fetch_cursor(cursor, config.page_size, **filters)
            landing.write_page(f"cursor_{page_index:05d}", page.records)
            _tick(progress, page, detail="cursor" if cursor else "first")
            page_index += 1
            if not page.records or not page.has_more or not page.next_cursor:
                break
            cursor = page.next_cursor
        return _finish(
            "cursor",
            progress,
            landing,
            client,
            notes="sequential on purpose: each cursor is minted by the previous response",
        )
    finally:
        client.close()


def run_keyset(config: ClientConfig, filters: dict | None = None, label: str = "keyset") -> RunResult:
    """WHERE id > last_seen_id. Fast at any depth because the index seeks."""
    progress = Progress(label)
    landing = LandingZone(_run_dir(config, label))
    client = MockClient(config, progress)
    filters = dict(filters or {})
    try:
        after_id = 0
        while True:
            page = client.fetch_keyset(after_id, config.page_size, **filters)
            landing.write_page(f"after_{after_id:09d}", page.records)
            _tick(progress, page, detail=f"after_id={after_id}")
            if not page.records or not page.has_more or page.last_id is None:
                break
            if page.last_id <= after_id:
                # A cursor that does not move would loop forever. Stop and let
                # validation report the shortfall instead of hanging.
                break
            after_id = page.last_id
        return _finish(label, progress, landing, client, notes="keyset seek; cost does not grow with depth")
    finally:
        client.close()


async def _async_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict,
    semaphore: asyncio.Semaphore,
    bucket: TokenBucket | None,
    max_retries: int,
    progress: Progress,
) -> dict:
    """GET one page with the same retry rules as the synchronous client."""
    async with semaphore:
        for attempt in range(max_retries + 1):
            if bucket is not None:
                await asyncio.to_thread(bucket.acquire)
            try:
                response = await client.get(path, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt >= max_retries:
                    raise RuntimeError(f"giving up after transport errors: {exc}") from exc
                delay = backoff_seconds(attempt)
                progress.note_retry()
                print(f"[retry] async waiting={delay:.2f}s reason={exc}", flush=True)
                await asyncio.sleep(delay)
                continue
            if response.status_code in RETRYABLE_STATUS:
                if attempt >= max_retries:
                    raise RuntimeError(f"giving up after HTTP {response.status_code}")
                hinted = retry_after_seconds(dict(response.headers))
                delay = hinted if hinted is not None else backoff_seconds(attempt)
                progress.note_retry()
                print(
                    f"[retry] async HTTP {response.status_code} waiting={delay:.2f}s",
                    flush=True,
                )
                await asyncio.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
    raise RuntimeError("retry loop exited without a response")


def run_parallel_offset(config: ClientConfig) -> RunResult:
    """Fetch independent offset pages at the same time.

    This is safe only when each page is addressed by its own offset and you
    accept offset's consistency problems. It is not safe for cursors: there is
    no page-5 cursor until page 4 has been seen.

    A semaphore caps in-flight requests. Without a client-side rate limiter,
    a low server limit turns extra concurrency into 429s. Watch the retry count.
    """
    progress = Progress("parallel_offset")
    landing = LandingZone(_run_dir(config, "parallel_offset"))
    sync = MockClient(config, progress)
    try:
        total = sync.count()
        progress.set_total(total)
        pages = ceil(total / config.page_size) if total else 0
        print(
            f"[parallel_offset] {total:,} records, {pages} pages, "
            f"concurrency={config.concurrency}. Pages do not depend on each other.",
            flush=True,
        )
    finally:
        sync.close()

    async def runner() -> None:
        semaphore = asyncio.Semaphore(config.concurrency)
        bucket = TokenBucket(config.rate_limit_rps) if config.rate_limit_rps > 0 else None
        limits = httpx.Limits(
            max_connections=config.concurrency,
            max_keepalive_connections=config.concurrency,
        )
        async with httpx.AsyncClient(
            base_url=config.base_url.rstrip("/"),
            timeout=httpx.Timeout(config.timeout_s),
            limits=limits,
            headers={"Accept": "application/json", "X-Client-Id": "lab-parallel"},
        ) as http:

            async def one(offset: int) -> None:
                payload = await _async_json(
                    http,
                    "/offset",
                    {"limit": config.page_size, "offset": offset},
                    semaphore,
                    bucket,
                    config.max_retries,
                    progress,
                )
                records = list(payload.get("data") or [])
                landing.write_page(f"offset_{offset:09d}", records)
                progress.set_total(payload.get("total") or total)
                progress.tick(len(records), detail=f"offset={offset}")

            await asyncio.gather(*(one(index * config.page_size) for index in range(pages)))

    started_requests = progress.requests
    asyncio.run(runner())
    # The async path does not share MockClient's counter. Record what progress saw.
    client = MockClient(config, progress)
    client.requests = progress.requests + progress.retries
    client.retries = progress.retries
    # started_requests is 0; keep the name so a reader sees we did not use it.
    del started_requests
    try:
        return _finish(
            "parallel_offset",
            progress,
            landing,
            client,
            notes="independent offsets fetched together; still subject to offset drift",
        )
    finally:
        client.close()


def plan_id_partitions(min_id: int, max_id: int, parts: int) -> list[tuple[int, int]]:
    """Inclusive id ranges. Each range can run its own keyset loop."""
    if max_id < min_id:
        return []
    parts = max(1, parts)
    width = max(1, ceil((max_id - min_id + 1) / parts))
    ranges: list[tuple[int, int]] = []
    start = min_id
    while start <= max_id:
        end = min(max_id, start + width - 1)
        ranges.append((start, end))
        start = end + 1
    return ranges


def plan_date_partitions(min_ts: str, max_ts: str, parts: int) -> list[tuple[str, str]]:
    """Inclusive ISO timestamps that do not overlap and do not leave a gap.

    Each window ends one second before the next window starts. The last window
    includes max_ts. A row timestamped on a boundary second belongs to exactly
    one window.
    """
    start = datetime.strptime(min_ts, "%Y-%m-%dT%H:%M:%SZ")
    end = datetime.strptime(max_ts, "%Y-%m-%dT%H:%M:%SZ")
    if end < start:
        return []
    parts = max(1, parts)
    total_seconds = max(1, int((end - start).total_seconds()) + 1)
    width = max(1, ceil(total_seconds / parts))
    ranges: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(seconds=width - 1))
        ranges.append(
            (
                cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
                window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )
        cursor = window_end + timedelta(seconds=1)
    return ranges


def run_partitioned(config: ClientConfig) -> RunResult:
    """The usual production shape: cut the key space, keyset inside each cut.

    Cursor tokens cannot be cut this way, because you cannot build a token for
    the middle of the table. Keyset can: after_id and end_id are real columns.
    Date cuts work the same way when the API has a time filter. Id cuts are the
    default here because id is unique, so the ranges cannot overlap.
    """
    progress = Progress("partitioned")
    landing = LandingZone(_run_dir(config, "partitioned"))
    sync = MockClient(config, progress)
    try:
        bounds = sync.bounds()
        total = int(bounds["total"])
        progress.set_total(total)
    finally:
        sync.close()

    if config.partition_by == "date":
        windows = plan_date_partitions(bounds["min_updated"], bounds["max_updated"], config.partitions)
        print(f"[partitioned] {len(windows)} date windows, keyset inside each", flush=True)
    else:
        windows = plan_id_partitions(int(bounds["min_id"]), int(bounds["max_id"]), config.partitions)
        print(
            f"[partitioned] {len(windows)} id ranges, keyset inside each, "
            f"concurrency={config.concurrency}",
            flush=True,
        )

    async def runner() -> None:
        semaphore = asyncio.Semaphore(config.concurrency)
        bucket = TokenBucket(config.rate_limit_rps) if config.rate_limit_rps > 0 else None
        async with httpx.AsyncClient(
            base_url=config.base_url.rstrip("/"),
            timeout=httpx.Timeout(config.timeout_s),
            limits=httpx.Limits(
                max_connections=max(1, config.concurrency),
                max_keepalive_connections=max(1, config.concurrency),
            ),
            headers={"Accept": "application/json", "X-Client-Id": "lab-partition"},
        ) as http:

            async def one_range(index: int, window: tuple) -> None:
                after_id = 0
                filters: dict = {}
                end_id = None
                if config.partition_by == "date":
                    filters = {"start_date": window[0], "end_date": window[1]}
                else:
                    after_id = int(window[0]) - 1
                    end_id = int(window[1])
                while True:
                    params = {
                        "limit": config.page_size,
                        "after_id": after_id,
                        "end_id": end_id,
                        **filters,
                    }
                    payload = await _async_json(
                        http,
                        "/keyset",
                        {k: v for k, v in params.items() if v is not None},
                        semaphore,
                        bucket,
                        config.max_retries,
                        progress,
                    )
                    records = list(payload.get("data") or [])
                    key = f"p{index:02d}_after_{after_id:09d}"
                    landing.write_page(key, records)
                    progress.tick(len(records), detail=f"partition={index} after_id={after_id}")
                    if not records or not payload.get("has_more"):
                        break
                    after_id = int(records[-1]["id"])

            await asyncio.gather(
                *(one_range(index, window) for index, window in enumerate(windows))
            )

    asyncio.run(runner())
    client = MockClient(config, progress)
    client.requests = progress.pages + progress.retries
    client.retries = progress.retries
    try:
        return _finish(
            "partitioned",
            progress,
            landing,
            client,
            notes=f"partition_by={config.partition_by}; keyset inside each range",
        )
    finally:
        client.close()


def run_resilient(config: ClientConfig) -> RunResult:
    """Keyset pull with the production safety kit.

    - client-side token bucket, so we do not discover the limit only via 429s
    - retries with Retry-After, exponential backoff, and jitter
    - timeouts and connection pooling (see MockClient)
    - a checkpoint after every page, so a hard kill resumes at the last id

    The page file is named after the cursor (after_id). Re-writing that file
    on resume replaces it. It does not append. That is what keeps a crash
    between "page written" and "checkpoint written" from duplicating rows.
    """
    run_dir = _run_dir(config, "resilient")
    checkpoint_path = run_dir / "checkpoint.json"
    existing = Checkpoint.load(checkpoint_path)
    progress = Progress("resilient")
    landing = LandingZone(run_dir)
    bucket = TokenBucket(config.rate_limit_rps) if config.rate_limit_rps > 0 else None
    client = MockClient(config, progress, bucket=bucket)
    resumed_from = 0
    try:
        after_id = 0
        if existing is not None:
            after_id = existing.last_id
            resumed_from = existing.last_id
            progress.pages = existing.pages
            progress.records = existing.records
            progress.retries = existing.retries
            client.retries = existing.retries
            print(
                f"[resilient] resuming after id={after_id} "
                f"({existing.records:,} records already landed)",
                flush=True,
            )
        else:
            print("[resilient] starting fresh; a checkpoint is written after every page", flush=True)
        while True:
            page = client.fetch_keyset(after_id, config.page_size)
            landing.write_page(f"after_{after_id:09d}", page.records)
            _tick(progress, page, detail=f"after_id={after_id}")
            if page.last_id is not None:
                after_id = page.last_id
            Checkpoint(
                strategy="resilient",
                last_id=after_id,
                pages=progress.pages,
                records=progress.records,
                requests=client.requests,
                retries=client.retries,
                run_dir=str(run_dir),
            ).save(checkpoint_path)
            if config.crash_after_pages is not None and progress.pages >= config.crash_after_pages:
                print(
                    f"[resilient] simulated crash after {progress.pages} pages. "
                    "Rerun the same --run-id to resume.",
                    flush=True,
                )
                raise SystemExit(75)
            if not page.records or not page.has_more:
                break
        return _finish(
            "resilient",
            progress,
            landing,
            client,
            notes="checkpointed keyset with client rate limit and retries",
            resumed_from_id=resumed_from,
        )
    finally:
        client.close()


def run_incremental(config: ClientConfig) -> RunResult:
    """Watermark load. The first run is the slow one. The next run is the delta.

    The watermark is the server clock at the start of the run, not the max
    updated_at we happened to see. Rows updated while we were pulling have a
    later timestamp, so the next run picks them up. Using "max updated_at we
    saw" can skip a row that was committed during the run with a timestamp
    between two pages.
    """
    state_path = Path(config.out_dir) / "state" / "watermark.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    previous = None
    if state_path.exists():
        previous = json.loads(state_path.read_text(encoding="utf-8")).get("watermark")
    progress = Progress("incremental")
    landing = LandingZone(_run_dir(config, "incremental"))
    client = MockClient(config, progress)
    try:
        health = client.health()
        run_started = health["server_time"]
        filters = {"updated_since": previous} if previous else {}
        if previous:
            print(f"[incremental] delta since {previous}", flush=True)
        else:
            print("[incremental] no watermark yet; this run is the full load", flush=True)
        after_id = 0
        while True:
            page = client.fetch_keyset(after_id, config.page_size, **filters)
            landing.write_page(f"after_{after_id:09d}", page.records)
            _tick(progress, page, detail=f"after_id={after_id}")
            if not page.records or not page.has_more or page.last_id is None:
                break
            after_id = page.last_id
        state_path.write_text(
            json.dumps(
                {
                    "watermark": run_started,
                    "previous_watermark": previous,
                    "records": progress.records,
                    "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        note = (
            f"full load, watermark saved as {run_started}"
            if not previous
            else f"delta since {previous}; next watermark {run_started}"
        )
        print(f"[incremental] {note}", flush=True)
        return _finish("incremental", progress, landing, client, notes=note)
    finally:
        client.close()


def run_bulk_export(config: ClientConfig) -> RunResult:
    """Ask the server to write the file. Poll, download, land it as Parquet.

    When an API offers a bulk export, a dump, or a nightly file, take it.
    You replace hundreds of round trips with a handful of calls plus a download.
    """
    progress = Progress("bulk_export")
    landing = LandingZone(_run_dir(config, "bulk_export"))
    client = MockClient(config, progress)
    try:
        print("[bulk_export] starting job", flush=True)
        job_id = client.start_export()
        status = {"status": "pending", "records": 0}
        while status["status"] in {"pending", "running"}:
            time.sleep(0.5)
            status = client.export_status(job_id)
            print(
                f"[bulk_export] job={job_id} status={status['status']} records={status.get('records')}",
                flush=True,
            )
        if status["status"] != "complete":
            raise RuntimeError(f"export failed: {status}")
        gzip_path = landing.run_dir / f"{job_id}.ndjson.gz"
        client.download_export(job_id, gzip_path)
        written = 0
        batch: list[dict] = []
        with gzip.open(gzip_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                batch.append(json.loads(line))
                if len(batch) >= config.page_size:
                    landing.write_page(f"export_{written:09d}", batch)
                    progress.tick(len(batch), requests=0, detail="from export file")
                    written += len(batch)
                    batch = []
        if batch:
            landing.write_page(f"export_{written:09d}", batch)
            progress.tick(len(batch), requests=0, detail="from export file")
        progress.set_total(progress.records)
        return _finish(
            "bulk_export",
            progress,
            landing,
            client,
            notes="server wrote one gzipped NDJSON file; client did not page",
        )
    finally:
        client.close()


STRATEGIES = {
    "naive": run_naive,
    "offset": run_offset,
    "cursor": run_cursor,
    "keyset": run_keyset,
    "parallel_offset": run_parallel_offset,
    "partitioned": run_partitioned,
    "resilient": run_resilient,
    "incremental": run_incremental,
    "bulk_export": run_bulk_export,
}
