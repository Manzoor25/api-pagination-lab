"""FDIC BankFind Suite adapter. No API key required.

Verified 2026-10-28 against the live API and the OpenAPI spec at
https://api.fdic.gov/banks/docs/swagger.yaml (servers url: /banks).

Facts, not guesses
------------------
- Base URL: https://api.fdic.gov/banks
- Institutions: GET /institutions
- Pagination: limit + offset (this is offset pagination, not a cursor)
- Default limit: 10. Maximum limit: 10000. limit=10001 returns HTTP 400.
- Total count: meta.total
- Records: data[].data  (each hit is wrapped)
- Primary key: CERT
- Fields: comma-separated UPPERCASE names in the `fields` parameter
- Filters: Elasticsearch query-string syntax in `filters`
- API key: documented, but not currently required
- Rate limit header observed on every call: X-Ratelimit-Limit
- Output: JSON by default. CSV if you send Accept: text/csv
- Terms: public FDIC data. Do not imply FDIC endorses a downstream app.
  Feedback contact published on the docs page: PublicDataFeedback@fdic.gov

Why offset is the right strategy here
--------------------------------------
The API does not offer a cursor or a keyset. It does offer a 10,000-row page,
and the institutions dataset is a few tens of thousands of rows, so a full
pull is 3 requests if you use the maximum page size. Using the default of 10
turns the same pull into thousands of requests and, if the rate limit window
is tight, into the hour-long job this lab exists to explain.

This module reuses the lab's landing zone, retry policy, and progress printer.
"""

from __future__ import annotations

import json
from datetime import datetime

import httpx

from fetcher.landing import LandingZone
from fetcher.models import ClientConfig, Page, RunResult
from fetcher.progress import Progress
from fetcher.retry import AttemptResult, TokenBucket, call_with_retry
from fetcher.strategies import _finish, _run_dir

# Documented maximum. Confirmed by a live call: 10000 works, 10001 is HTTP 400.
FDIC_MAX_PAGE_SIZE = 10_000
FDIC_BASE = "https://api.fdic.gov/banks"
DOCS_URL = "https://api.fdic.gov/banks/docs/"
SPEC_URL = "https://api.fdic.gov/banks/docs/swagger.yaml"


def normalize_date(value: object) -> str:
    """DATEUPDT comes back as MM/DD/YYYY. Store ISO-8601 so sorts and watermarks work."""
    text = str(value or "")
    if not text:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return text


def to_record(hit: dict) -> dict:
    """Unwrap data[].data and keep every returned field inside payload."""
    data = hit.get("data") if isinstance(hit, dict) and "data" in hit else hit
    if not isinstance(data, dict):
        data = {"value": data}
    cert = data.get("CERT")
    asset = data.get("ASSET")
    try:
        amount = float(asset) if asset not in (None, "") else 0.0
    except (TypeError, ValueError):
        amount = 0.0
    return {
        "id": int(cert) if cert not in (None, "") else -1,
        "updated_at": normalize_date(data.get("DATEUPDT")),
        "category": str(data.get("STALP") or data.get("STNAME") or ""),
        "amount": amount,
        "payload": json.dumps(data, separators=(",", ":"), default=str),
    }


class FdicClient:
    """Small polite client. Retries 429 and 5xx. Does not retry 400."""

    def __init__(self, config: ClientConfig, progress: Progress | None = None) -> None:
        self.config = config
        self.progress = progress
        self.requests = 0
        self.retries = 0
        rate = config.rate_limit_rps if config.rate_limit_rps > 0 else 2.0
        # 2 requests/second is deliberately below the header we observed
        # (X-Ratelimit-Limit: 20) until diagnose.py has measured the window.
        # Three pages at this pace still finish in a couple of seconds.
        self.bucket = TokenBucket(rate)
        self.http = httpx.Client(
            base_url=config.base_url.rstrip("/"),
            timeout=httpx.Timeout(config.timeout_s),
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "User-Agent": "api-pagination-lab/1.0 (teaching pull; contact via GitHub repo)",
            },
            follow_redirects=True,
        )

    def close(self) -> None:
        self.http.close()

    def _get(self, path: str, params: dict) -> httpx.Response:
        def do_request() -> AttemptResult:
            self.requests += 1
            try:
                response = self.http.get(path, params=params)
            except httpx.TimeoutException as exc:
                return AttemptResult(0, {}, exc, retryable_error=f"timeout: {exc}")
            except httpx.TransportError as exc:
                return AttemptResult(0, {}, exc, retryable_error=f"transport: {exc}")
            return AttemptResult(response.status_code, dict(response.headers), response)

        def on_retry(attempt: int, delay: float, reason: str) -> None:
            self.retries += 1
            if self.progress is not None:
                self.progress.note_retry()
            print(f"[fdic retry] attempt={attempt} waiting={delay:.2f}s reason={reason}", flush=True)

        result, _attempt = call_with_retry(
            do_request,
            max_retries=self.config.max_retries,
            on_retry=on_retry,
            bucket=self.bucket,
        )
        response = result.body
        if not isinstance(response, httpx.Response):
            raise RuntimeError(result.retryable_error or "FDIC request failed")
        if response.status_code >= 400:
            raise RuntimeError(f"FDIC HTTP {response.status_code}: {response.text[:400]}")
        return response

    def fetch_offset(self, offset: int, limit: int) -> Page:
        params: dict[str, object] = {"limit": limit, "offset": offset}
        if self.config.fields:
            params["fields"] = self.config.fields
        # sort_by=CERT makes offset pages stable if the index is rebuilt.
        params["sort_by"] = "CERT"
        params["sort_order"] = "ASC"
        response = self._get("/institutions", params)
        payload = response.json()
        hits = list(payload.get("data") or [])
        records = [to_record(hit) for hit in hits]
        total = int(payload.get("meta", {}).get("total") or 0)
        has_more = offset + len(records) < total and len(records) > 0
        last_id = records[-1]["id"] if records else None
        return Page(
            records=records,
            total=total,
            has_more=has_more,
            next_offset=offset + len(records) if has_more else None,
            last_id=last_id,
            status_code=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            raw_bytes=len(response.content),
        )


def run_fdic(strategy: str, config: ClientConfig) -> RunResult:
    """Full institutions pull. Only offset-style strategies apply."""
    if strategy not in {"offset", "parallel_offset", "naive"}:
        raise SystemExit(f"FDIC uses offset pagination; {strategy} is not applicable")
    progress = Progress(f"fdic-{strategy}")
    landing = LandingZone(_run_dir(config, f"fdic-{strategy}"))
    client = FdicClient(config, progress)
    try:
        if strategy == "naive":
            print(
                "[fdic] naive: the default limit is 10. One call does not return the dataset.",
                flush=True,
            )
            page = client.fetch_offset(0, min(config.page_size, 10) if config.page_size else 10)
            landing.write_page("offset_000000000", page.records)
            progress.set_total(page.total)
            progress.tick(len(page.records), detail="default-or-small page only")
            note = (
                f"one page returned {len(page.records):,} of {page.total:,}. "
                "FDIC max page size is 10000; use --strategy offset --page-size 10000."
            )
            print(f"[fdic] {note}", flush=True)
            return _finish(f"fdic-{strategy}", progress, landing, client, notes=note)

        offset = 0
        page_size = min(config.page_size, FDIC_MAX_PAGE_SIZE)
        print(
            f"[fdic] offset pull page_size={page_size} base={config.base_url} docs={DOCS_URL}",
            flush=True,
        )
        while True:
            page = client.fetch_offset(offset, page_size)
            landing.write_page(f"offset_{offset:09d}", page.records)
            progress.set_total(page.total)
            progress.tick(len(page.records), detail=f"offset={offset}")
            if not page.records or not page.has_more:
                break
            offset += len(page.records)
        note = (
            "FDIC institutions, offset/limit, page size clamped to the documented "
            f"maximum of {FDIC_MAX_PAGE_SIZE}. Spec: {SPEC_URL}"
        )
        return _finish(f"fdic-{strategy}", progress, landing, client, notes=note)
    finally:
        client.close()
