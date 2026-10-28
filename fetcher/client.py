"""HTTP adapter for the mock API.

Strategies talk to this object, not to URLs. That is what lets the same
offset loop run against the lab server and, with a different adapter, against
FDIC BankFind. See real_api/fdic.py.
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx

from fetcher.models import ClientConfig, Page
from fetcher.progress import Progress
from fetcher.retry import AttemptResult, TokenBucket, call_with_retry


class MockClient:
    """Synchronous client. Parallel strategies use httpx.AsyncClient directly."""

    def __init__(
        self,
        config: ClientConfig,
        progress: Progress | None = None,
        bucket: TokenBucket | None = None,
    ) -> None:
        self.config = config
        self.progress = progress
        self.bucket = bucket
        self.requests = 0
        self.retries = 0
        # keep-alive is the default. Reusing sockets matters once you are
        # paying 300 ms of server time and do not also want a fresh TCP
        # handshake on every page.
        client_kwargs: dict = {
            "base_url": config.base_url.rstrip("/"),
            "timeout": httpx.Timeout(config.timeout_s),
            "limits": httpx.Limits(max_keepalive_connections=20, max_connections=20),
            "headers": {
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Client-Id": "lab",
            },
        }
        if config.transport is not None:
            client_kwargs["transport"] = config.transport
        self.http = httpx.Client(**client_kwargs)

    def close(self) -> None:
        self.http.close()

    def _params(self, extra: dict) -> dict:
        params = {key: value for key, value in extra.items() if value is not None}
        if self.config.fields:
            params["fields"] = self.config.fields
        return params

    def _send(self, path: str, params: dict) -> httpx.Response:
        def do_request() -> AttemptResult:
            self.requests += 1
            started = time.perf_counter()
            try:
                response = self.http.get(path, params=params)
            except httpx.TimeoutException as exc:
                return AttemptResult(0, {}, exc, retryable_error=f"timeout: {exc}")
            except httpx.TransportError as exc:
                return AttemptResult(0, {}, exc, retryable_error=f"transport: {exc}")
            response.elapsed_ms = (time.perf_counter() - started) * 1000  # type: ignore[attr-defined]
            return AttemptResult(response.status_code, dict(response.headers), response)

        def on_retry(attempt: int, delay: float, reason: str) -> None:
            self.retries += 1
            if self.progress is not None:
                self.progress.note_retry()
            print(
                f"[retry] attempt={attempt} waiting={delay:.2f}s reason={reason}",
                flush=True,
            )

        result, _attempt = call_with_retry(
            do_request,
            max_retries=self.config.max_retries,
            on_retry=on_retry,
            bucket=self.bucket,
        )
        if result.retryable_error and not isinstance(result.body, httpx.Response):
            raise RuntimeError(result.retryable_error)
        response = result.body
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code} from {path}: {response.text[:500]}")
        return response

    def _page_from(self, response: httpx.Response) -> Page:
        payload = response.json()
        records = list(payload.get("data") or [])
        last_id = int(records[-1]["id"]) if records else None
        return Page(
            records=records,
            total=payload.get("total"),
            has_more=bool(payload.get("has_more")),
            next_cursor=payload.get("next_cursor"),
            next_offset=payload.get("next_offset"),
            last_id=last_id,
            request_ms=getattr(response, "elapsed_ms", 0.0),
            status_code=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            raw_bytes=len(response.content),
        )

    def count(self, **filters: str | None) -> int:
        response = self._send("/count", self._params(filters))
        return int(response.json()["total"])

    def bounds(self) -> dict:
        return self._send("/bounds", {}).json()

    def health(self) -> dict:
        return self._send("/health", {}).json()

    def fetch_offset(self, offset: int, limit: int, **filters: str | None) -> Page:
        params = self._params({"limit": limit, "offset": offset, **filters})
        return self._page_from(self._send("/offset", params))

    def fetch_pages(self, page: int, limit: int, **filters: str | None) -> Page:
        params = self._params({"page": page, "page_size": limit, **filters})
        return self._page_from(self._send("/pages", params))

    def fetch_cursor(self, cursor: str | None, limit: int, **filters: str | None) -> Page:
        params = self._params({"cursor": cursor, "limit": limit, **filters})
        return self._page_from(self._send("/cursor", params))

    def fetch_keyset(
        self,
        after_id: int,
        limit: int,
        end_id: int | None = None,
        **filters: str | None,
    ) -> Page:
        params = self._params(
            {"after_id": after_id, "limit": limit, "end_id": end_id, **filters}
        )
        return self._page_from(self._send("/keyset", params))

    def fetch_link(self, url: str) -> tuple[Page, str | None]:
        """Follow a Link-header URL. Returns the page and the next URL, if any."""
        response = self._send(url, {})
        page = self._page_from(response)
        return page, _next_link(response.headers.get("link") or response.headers.get("Link"))

    def get_raw(self, path: str, params: dict) -> httpx.Response:
        """Used by the naive strategy, which needs to see the 400 body."""
        return self.http.get(path, params=params)

    def start_export(self, **filters: str | None) -> str:
        response = self.http.post("/export", params=self._params(filters))
        self.requests += 1
        response.raise_for_status()
        return str(response.json()["job_id"])

    def export_status(self, job_id: str) -> dict:
        response = self._send(f"/export/{job_id}", {})
        return response.json()

    def download_export(self, job_id: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self.http.stream("GET", f"/export/{job_id}/download") as response:
            self.requests += 1
            response.raise_for_status()
            with dest.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        return dest


def _next_link(header: str | None) -> str | None:
    """Pick rel=next out of an RFC 5988 Link header."""
    if not header:
        return None
    for part in header.split(","):
        if 'rel="next"' not in part and "rel=next" not in part:
            continue
        start = part.find("<")
        end = part.find(">")
        if start >= 0 and end > start:
            return part[start + 1 : end]
    return None
