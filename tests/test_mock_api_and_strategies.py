"""API behavior and the pagination loops, against an in-process app."""

from __future__ import annotations

import gzip
import json
import socket
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from fetcher.models import ClientConfig
from fetcher.strategies import run_cursor, run_keyset, run_naive, run_offset, run_resilient
from mock_api.server import Settings, create_app


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _serve(app) -> tuple[str, uvicorn.Server]:
    """Run the app on a real port. httpx 0.28's ASGI transport is async-only."""
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            httpx.get(f"{url}/health", timeout=0.2)
            return url, server
        except httpx.HTTPError:
            time.sleep(0.05)
    raise RuntimeError("test server did not start")


def test_huge_page_is_rejected_and_explains_why(client) -> None:
    response = client.get("/offset", params={"limit": 10_000_000})
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "page_size_exceeded"
    assert "memory" in " ".join(detail["reasons"]).lower()
    assert detail["max_page_size"] == 10


def test_omitted_limit_truncates_at_the_cap(client) -> None:
    response = client.get("/offset")
    assert response.status_code == 200
    body = response.json()
    assert body["returned"] == 10
    assert body["truncated"] is True
    assert body["total"] == 40
    assert body["has_more"] is True


def test_offset_loop_stops_and_returns_every_id(client) -> None:
    seen: list[int] = []
    offset = 0
    pages = 0
    while True:
        body = client.get("/offset", params={"limit": 10, "offset": offset}).json()
        pages += 1
        seen.extend(row["id"] for row in body["data"])
        if not body["has_more"]:
            break
        offset = body["next_offset"]
        assert pages < 20
    assert seen == list(range(1, 41))
    assert pages == 4


def test_page_numbers_are_one_based(client) -> None:
    assert client.get("/pages", params={"page": 0, "page_size": 10}).status_code == 422
    first = client.get("/pages", params={"page": 1, "page_size": 10}).json()
    second = client.get("/pages", params={"page": 2, "page_size": 10}).json()
    assert first["data"][0]["id"] == 1
    assert second["data"][0]["id"] == 11


def test_cursor_is_opaque_and_consistent(client) -> None:
    first = client.get("/cursor", params={"limit": 10}).json()
    assert first["next_cursor"]
    assert "last_id" not in first["next_cursor"]
    forged = client.get("/cursor", params={"limit": 10, "cursor": "bm90LWEtdG9rZW4.deadbeef"})
    assert forged.status_code == 400
    second = client.get("/cursor", params={"limit": 10, "cursor": first["next_cursor"]}).json()
    assert second["data"][0]["id"] == 11
    ids = [row["id"] for row in first["data"] + second["data"]]
    assert ids == list(range(1, 21))


def test_keyset_stays_on_the_index(client) -> None:
    page = client.get("/keyset", params={"limit": 10, "after_id": 25, "end_id": 28}).json()
    assert [row["id"] for row in page["data"]] == [26, 27, 28]
    assert page["has_more"] is False


def test_link_header_has_next(client) -> None:
    response = client.get("/linkheader", params={"limit": 10})
    assert response.status_code == 200
    assert 'rel="next"' in response.headers["link"]
    assert response.json()["returned"] == 10


def test_field_selection_drops_payload_but_keeps_id(client) -> None:
    body = client.get("/offset", params={"limit": 2, "fields": "amount"}).json()
    assert set(body["data"][0]) == {"id", "amount"}


def test_rate_limit_returns_retry_after() -> None:
    from mock_api.seed import seed_database

    # A fresh app so the bucket is not shared with other tests.
    path = Path("data") / "pytest-rate.db"
    path.parent.mkdir(exist_ok=True)
    seed_database(path, count=5, seed=42, force=True)
    app = create_app(
        Settings(
            db_path=str(path),
            records=5,
            max_page_size=10,
            latency_ms=0,
            failure_rate=0,
            timeout_rate=0,
            rate_limit_rps=1,
        )
    )
    from fastapi.testclient import TestClient

    local = TestClient(app)
    assert local.get("/count").status_code == 200
    limited = local.get("/count")
    assert limited.status_code == 429
    assert limited.headers["retry-after"]
    assert limited.json()["detail"]["error"] == "rate_limited"


def test_filters_and_count(client) -> None:
    counted = client.get("/count", params={"start_date": "2024-01-01", "end_date": "2024-01-02"})
    assert counted.status_code == 200
    assert "total" in counted.json()
    bad = client.get("/offset", params={"limit": 5, "updated_since": "yesterday"})
    assert bad.status_code == 400


def test_export_round_trip(client) -> None:
    started = client.post("/export")
    assert started.status_code == 200
    job_id = started.json()["job_id"]
    status = {"status": "pending"}
    for _ in range(50):
        status = client.get(f"/export/{job_id}").json()
        if status["status"] in {"complete", "failed"}:
            break
    assert status["status"] == "complete"
    assert status["records"] == 40
    downloaded = client.get(f"/export/{job_id}/download")
    assert downloaded.status_code == 200
    text = gzip.decompress(downloaded.content).decode()
    assert len([line for line in text.splitlines() if line]) == 40


def test_strategy_offset_cursor_keyset_land_every_row(tmp_path: Path, app) -> None:
    url, server = _serve(app)
    base = ClientConfig(
        base_url=url,
        page_size=10,
        out_dir=tmp_path / "landing",
        max_retries=1,
    )
    for runner, name in ((run_offset, "offset"), (run_cursor, "cursor"), (run_keyset, "keyset")):
        result = runner(base)
        page_dir = Path(result.run_dir) / "pages"
        ids = []
        for path in sorted(page_dir.glob("*.ndjson")):
            for line in path.read_text(encoding="utf-8").splitlines():
                ids.append(json.loads(line)["id"])
        assert ids == list(range(1, 41)), name
        assert result.records == 40
        assert (Path(result.run_dir) / "data.parquet").exists()
        assert (Path(result.run_dir) / "manifest.json").exists()
    server.should_exit = True


def test_naive_explains_truncation(tmp_path: Path, app) -> None:
    url, server = _serve(app)
    config = ClientConfig(
        base_url=url,
        page_size=10,
        out_dir=tmp_path / "landing",
        max_retries=0,
    )
    result = run_naive(config)
    server.should_exit = True
    assert result.records == 10
    assert "loses" in result.notes or "99" in result.notes


def test_resilient_resume_does_not_duplicate(tmp_path: Path, app) -> None:
    url, server = _serve(app)
    config = ClientConfig(
        base_url=url,
        page_size=10,
        out_dir=tmp_path / "landing",
        run_id="resume-me",
        crash_after_pages=2,
        max_retries=1,
    )
    with pytest.raises(SystemExit):
        run_resilient(config)
    config.crash_after_pages = None
    result = run_resilient(config)
    assert result.resumed_from_id == 20
    assert result.records == 40
    ids = []
    for path in sorted((Path(result.run_dir) / "pages").glob("*.ndjson")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                ids.append(json.loads(line)["id"])
    assert sorted(ids) == list(range(1, 41))
    assert len(ids) == 40
    server.should_exit = True


def test_live_offset_drifts_and_keyset_does_not(tmp_path: Path) -> None:
    from mock_api.seed import seed_database

    db = tmp_path / "live.db"
    seed_database(db, count=80, seed=42)
    app = create_app(
        Settings(
            db_path=str(db),
            records=80,
            max_page_size=10,
            latency_ms=0,
            failure_rate=0,
            timeout_rate=0,
            rate_limit_rps=0,
            live_data=True,
            live_every=1,
        )
    )
    url, server = _serve(app)
    config = ClientConfig(
        base_url=url,
        page_size=10,
        out_dir=tmp_path / "landing",
        max_retries=1,
    )
    offset = run_offset(config)
    keyset = run_keyset(config)
    offset_ids = _ids(Path(offset.run_dir))
    keyset_ids = _ids(Path(keyset.run_dir))
    assert len(offset_ids) != len(set(offset_ids)) or len(set(offset_ids)) < 80
    assert len(keyset_ids) == len(set(keyset_ids))
    server.should_exit = True


def _ids(run_dir: Path) -> list[int]:
    found = []
    for path in sorted((run_dir / "pages").glob("*.ndjson")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                found.append(json.loads(line)["id"])
    return found
