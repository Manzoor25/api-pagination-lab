"""Shared fixtures. Tests use a 40-row database and zero artificial latency."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from mock_api.seed import seed_database
from mock_api.server import Settings, create_app


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "lab.db"
    seed_database(path, count=40, seed=42)
    return path


@pytest.fixture()
def settings(db_path: Path) -> Settings:
    return Settings(
        db_path=str(db_path),
        records=40,
        seed=42,
        max_page_size=10,
        latency_ms=0,
        deep_offset_ms_per_1k=0,
        rate_limit_rps=0,
        failure_rate=0,
        timeout_rate=0,
        live_data=False,
        admin=True,
    )


@pytest.fixture()
def app(settings: Settings):
    return create_app(settings)


@pytest.fixture()
def client(app) -> TestClient:
    return TestClient(app)


@pytest.fixture()
def transport(app) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=app)
