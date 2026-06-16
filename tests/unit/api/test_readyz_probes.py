"""readyz dependency probes — Postgres + Redis + consumer aliveness."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.health import router as health_router


class _FakeConn:
    async def execute(self, *_a: object, **_k: object) -> None:
        return None


class _FakeEngine:
    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_FakeConn]:
        if self._fail:
            raise RuntimeError("pg down")
        yield _FakeConn()


def _app(*, engine: object, redis: object, consumers: object) -> FastAPI:
    app = FastAPI()
    app.state.engine = engine
    app.state.redis = redis
    app.state.consumer_alive = consumers
    app.include_router(health_router)
    return app


def test_readyz_ok_when_all_healthy() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(), redis=redis, consumers={"diagnosis": True})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"] == "ok"
    assert body["checks"]["consumer:diagnosis"] == "ok"


def test_readyz_503_when_postgres_down() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(fail=True), redis=redis, consumers={})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["checks"]["postgres"] == "dead"


def test_readyz_503_when_consumer_dead() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(), redis=redis, consumers={"diagnosis": False})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["checks"]["consumer:diagnosis"] == "dead"


def test_readyz_unknown_when_no_consumer_signal_but_deps_healthy() -> None:
    # Healthy deps but no consumer-aliveness signal (e.g. consumers disabled or
    # lifespan not yet complete): stay 503 with "unknown" rather than route.
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(), redis=redis, consumers={})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unknown"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"] == "ok"
