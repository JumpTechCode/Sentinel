from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.webhooks import router as webhooks_router
from sentinel.ingestion.webhook import HandlerOutcome, HandlerResult


@pytest.fixture
def app_with_mock_handler() -> Iterator[tuple[FastAPI, MagicMock]]:
    app = FastAPI()
    app.include_router(webhooks_router)
    handler = MagicMock()
    handler.handle = AsyncMock(
        return_value=HandlerResult(outcome=HandlerOutcome.ACCEPTED, incident_id=uuid4())
    )
    app.state.webhook_handler = handler
    yield app, handler


def test_accepted_returns_202(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, _handler = app_with_mock_handler
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "accepted"
    assert body["incident_id"]


def test_recurred_returns_202(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(
        return_value=HandlerResult(outcome=HandlerOutcome.RECURRED, incident_id=uuid4())
    )
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 202
    assert r.json()["status"] == "recurred"


def test_duplicate_returns_200(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(return_value=HandlerResult(outcome=HandlerOutcome.DUPLICATE))
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 200
    assert r.json()["status"] == "duplicate"


def test_unauthorized_returns_401(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(
        return_value=HandlerResult(outcome=HandlerOutcome.UNAUTHORIZED, reason="bad_signature")
    )
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 401


def test_bad_request_returns_422(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(
        return_value=HandlerResult(outcome=HandlerOutcome.BAD_REQUEST, reason="bad_json")
    )
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 422


def test_unknown_source_returns_404(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(return_value=HandlerResult(outcome=HandlerOutcome.UNKNOWN_SOURCE))
    client = TestClient(app)
    r = client.post("/webhooks/nope", content=b"{}")
    assert r.status_code == 404


def test_infra_unavailable_returns_503(app_with_mock_handler: tuple[FastAPI, MagicMock]) -> None:
    app, handler = app_with_mock_handler
    handler.handle = AsyncMock(return_value=HandlerResult(outcome=HandlerOutcome.INFRA_UNAVAILABLE))
    client = TestClient(app)
    r = client.post("/webhooks/sentry", content=b'{"a":1}')
    assert r.status_code == 503
