"""Eval read routes against a fake EvalRunRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.evals import router as evals_router
from sentinel.persistence.repositories import EvalRunRecord


def _record(**overrides: Any) -> EvalRunRecord:
    kwargs: dict[str, Any] = dict(
        id=uuid4(),
        status="ok",
        trigger="baseline",
        git_sha="abc123",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=1,
        fetcher_fixture_hash="fh",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha="def456",
        regression_passed=True,
        regression_detail={"category_match": {"delta": 0.0}},
        extra={},
        started_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 6, 15, 12, 2, tzinfo=UTC),
    )
    kwargs.update(overrides)
    return EvalRunRecord(**kwargs)


def _app(repo: object) -> FastAPI:
    app = FastAPI()
    app.state.eval_run_repo = repo
    app.include_router(evals_router)
    return app


def test_list_runs_returns_summaries() -> None:
    rec = _record()
    repo = type("R", (), {})()
    repo.list_recent = AsyncMock(return_value=[rec])
    client = TestClient(_app(repo))
    resp = client.get("/evals/runs?limit=10")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data) == 1
    assert data[0]["id"] == str(rec.id)
    assert data[0]["summary"]["category_match"] == 0.9
    assert data[0]["started_at"] is not None
    repo.list_recent.assert_awaited_once_with(limit=10)


def test_list_runs_rejects_bad_limit() -> None:
    repo = type("R", (), {})()
    repo.list_recent = AsyncMock(return_value=[])
    client = TestClient(_app(repo))
    assert client.get("/evals/runs?limit=0").status_code == 422  # below ge=1
    assert client.get("/evals/runs?limit=201").status_code == 422  # above le=200
