"""Eval read routes end-to-end against the real app + a seeded run."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

pytestmark = pytest.mark.integration


async def test_evals_read_surface_end_to_end(client: AsyncClient, app: FastAPI) -> None:
    from sentinel.persistence.repositories import PostgresEvalRunRepository

    repo: PostgresEvalRunRepository = app.state.eval_run_repo
    run_id = await repo.start_run(
        status="running",
        trigger="baseline",
        git_sha="testsha",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=1,
        fetcher_fixture_hash="fh",
        extra={},
    )
    await repo.finalize_run(
        run_id,
        status="ok",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha="testsha",
        regression_passed=True,
        regression_detail={"ok": True},
    )

    listed = await client.get("/evals/runs")
    assert listed.status_code == 200, listed.text
    assert any(r["id"] == str(run_id) for r in listed.json())

    detail = await client.get(f"/evals/runs/{run_id}")
    assert detail.status_code == 200
    assert detail.json()["status"] == "ok"
    assert detail.json()["metrics"]["category_match"] == 0.9

    baseline = await client.get("/evals/baseline")
    assert baseline.status_code == 200
    assert baseline.json()["id"] == str(run_id)


async def test_get_unknown_eval_run_404(client: AsyncClient) -> None:
    resp = await client.get("/evals/runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "eval_run_not_found"


async def test_baseline_404_when_no_runs(client: AsyncClient) -> None:
    # Fresh state (the autouse _reset_state truncates eval tables before each
    # test) → no baseline recorded yet. Real fresh-deploy scenario.
    resp = await client.get("/evals/baseline")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "no_baseline"
