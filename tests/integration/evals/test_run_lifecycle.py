"""Integration test for PostgresEvalRunRepository lifecycle.

Tests `start_run -> persist_shot x N -> finalize_run -> get_run` against a real
Postgres. Does not boot the FastAPI app or run cassettes — that's the e2e
test (PR 7, #50). This is a narrow lifecycle test for PR 5.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from sentinel.persistence.repositories import EvalCaseResultRecord, PostgresEvalRunRepository

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_lifecycle_success(session_factory) -> None:  # type: ignore[no-untyped-def]
    """start_run → 2 persist_shot → finalize_run(ok) → get_run returns ok."""
    repo = PostgresEvalRunRepository(session_factory)

    run_id: UUID = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="0" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra={"cassette_mode": "replay", "allow_dirty": False, "subcommand": "run"},
    )
    assert isinstance(run_id, UUID)

    for shot_index in range(2):
        await repo.persist_shot(
            EvalCaseResultRecord(
                run_id=run_id,
                case_id="cf-2019-cpu",
                shot_index=shot_index,
                case_status="ok",
                metrics={
                    "category_match": 1.0,
                    "hypothesis_cosine": 0.85,
                    "action_coverage": 0.75,
                    "evidence_quality": 0.9,
                },
                raw_response=None,
                diagnosis=None,
                incident_id=uuid4(),
                incident_fingerprint="f" * 64,
                incident_title="CPU spike",
                incident_severity="critical",
                token_usage={"input": 1000, "output": 200},
                latency_ms=1200,
                error_detail=None,
            )
        )

    await repo.finalize_run(
        run_id,
        status="ok",
        metrics={
            "category_match": 1.0,
            "hypothesis_cosine": 0.85,
            "action_coverage": 0.75,
            "evidence_quality": 0.9,
        },
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
    )

    record = await repo.get_run(run_id)
    assert record is not None
    assert record.status == "ok"
    assert record.completed_at is not None


@pytest.mark.asyncio
async def test_lifecycle_failure(session_factory) -> None:  # type: ignore[no-untyped-def]
    """start_run → finalize_run(failed) → get_run returns failed with detail."""
    repo = PostgresEvalRunRepository(session_factory)

    run_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="0" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )

    await repo.finalize_run(
        run_id,
        status="failed",
        metrics={},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail={"error": "CassetteMiss(...)"},
    )

    record = await repo.get_run(run_id)
    assert record is not None
    assert record.status == "failed"
    assert record.regression_detail == {"error": "CassetteMiss(...)"}


@pytest.mark.asyncio
async def test_get_latest_ok_run_filters_failed(session_factory) -> None:  # type: ignore[no-untyped-def]
    """A failed run is not returned by get_latest_ok_run."""
    repo = PostgresEvalRunRepository(session_factory)

    # Create a failed run.
    failed_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="1" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )
    await repo.finalize_run(
        failed_id,
        status="failed",
        metrics={},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail={"error": "boom"},
    )

    # Create an ok run.
    ok_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="2" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )
    await repo.finalize_run(
        ok_id,
        status="ok",
        metrics={"category_match": 1.0, "hypothesis_cosine": 0.9, "action_coverage": 0.8},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
    )

    latest = await repo.get_latest_ok_run()
    assert latest is not None
    assert latest.id == ok_id
