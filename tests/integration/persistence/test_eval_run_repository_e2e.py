# tests/integration/persistence/test_eval_run_repository_e2e.py
"""End-to-end repository tests against real Postgres (testcontainers).

Covers:
- Happy-path lifecycle (start → persist shots → finalize → reads)
- finalize_run raises EvalRunNotFoundOrAlreadyFinalized on double-finalize
- finalize_run raises on unknown run_id
- persist_shot raises IntegrityError on duplicate (run, case, shot)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sentinel.persistence.errors import EvalRunNotFoundOrAlreadyFinalized
from sentinel.persistence.repositories import (
    EvalCaseResultRecord,
    PostgresEvalRunRepository,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def test_full_run_lifecycle(pg_dsn: str) -> None:
    """start_run -> persist_shot x3 -> finalize_run -> get_run round-trip."""

    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="abc1234",
                model="claude-sonnet-4-5-20250929",
                prompt_version="v1",
                embedding_model_id="BAAI/bge-large-en-v1.5",
                corpus_version="1",
                corpus_size=10,
                shots_per_case=3,
                fetcher_fixture_hash="deadbeef",
            )
            assert run_id is not None

            for i in range(3):
                await repo.persist_shot(
                    EvalCaseResultRecord(
                        run_id=run_id,
                        case_id="cloudflare-bgp",
                        shot_index=i,
                        case_status="ok",
                        metrics={"category_match": 1.0, "evidence_quality": 0.9},
                        raw_response={"id": f"msg_{i}", "content": []},
                        diagnosis={"hypothesis": "BGP misconfig", "confidence": 0.85},
                        incident_id=uuid4(),
                        incident_fingerprint="fp-bgp",
                        incident_title="Elevated 5xx errors across multiple POPs",
                        incident_severity="SEV1",
                        token_usage={"input": 1200, "output": 800},
                        latency_ms=2400 + i,
                        error_detail=None,
                    )
                )

            await repo.finalize_run(
                run_id,
                status="ok",
                metrics={"category_match": 1.0, "evidence_quality": 0.9},
                metrics_stability={"category_match": 0.0, "evidence_quality": 0.05},
                regression_baseline_sha=None,
                regression_passed=None,
                regression_detail=None,
            )

            row = await repo.get_run(run_id)
            assert row is not None
            assert row.status == "ok"
            assert row.metrics["category_match"] == 1.0
            assert row.metrics_stability["evidence_quality"] == 0.05

            latest = await repo.get_latest_ok_run()
            assert latest is not None
            assert latest.id == run_id

            latest_baseline = await repo.get_latest_ok_run(trigger="baseline")
            assert latest_baseline is None  # this run had trigger='local'

            recents = await repo.list_recent(limit=10)
            assert any(r.id == run_id for r in recents)
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_finalize_run_raises_on_double_finalize(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="x",
                model="m",
                prompt_version="v1",
                embedding_model_id="e",
                corpus_version="1",
                corpus_size=1,
                shots_per_case=1,
                fetcher_fixture_hash="h",
            )

            # First finalize succeeds
            await repo.finalize_run(
                run_id,
                status="ok",
                metrics={"x": 1.0},
                metrics_stability={"x": 0.0},
                regression_baseline_sha=None,
                regression_passed=None,
                regression_detail=None,
            )

            # Second finalize raises (status is already terminal)
            with pytest.raises(EvalRunNotFoundOrAlreadyFinalized) as exc:
                await repo.finalize_run(
                    run_id,
                    status="failed",
                    metrics={"x": 0.5},
                    metrics_stability={"x": 0.0},
                    regression_baseline_sha=None,
                    regression_passed=None,
                    regression_detail=None,
                )
            assert exc.value.run_id == run_id

            # First-finalize state preserved
            row = await repo.get_run(run_id)
            assert row is not None and row.status == "ok"
            assert row.metrics["x"] == 1.0
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_finalize_run_raises_on_unknown_run_id(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            bogus = uuid4()
            with pytest.raises(EvalRunNotFoundOrAlreadyFinalized) as exc:
                await repo.finalize_run(
                    bogus,
                    status="ok",
                    metrics={},
                    metrics_stability={},
                    regression_baseline_sha=None,
                    regression_passed=None,
                    regression_detail=None,
                )
            assert exc.value.run_id == bogus
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_persist_shot_duplicate_raises_integrity_error(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="x",
                model="m",
                prompt_version="v1",
                embedding_model_id="e",
                corpus_version="1",
                corpus_size=1,
                shots_per_case=1,
                fetcher_fixture_hash="h",
            )

            shot = EvalCaseResultRecord(
                run_id=run_id,
                case_id="case-a",
                shot_index=0,
                case_status="ok",
                metrics={},
                raw_response=None,
                diagnosis=None,
                incident_id=None,
                incident_fingerprint=None,
                incident_title=None,
                incident_severity=None,
                token_usage=None,
                latency_ms=None,
                error_detail=None,
            )
            await repo.persist_shot(shot)
            with pytest.raises(IntegrityError):
                await repo.persist_shot(shot)
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())
