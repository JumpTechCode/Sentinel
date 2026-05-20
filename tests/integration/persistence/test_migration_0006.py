# tests/integration/persistence/test_migration_0006.py
"""Migration 0006 produces the expected eval schema and is reversible."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


async def _column_exists(engine, table: str, column: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = :table AND column_name = :column"
                ),
                {"table": table, "column": column},
            )
        ).first()
    return row is not None


async def _table_exists(engine, table: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
                {"t": table},
            )
        ).first()
    return row is not None


async def _constraint_exists(engine, table: str, constraint: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT 1 FROM information_schema.table_constraints "
                    "WHERE table_name = :t AND constraint_name = :c"
                ),
                {"t": table, "c": constraint},
            )
        ).first()
    return row is not None


def test_upgrade_produces_full_eval_schema(pg_dsn: str) -> None:
    async def _check() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            # eval_runs production columns
            for col in (
                "id",
                "started_at",
                "completed_at",
                "status",
                "trigger",
                "git_sha",
                "model",
                "prompt_version",
                "embedding_model_id",
                "corpus_version",
                "corpus_size",
                "shots_per_case",
                "fetcher_fixture_hash",
                "metrics",
                "metrics_stability",
                "regression_baseline_sha",
                "regression_passed",
                "regression_detail",
                "extra",
            ):
                assert await _column_exists(engine, "eval_runs", col), f"eval_runs.{col} missing"
            # placeholder columns gone
            assert not await _column_exists(engine, "eval_runs", "results")
            assert not await _column_exists(engine, "eval_runs", "summary")

            # eval_case_results table + columns (including denormalised incident snapshot)
            assert await _table_exists(engine, "eval_case_results")
            for col in (
                "id",
                "run_id",
                "case_id",
                "shot_index",
                "case_status",
                "metrics",
                "raw_response",
                "diagnosis",
                "incident_id",
                "incident_fingerprint",
                "incident_title",
                "incident_severity",
                "token_usage",
                "latency_ms",
                "error_detail",
            ):
                assert await _column_exists(
                    engine, "eval_case_results", col
                ), f"eval_case_results.{col} missing"

            # CHECK constraints
            assert await _constraint_exists(engine, "eval_runs", "ck_eval_runs_status_valid")
            assert await _constraint_exists(engine, "eval_runs", "ck_eval_runs_trigger_valid")
            assert await _constraint_exists(
                engine, "eval_case_results", "ck_eval_case_results_status_valid"
            )

            # UNIQUE constraint
            assert await _constraint_exists(
                engine, "eval_case_results", "uq_eval_case_results_run_case_shot"
            )
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_check())


def test_downgrade_restores_placeholder_eval_runs(pg_dsn: str) -> None:
    async def _check_post_downgrade() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            # eval_case_results gone
            assert not await _table_exists(engine, "eval_case_results")
            # eval_runs back to placeholder shape: results + summary restored, prod cols gone
            assert await _column_exists(engine, "eval_runs", "results")
            assert await _column_exists(engine, "eval_runs", "summary")
            assert not await _column_exists(engine, "eval_runs", "status")
            assert not await _column_exists(engine, "eval_runs", "metrics")
            assert not await _column_exists(engine, "eval_runs", "fetcher_fixture_hash")
            # placeholder shape is also reachable via the model column
            assert await _column_exists(engine, "eval_runs", "model")
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0005_embedding_1024_event_id")
    asyncio.run(_check_post_downgrade())
    # Restore head so subsequent tests start in a known state.
    command.upgrade(cfg, "head")
