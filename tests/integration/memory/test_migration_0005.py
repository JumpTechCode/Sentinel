# tests/integration/memory/test_migration_0005.py
"""Migration 0005 produces the expected schema and is reversible."""

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


async def _vector_dim(engine, table: str, column: str) -> int:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT a.atttypmod "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "WHERE c.relname = :table AND a.attname = :column"
                ),
                {"table": table, "column": column},
            )
        ).first()
    assert row is not None, f"{table}.{column} missing"
    return int(row[0])


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


async def _index_names(engine) -> set[str]:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE indexname IN ("
                    "  'idx_incidents_embedding', 'idx_runbooks_embedding', "
                    "  'ix_incidents_last_embedding_event_id'"
                    ")"
                )
            )
        ).all()
    return {r[0] for r in rows}


def test_upgrade_produces_1024_dim_columns_and_event_id(pg_dsn: str) -> None:
    async def _check() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            assert await _vector_dim(engine, "incidents", "embedding") == 1024
            assert await _vector_dim(engine, "runbooks", "embedding") == 1024
            assert await _column_exists(engine, "incidents", "last_embedding_event_id")
            assert await _index_names(engine) == {
                "idx_incidents_embedding",
                "idx_runbooks_embedding",
                "ix_incidents_last_embedding_event_id",
            }
        finally:
            await engine.dispose()

    asyncio.run(_check())


def test_round_trip_downgrade_then_upgrade(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)
    command.downgrade(cfg, "-1")

    async def _check_downgraded() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            assert await _vector_dim(engine, "incidents", "embedding") == 1536
            assert await _vector_dim(engine, "runbooks", "embedding") == 1536
            assert not await _column_exists(engine, "incidents", "last_embedding_event_id")
        finally:
            await engine.dispose()

    asyncio.run(_check_downgraded())

    command.upgrade(cfg, "head")

    async def _check_upgraded_again() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            assert await _vector_dim(engine, "incidents", "embedding") == 1024
            assert await _vector_dim(engine, "runbooks", "embedding") == 1024
            assert await _column_exists(engine, "incidents", "last_embedding_event_id")
        finally:
            await engine.dispose()

    asyncio.run(_check_upgraded_again())
