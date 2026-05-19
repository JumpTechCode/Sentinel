# tests/integration/diagnosis/test_migration_round_trip.py
"""Migration 0004 — alembic upgrade head → downgrade -1 → upgrade head round-trip.

Verifies that the diagnoses-idempotency migration is cleanly reversible against
a real Postgres. Uniqueness-constraint behavior is exercised by the integration
tests in tests/integration/persistence/test_diagnosis_repository.py.

Note: alembic's sync ``command.upgrade``/``command.downgrade`` work here
because env.py calls ``asyncio.run(run_migrations_online())``. This is only
safe from a non-async context (no running event loop), which pytest provides.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def test_upgrade_then_downgrade_then_upgrade_is_clean(pg_dsn: str) -> None:
    """0004 round-trip: head → -1 → head must succeed without errors."""
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "-1")
    command.upgrade(cfg, "head")
