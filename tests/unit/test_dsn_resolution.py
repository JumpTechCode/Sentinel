"""Unit tests for the DSN-selection helper used by migrations/env.py."""

from __future__ import annotations

import pytest
from sentinel.persistence.dsn_resolution import choose_dsn


def test_explicit_alembic_url_wins_over_settings() -> None:
    assert (
        choose_dsn(
            "postgresql+asyncpg://test/from-alembic",
            lambda: "postgresql+asyncpg://prod/from-settings",
        )
        == "postgresql+asyncpg://test/from-alembic"
    )


def test_empty_alembic_url_falls_back_to_settings() -> None:
    assert (
        choose_dsn("", lambda: "postgresql+asyncpg://prod/from-settings")
        == "postgresql+asyncpg://prod/from-settings"
    )


def test_none_alembic_url_falls_back_to_settings() -> None:
    assert (
        choose_dsn(None, lambda: "postgresql+asyncpg://prod/from-settings")
        == "postgresql+asyncpg://prod/from-settings"
    )


def test_settings_failure_with_no_alembic_url_raises_runtime_error() -> None:
    def _explode() -> str:
        raise RuntimeError("missing config")

    with pytest.raises(RuntimeError, match="alembic could not resolve a Postgres DSN"):
        choose_dsn(None, _explode)


def test_settings_failure_chains_original_exception() -> None:
    class _OriginalError(Exception):
        pass

    def _explode() -> str:
        raise _OriginalError("missing config")

    with pytest.raises(RuntimeError) as exc_info:
        choose_dsn(None, _explode)
    assert isinstance(exc_info.value.__cause__, _OriginalError)
