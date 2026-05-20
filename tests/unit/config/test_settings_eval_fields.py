"""Tests for the eval-harness Settings fields (Work Area K, PR 3b)."""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pydantic import ValidationError
from sentinel.config.settings import Settings


def _set_required_env(monkeypatch: MonkeyPatch) -> None:
    """Set the env vars required to instantiate Settings."""
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")
    # Make sure no eval-mode env vars leak from the caller's shell.
    monkeypatch.delenv("SENTINEL_EVAL_MODE", raising=False)
    monkeypatch.delenv("SENTINEL_EVAL_CORPUS_DIR", raising=False)
    monkeypatch.delenv("SENTINEL_EVAL_CASSETTE_DIR", raising=False)


def test_default_eval_mode_is_false(monkeypatch: MonkeyPatch) -> None:
    _set_required_env(monkeypatch)

    settings = Settings()

    assert settings.eval_mode is False
    assert settings.eval_corpus_dir is None
    assert settings.eval_cassette_dir is None


def test_eval_mode_true_without_corpus_dir_raises(monkeypatch: MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SENTINEL_EVAL_MODE", "true")

    with pytest.raises(ValidationError, match="eval_corpus_dir"):
        Settings()


def test_eval_mode_true_with_corpus_dir_succeeds(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SENTINEL_EVAL_MODE", "true")
    monkeypatch.setenv("SENTINEL_EVAL_CORPUS_DIR", str(tmp_path))

    settings = Settings()

    assert settings.eval_mode is True
    assert settings.eval_corpus_dir == tmp_path
    assert settings.eval_cassette_dir is None


def test_eval_cassette_dir_independent_of_corpus_dir(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Cassette dir can be set on its own (eval_mode stays off — valid)."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("SENTINEL_EVAL_CASSETTE_DIR", str(tmp_path))

    settings = Settings()

    assert settings.eval_mode is False
    assert settings.eval_corpus_dir is None
    assert settings.eval_cassette_dir == tmp_path
