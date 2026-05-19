from __future__ import annotations

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pydantic import SecretStr, ValidationError
from sentinel.config.settings import Settings


def test_settings_load_from_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINEL_ENV", "test")
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")

    settings = Settings()

    assert settings.env == "test"
    assert "asyncpg" in settings.postgres_dsn
    assert settings.anthropic_model == "claude-sonnet-4-5"


def test_settings_rejects_unknown_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINEL_ENV", "bogus")
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")

    with pytest.raises(ValidationError):
        Settings()


def test_env_overrides_yaml_default(monkeypatch: MonkeyPatch) -> None:
    """Env vars must win over YAML file values (C1 precedence check).

    The real config/dev.yaml sets log_level: INFO.  We set SENTINEL_LOG_LEVEL=WARNING
    via env and assert the env value wins.
    """
    monkeypatch.setenv("SENTINEL_ENV", "dev")
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")
    # dev.yaml has log_level: INFO — env var must override it.
    monkeypatch.setenv("SENTINEL_LOG_LEVEL", "WARNING")

    settings = Settings()
    assert settings.log_level == "WARNING"


def test_settings_missing_required_field_raises(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("SENTINEL_POSTGRES_DSN", raising=False)
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")

    with pytest.raises(ValidationError, match="postgres_dsn"):
        Settings()


def test_secret_is_secretstr(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-real-secret-do-not-leak")

    s = Settings()
    assert str(s.anthropic_api_key) == "**********"
    assert s.anthropic_api_key.get_secret_value() == "sk-real-secret-do-not-leak"


def test_webhook_secrets_default_to_none(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://x/y")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "localhost:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")
    monkeypatch.delenv("SENTINEL_SENTRY_WEBHOOK_SECRET", raising=False)
    monkeypatch.delenv("SENTINEL_PAGERDUTY_WEBHOOK_SECRET", raising=False)
    monkeypatch.delenv("SENTINEL_DATADOG_WEBHOOK_SECRET", raising=False)
    monkeypatch.delenv("SENTINEL_GENERIC_WEBHOOK_SECRET", raising=False)
    s = Settings()
    assert s.sentry_webhook_secret is None
    assert s.pagerduty_webhook_secret is None
    assert s.datadog_webhook_secret is None
    assert s.generic_webhook_secret is None


def test_webhook_secrets_loaded_from_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://x/y")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "localhost:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")
    monkeypatch.setenv("SENTINEL_SENTRY_WEBHOOK_SECRET", "abc123")
    s = Settings()
    assert isinstance(s.sentry_webhook_secret, SecretStr)
    assert s.sentry_webhook_secret.get_secret_value() == "abc123"
