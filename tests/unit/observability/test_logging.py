# tests/unit/observability/test_logging.py
"""Logging config produces JSON in prod, attaches context vars."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import structlog
from sentinel.config.settings import Settings
from sentinel.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_llm_audit_logger,
)


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "postgres_dsn": "postgresql+asyncpg://u:p@localhost/d",
        "redis_url": "redis://localhost:6379/0",
        "kafka_brokers": "localhost:9092",
        "anthropic_api_key": "x",
        "log_level": "INFO",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_json_renderer_in_prod(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    s = _settings(env="prod", llm_audit_log_path=str(tmp_path / "llm.log"))
    configure_logging(s)
    structlog.reset_defaults()
    configure_logging(s)
    log = structlog.get_logger("test")
    bind_request_context(incident_id="abc-123", correlation_id="corr-1", source="sentry")
    try:
        log.info("hello", k="v")
    finally:
        clear_request_context()

    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "hello"
    assert payload["k"] == "v"
    assert payload["incident_id"] == "abc-123"
    assert payload["correlation_id"] == "corr-1"
    assert payload["source"] == "sentry"


def test_llm_audit_logger_writes_to_audit_path(tmp_path: Path) -> None:
    audit = tmp_path / "llm-audit.log"
    s = _settings(env="dev", llm_audit_log_path=str(audit))
    configure_logging(s)
    audit_log = get_llm_audit_logger()
    audit_log.info("prompt-and-response", prompt="hi", response="world")
    for h in logging.getLogger("sentinel.llm_audit").handlers:
        h.flush()
    text = audit.read_text(encoding="utf-8")
    assert "prompt-and-response" in text


def test_context_vars_bind_and_clear() -> None:
    s = _settings()
    configure_logging(s)
    bind_request_context(incident_id="i", correlation_id="c", source="x")
    assert structlog.contextvars.get_contextvars() == {
        "incident_id": "i",
        "correlation_id": "c",
        "source": "x",
    }
    clear_request_context()
    assert structlog.contextvars.get_contextvars() == {}


def test_bind_request_context_omits_none_fields() -> None:
    clear_request_context()
    bind_request_context(correlation_id="c")
    ctx = structlog.contextvars.get_contextvars()
    assert ctx == {"correlation_id": "c"}
    clear_request_context()
