# tests/unit/diagnosis/test_prompt.py
from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from sentinel.diagnosis.prompt import PromptBundle, serialize
from sentinel.schemas.api import IncidentDetailResponse

from tests.unit.diagnosis.fakes import (
    fetcher_degraded,
    make_context,
    make_deploy,
    make_log,
    make_runbook,
    make_similar,
)


def test_load_v1_succeeds() -> None:
    bundle = PromptBundle.load("v1")
    assert bundle.version == "v1"
    assert len(bundle.sha256_hex) == 64
    assert "submit_diagnosis" in bundle.system_text


def test_load_unknown_version_raises() -> None:
    with pytest.raises(FileNotFoundError):
        PromptBundle.load("v99")


def test_hash_mismatch_logs_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Point the loader at a temp dir where the baseline doesn't match.
    monkeypatch.setattr("sentinel.diagnosis.prompt._PROMPTS_DIR", tmp_path)
    (tmp_path / "vX.md").write_text("hello")
    (tmp_path / "vX.sha256").write_text("0" * 64 + "\n")

    with caplog.at_level(logging.WARNING, logger="sentinel.diagnosis.prompt"):
        bundle = PromptBundle.load("vX")

    assert bundle.system_text == "hello"
    assert any("prompt_sha_mismatch" in rec.message for rec in caplog.records)


def test_missing_baseline_file_logs_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sentinel.diagnosis.prompt._PROMPTS_DIR", tmp_path)
    (tmp_path / "vY.md").write_text("hi")
    # No baseline file written.

    with caplog.at_level(logging.WARNING, logger="sentinel.diagnosis.prompt"):
        bundle = PromptBundle.load("vY")

    assert bundle.version == "vY"
    assert any("prompt_sha_baseline_missing" in rec.message for rec in caplog.records)


def _incident(incident_id: UUID) -> IncidentDetailResponse:
    return IncidentDetailResponse(
        id=incident_id,
        source="generic",
        external_id="ext-1",
        service="payments-api",
        severity="SEV1",
        status="open",
        title="Elevated 5xx errors",
        fingerprint="f1",
        opened_at=datetime(2026, 5, 19, 13, 42, tzinfo=UTC),
    )


def test_serialize_includes_incident_header_and_section_ids() -> None:
    incident_id = UUID("00000000-0000-0000-0000-000000000001")
    ctx = make_context(
        deploys=[make_deploy("abc123")],
        similar=[make_similar()],
        runbooks=[make_runbook()],
        logs=[make_log(0)],
        incident_id=incident_id,
    )
    text = serialize(_incident(incident_id), ctx)
    assert "service:  payments-api" in text
    assert "[deploy:abc123]" in text
    assert "[similar:" in text
    assert "[runbook:" in text
    assert "[log:0]" in text
    assert "status=ok" in text


def test_serialize_marks_degraded_sections() -> None:
    incident_id = UUID("00000000-0000-0000-0000-000000000002")
    ctx = make_context(incident_id=incident_id)
    ctx = ctx.model_copy(update={"similar_incidents": fetcher_degraded("not_configured")})
    text = serialize(_incident(incident_id), ctx)
    assert "SIMILAR INCIDENTS" in text
    assert "status=degraded" in text
