# tests/unit/observability/test_llm_audit.py
from __future__ import annotations

import json
from pathlib import Path

from sentinel.observability.llm_audit import LLMAuditLogger


def test_writes_one_json_line_per_call(tmp_path: Path) -> None:
    path = tmp_path / "audit.log"
    logger = LLMAuditLogger(path)
    logger.record(
        incident_id="i1",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        prompt_sha="abc",
        input_tokens=100,
        output_tokens=20,
        cost_usd="0.001",
        retry_attempt=1,
    )
    logger.record(
        incident_id="i2",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        prompt_sha="abc",
        input_tokens=200,
        output_tokens=40,
        cost_usd="0.002",
        retry_attempt=1,
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    obj0 = json.loads(lines[0])
    assert obj0["incident_id"] == "i1"
    assert "ts" in obj0


def test_creates_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deep" / "audit.log"
    logger = LLMAuditLogger(path)
    logger.record(
        incident_id="i",
        model="m",
        prompt_version="v1",
        prompt_sha="s",
        input_tokens=1,
        output_tokens=1,
        cost_usd="0",
        retry_attempt=1,
    )
    assert path.exists()


def test_accepts_string_path(tmp_path: Path) -> None:
    path = tmp_path / "audit.log"
    logger = LLMAuditLogger(str(path))
    logger.record(
        incident_id="i",
        model="m",
        prompt_version="v1",
        prompt_sha="s",
        input_tokens=1,
        output_tokens=1,
        cost_usd="0",
        retry_attempt=1,
    )
    assert path.exists()
