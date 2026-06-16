# tests/unit/persistence/test_eval_run_repository.py
"""Unit tests for EvalRunRepository value objects + Protocol shape."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from uuid import uuid4

import pytest


def test_eval_run_record_is_frozen() -> None:
    from sentinel.persistence.repositories import EvalRunRecord

    rec = EvalRunRecord(
        id=uuid4(),
        status="ok",
        trigger="local",
        git_sha="abc123",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=3,
        fetcher_fixture_hash="deadbeef",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
        extra={},
        started_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.status = "failed"  # type: ignore[misc]


def test_eval_case_result_record_is_frozen_and_carries_incident_snapshot() -> None:
    from sentinel.persistence.repositories import EvalCaseResultRecord

    rec = EvalCaseResultRecord(
        run_id=uuid4(),
        case_id="cloudflare-2022-06-21-bgp",
        shot_index=0,
        case_status="ok",
        metrics={"category_match": 1.0},
        raw_response={"id": "msg_x", "content": []},
        diagnosis={"hypothesis": "BGP misconfig"},
        incident_id=uuid4(),
        incident_fingerprint="fp-abc",
        incident_title="Elevated 5xx errors",
        incident_severity="SEV1",
        token_usage={"input": 1200, "output": 800},
        latency_ms=2400,
        error_detail=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.case_status = "timeout"  # type: ignore[misc]
    assert rec.incident_fingerprint == "fp-abc"
    assert rec.incident_title == "Elevated 5xx errors"


def test_repository_protocol_exposes_expected_methods() -> None:
    from sentinel.persistence.repositories import EvalRunRepository

    proto_attrs = set(dir(EvalRunRepository))
    expected = {
        "start_run",
        "persist_shot",
        "finalize_run",
        "get_run",
        "get_latest_ok_run",
        "list_recent",
    }
    assert expected <= proto_attrs, f"missing methods: {expected - proto_attrs}"
