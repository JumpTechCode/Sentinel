# tests/unit/schemas/test_diagnosis.py
"""Diagnosis enforces the spec's confidence rubric and evidence min_length."""

import pytest
from pydantic import ValidationError
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction


def _valid_evidence() -> EvidenceRef:
    return EvidenceRef(kind="deploy", id="deploy:abc123", note="why it matters")


def _valid_action() -> SuggestedAction:
    return SuggestedAction(
        description="Roll back deploy",
        command="git revert abc123",
        risk="medium",
        rationale="recent change suspected",
    )


def test_diagnosis_round_trip() -> None:
    d = Diagnosis(
        hypothesis="bad deploy abc123 broke checkout",
        confidence=0.78,
        reasoning="recent deploy at 06:25 immediately preceded errors",
        evidence=[_valid_evidence()],
        suggested_actions=[_valid_action()],
        likely_category="deploy",
    )
    assert d.confidence == 0.78
    assert d.suggested_actions[0].requires_human_approval is True


def test_confidence_must_be_in_unit_interval() -> None:
    with pytest.raises(ValidationError):
        Diagnosis(
            hypothesis="x",
            confidence=1.5,
            reasoning="x",
            evidence=[_valid_evidence()],
            suggested_actions=[],
            likely_category="deploy",
        )


def test_evidence_min_length_one() -> None:
    with pytest.raises(ValidationError, match="at least 1"):
        Diagnosis(
            hypothesis="x",
            confidence=0.5,
            reasoning="x",
            evidence=[],
            suggested_actions=[],
            likely_category="deploy",
        )


def test_rejects_bad_category() -> None:
    with pytest.raises(ValidationError):
        Diagnosis(
            hypothesis="x",
            confidence=0.5,
            reasoning="x",
            evidence=[_valid_evidence()],
            suggested_actions=[],
            likely_category="totally-bogus",
        )


def test_rejects_bad_evidence_kind() -> None:
    with pytest.raises(ValidationError):
        EvidenceRef(kind="hunch", id="deploy:abc", note="x")


def test_requires_human_approval_defaults_true() -> None:
    a = SuggestedAction(description="x", risk="low", rationale="y")
    assert a.requires_human_approval is True


def test_rejects_invalid_risk() -> None:
    with pytest.raises(ValidationError):
        SuggestedAction(description="x", risk="extreme", rationale="y")
