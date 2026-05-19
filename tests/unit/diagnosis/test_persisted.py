# tests/unit/diagnosis/test_persisted.py
from __future__ import annotations

from decimal import Decimal

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.schemas.diagnosis import SuggestedAction


def _make(**overrides: object) -> PersistedDiagnosis:
    base: dict[str, object] = dict(
        hypothesis="x",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
        latency_ms=100,
        token_usage={"input": 10, "output": 5},
    )
    base.update(overrides)
    return PersistedDiagnosis(**base)  # type: ignore[arg-type]


def test_persisted_allows_empty_evidence() -> None:
    pd = _make(evidence=[])
    assert pd.evidence == []


def test_persisted_is_frozen() -> None:
    import dataclasses

    import pytest

    pd = _make()
    with pytest.raises(dataclasses.FrozenInstanceError):
        pd.hypothesis = "y"  # type: ignore[misc]


def test_persisted_carries_suggested_actions() -> None:
    sa = SuggestedAction(description="d", risk="low", rationale="r", requires_human_approval=True)
    pd = _make(suggested_actions=[sa])
    assert len(pd.suggested_actions) == 1
