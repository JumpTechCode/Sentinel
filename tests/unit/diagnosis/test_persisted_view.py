"""PersistedDiagnosis -> DiagnosisView conversion, incl. the empty-evidence case."""

from __future__ import annotations

from decimal import Decimal

from sentinel.diagnosis.persisted import PersistedDiagnosis, to_view
from sentinel.schemas.api import DiagnosisView
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction


def _persisted(*, evidence: list[EvidenceRef], hallucinated: bool) -> PersistedDiagnosis:
    return PersistedDiagnosis(
        hypothesis="db connection pool exhausted",
        confidence=Decimal("0.82"),
        reasoning="pool saturated after deploy",
        evidence=evidence,
        suggested_actions=[
            SuggestedAction(description="roll back", risk="medium", rationale="recent deploy")
        ],
        likely_category="deploy",
        hallucinated_evidence=hallucinated,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=1234,
        token_usage={"input": 100, "output": 50},
    )


def test_to_view_maps_all_fields() -> None:
    ev = [EvidenceRef(kind="deploy", id="deploy:abc123", note="deploy 10m before")]
    view = to_view(_persisted(evidence=ev, hallucinated=False))
    assert isinstance(view, DiagnosisView)
    assert view.hypothesis == "db connection pool exhausted"
    assert view.confidence == 0.82  # Decimal -> float
    assert view.evidence == ev
    assert view.likely_category == "deploy"
    assert view.hallucinated_evidence is False
    assert view.model == "claude-sonnet-4-5"
    assert view.prompt_version == "v1"


def test_to_view_allows_empty_evidence() -> None:
    view = to_view(_persisted(evidence=[], hallucinated=True))
    assert view.evidence == []
    assert view.hallucinated_evidence is True
