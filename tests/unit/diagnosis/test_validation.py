# tests/unit/diagnosis/test_validation.py
from __future__ import annotations

from sentinel.diagnosis.validation import verify_evidence
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef

from tests.unit.diagnosis.fakes import (
    make_context,
    make_deploy,
    make_log,
    make_related,
    make_similar,
)


def _diag(*refs: EvidenceRef) -> Diagnosis:
    return Diagnosis(
        hypothesis="h",
        confidence=0.7,
        reasoning="r",
        evidence=list(refs) or [EvidenceRef(kind="deploy", id="deploy:placeholder", note="n")],
        suggested_actions=[],
        likely_category="deploy",
    )


def test_all_verified_returns_no_hallucination() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    diag = _diag(EvidenceRef(kind="deploy", id="deploy:abc", note="touched code"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is False
    assert verdict.invented == []
    assert len(verdict.verified) == 1


def test_invented_id_is_flagged() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    diag = _diag(EvidenceRef(kind="deploy", id="deploy:never_existed", note="?"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.invented) == 1
    assert verdict.verified == []


def test_wrong_kind_counts_as_invented_even_when_id_exists_under_another_kind() -> None:
    log = make_log(0)
    ctx = make_context(logs=[log])
    diag = _diag(EvidenceRef(kind="deploy", id="log:0", note="wrong-kind ref"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.invented) == 1


def test_active_and_related_alerts_share_the_related_kind() -> None:
    related = make_related()
    active = make_related()
    ctx = make_context(related=[related], active=[active])
    diag = _diag(
        EvidenceRef(kind="related_alert", id=related.id, note="r"),
        EvidenceRef(kind="related_alert", id=active.id, note="a"),
    )
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is False
    assert len(verdict.verified) == 2


def test_bare_id_matches_prefixed_bucket_entry() -> None:
    # The prompt renders citations as `[deploy:abc]` but real LLM completions
    # routinely split on the colon and emit `{"kind":"deploy","id":"abc"}`.
    # That MUST verify against a bucket that holds `"deploy:abc"`, otherwise
    # evidence_quality collapses to ~0 on every corpus case (eval harness bug
    # surfaced 2026-05-20).
    ctx = make_context(deploys=[make_deploy("abc")])
    diag = _diag(EvidenceRef(kind="deploy", id="abc", note="bare id form"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is False
    assert len(verdict.verified) == 1


def test_bare_id_does_not_match_wrong_kind_bucket() -> None:
    # Lenient match must NOT cross kind boundaries. A bare id "0" claimed as
    # a deploy must not silently resolve to log:0.
    ctx = make_context(logs=[make_log(0)])
    diag = _diag(EvidenceRef(kind="deploy", id="0", note="wrong kind, bare id"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.invented) == 1


def test_mixed_some_verified_some_invented() -> None:
    deploy = make_deploy("abc")
    similar = make_similar()
    ctx = make_context(deploys=[deploy], similar=[similar])
    diag = _diag(
        EvidenceRef(kind="deploy", id=deploy.id, note="ok"),
        EvidenceRef(kind="similar_incident", id="similar:nope", note="invented"),
    )
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.verified) == 1
    assert len(verdict.invented) == 1
