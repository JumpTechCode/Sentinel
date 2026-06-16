"""POST /incidents/{id}/diagnose end-to-end with a fake LLM (creditless)."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.llm_client import LLMResult
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.observability.llm_audit import LLMAuditLogger
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.context import DeployItem, FetcherResult, IncidentContext

pytestmark = pytest.mark.integration

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "integration"
    / "diagnosis"
    / "fixtures"
    / "anthropic_response_ok.json"
)


# ---------------------------------------------------------------------------
# Fake LLM — mirrors _FakeAnthropicClient in tests/integration/diagnosis/test_end_to_end.py
# ---------------------------------------------------------------------------


class _FakeAnthropicClient:
    """Stand-in for AnthropicClient — returns canned tool input without a live API call."""

    model = "claude-sonnet-4-5"

    def __init__(self, tool_input: dict[str, Any]) -> None:
        self._tool_input = tool_input

    async def diagnose_call(self, **_: Any) -> LLMResult:
        return LLMResult(
            tool_input=self._tool_input,
            input_tokens=200,
            output_tokens=80,
            stop_reason="tool_use",
            latency_ms=42,
        )


def _fake_diagnosis_deps() -> DiagnosisDeps:
    """Build a DiagnosisDeps that uses the fake LLM (no Anthropic spend)."""
    tool_input = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    audit_path = Path(tempfile.gettempdir()) / f"audit-api-{uuid4().hex}.log"
    return DiagnosisDeps(
        llm=_FakeAnthropicClient(tool_input),  # type: ignore[arg-type]
        prompt=PromptBundle(version="v1", system_text="sys", sha256_hex="0" * 64),
        max_input_tokens=12_000,
        audit_logger=LLMAuditLogger(audit_path),
    )


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------


def _make_context(incident_id: UUID, assembled_at: datetime) -> IncidentContext:
    """Build an IncidentContext whose deploy ID matches the canned fixture evidence.

    The fixture (anthropic_response_ok.json) cites evidence id ``deploy:abc123``;
    since a deploy id is ``f"deploy:{sha}"``, ``sha="abc123"`` yields that id and
    the evidence-citation gate passes without flagging a hallucination.
    """
    deploy = DeployItem(
        id="deploy:abc123",
        service="payments-api",
        sha="abc123",
        pr_number=4421,
        pr_title="Switch idempotency store to Redis cluster",
        pr_diff_summary="Changed backend from in-process dict to Redis cluster.",
        deployed_at=assembled_at,
        deployed_by="alice",
    )

    def fr_ok(data: list[Any]) -> FetcherResult[Any]:
        return FetcherResult(status="ok", data=data, error=None, fetched_at=assembled_at)

    return IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        recent_deploys=fr_ok([deploy]),
        related_alerts=fr_ok([]),
        similar_incidents=fr_ok([]),
        runbooks=fr_ok([]),
        recent_logs=fr_ok([]),
        active_alerts=fr_ok([]),
    )


async def _seed_incident_with_context(app: FastAPI) -> UUID:
    """Create an incident + enrichment context via the real repos on app.state."""
    incident_repo = app.state.incident_repo
    alert = NormalizedAlert(
        source="generic",
        external_id=f"ext-diagnose-api-{uuid4().hex[:8]}",
        service="payments-api",
        severity="SEV1",
        title="boom",
        raw_payload={},
        received_at=datetime.now(UTC),
    )
    incident_id: UUID = await incident_repo.create_from_alert(
        alert, fingerprint=f"fp-diagnose-api-{uuid4().hex[:8]}"
    )

    assembled_at = datetime.now(UTC)
    await incident_repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=uuid4(),
        context=_make_context(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=None,
    )
    return incident_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_diagnose_persists_and_appears_in_detail(
    client: Any, app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1. Seed an incident + enrichment context via the real repos on app.state.
    incident_id = await _seed_incident_with_context(app)

    # 2. Override diagnosis deps with a fake LLM so diagnose() returns a valid
    #    PersistedDiagnosis with no Anthropic call. monkeypatch restores the real
    #    deps after the test regardless of the `app` fixture's scope.
    monkeypatch.setattr(app.state, "diagnosis_deps", _fake_diagnosis_deps())

    # 3. Diagnose. First diagnosis of a freshly-seeded incident is always "new".
    resp = await client.post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["persisted"] == "new"
    assert body["diagnosis"]["hypothesis"]  # non-empty

    # 4. It surfaces in the detail read.
    detail = await client.get(f"/incidents/{incident_id}")
    assert detail.status_code == 200
    assert len(detail.json()["diagnoses"]) == 1


async def test_diagnose_404_for_unknown_incident(client: Any) -> None:
    resp = await client.post("/incidents/00000000-0000-0000-0000-000000000000/diagnose")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"
