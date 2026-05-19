# tests/unit/diagnosis/test_agent.py
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.errors import DiagnosisInvalid, LLMTimeout
from sentinel.diagnosis.llm_client import LLMResult
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.schemas.api import IncidentDetailResponse

from tests.unit.diagnosis.fakes import make_context, make_deploy


def _incident(uid: UUID) -> IncidentDetailResponse:
    return IncidentDetailResponse(
        id=uid,
        source="generic",
        external_id="ext-1",
        service="payments-api",
        severity="SEV1",
        status="open",
        title="t",
        fingerprint="f",
        opened_at=datetime.now(UTC),
    )


def _bundle() -> PromptBundle:
    return PromptBundle(version="v1", system_text="sys", sha256_hex="0" * 64)


def _deps(llm: Any) -> DiagnosisDeps:
    return DiagnosisDeps(
        llm=llm, prompt=_bundle(), max_input_tokens=12_000, audit_logger=MagicMock()
    )


pytestmark = pytest.mark.asyncio


async def test_happy_path_returns_persisted_diagnosis() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    tool_input = dict(
        hypothesis="h",
        confidence=0.7,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "claude-sonnet-4-5"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=tool_input,
        input_tokens=100,
        output_tokens=50,
        stop_reason="tool_use",
        latency_ms=123,
    )
    out = await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
    assert isinstance(out, PersistedDiagnosis)
    assert out.confidence == Decimal("0.7")
    assert out.hallucinated_evidence is False
    assert out.latency_ms == 123
    assert out.token_usage["input"] == 100
    assert out.prompt_version == "v1"


async def test_hallucinated_caps_confidence_and_drops_invented() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    tool_input = dict(
        hypothesis="h",
        confidence=0.9,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:never", "note": "?"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=tool_input,
        input_tokens=10,
        output_tokens=5,
        stop_reason="tool_use",
        latency_ms=10,
    )
    out = await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
    assert out.hallucinated_evidence is True
    assert out.confidence == Decimal("0.4")
    assert out.evidence == []


async def test_schema_invalid_then_valid_succeeds_after_one_retry() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    bad = dict(
        hypothesis="",  # empty — fails min_length
        confidence=0.5,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    good = dict(
        hypothesis="h",
        confidence=0.5,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.side_effect = [
        LLMResult(
            tool_input=bad, input_tokens=10, output_tokens=5, stop_reason="tool_use", latency_ms=5
        ),
        LLMResult(
            tool_input=good, input_tokens=10, output_tokens=5, stop_reason="tool_use", latency_ms=5
        ),
    ]
    audit = MagicMock()
    deps = DiagnosisDeps(llm=llm, prompt=_bundle(), max_input_tokens=12_000, audit_logger=audit)
    out = await diagnose(_incident(ctx.incident_id), ctx, deps)
    assert out.hypothesis == "h"
    assert llm.diagnose_call.await_count == 2
    assert audit.record.call_count == 2


async def test_audit_logger_records_one_line_per_call_attempt() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    good = dict(
        hypothesis="h",
        confidence=0.7,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "claude-sonnet-4-5"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=good,
        input_tokens=100,
        output_tokens=50,
        stop_reason="tool_use",
        latency_ms=10,
    )
    audit = MagicMock()
    deps = DiagnosisDeps(llm=llm, prompt=_bundle(), max_input_tokens=12_000, audit_logger=audit)
    await diagnose(_incident(ctx.incident_id), ctx, deps)

    assert audit.record.call_count == 1
    kwargs = audit.record.call_args.kwargs
    assert kwargs["model"] == "claude-sonnet-4-5"
    assert kwargs["prompt_version"] == "v1"
    assert kwargs["input_tokens"] == 100
    assert kwargs["output_tokens"] == 50
    assert kwargs["retry_attempt"] == 1
    assert kwargs["incident_id"] == str(ctx.incident_id)
    assert "cost_usd" in kwargs and "prompt_sha" in kwargs


async def test_double_schema_invalid_raises_DiagnosisInvalid() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    bad = dict(
        hypothesis="",
        confidence=0.5,
        reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=bad,
        input_tokens=10,
        output_tokens=5,
        stop_reason="tool_use",
        latency_ms=5,
    )
    with pytest.raises(DiagnosisInvalid):
        await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))


async def test_timeout_bubbles_LLMTimeout() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.side_effect = LLMTimeout("t")
    with pytest.raises(LLMTimeout):
        await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
