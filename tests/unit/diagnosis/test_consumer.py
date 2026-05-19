# tests/unit/diagnosis/test_consumer.py
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.persistence.repositories import StoredEnrichmentContext

from tests.unit.diagnosis.fakes import make_context

pytestmark = pytest.mark.asyncio


def _msg(payload: dict[str, Any] | None = None, *, raw: bytes | None = None) -> Any:
    if raw is not None:
        value = raw
    elif payload is not None:
        value = json.dumps(payload).encode()
    else:
        value = b""
    return SimpleNamespace(value=value, offset=0, partition=0)


class _FakeKafkaConsumer:
    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)
        self.committed = 0

    def __aiter__(self) -> _FakeKafkaConsumer:
        return self

    async def __anext__(self) -> Any:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def commit(self) -> None:
        self.committed += 1


def _deps_namespace(*, incident_repo: Any, diagnosis_repo: Any) -> Any:
    """ConsumerDeps-shaped namespace for tests (avoids importing real dep types)."""
    return SimpleNamespace(
        llm=SimpleNamespace(model="m"),
        prompt=PromptBundle(version="v1", system_text="s", sha256_hex="0" * 64),
        max_input_tokens=12_000,
        incident_repo=incident_repo,
        diagnosis_repo=diagnosis_repo,
        audit_logger=MagicMock(),
        agent_deps=lambda: SimpleNamespace(
            llm=SimpleNamespace(model="m"),
            prompt=PromptBundle(version="v1", system_text="s", sha256_hex="0" * 64),
            max_input_tokens=12_000,
            audit_logger=MagicMock(),
        ),
    )


async def test_skips_non_enriched_event_types() -> None:
    incident_id = uuid4()
    msgs = [
        _msg(
            {
                "event": "incident.opened",
                "event_id": str(uuid4()),
                "incident_id": str(incident_id),
                "fingerprint": "f",
                "source": "s",
                "ts": datetime.now(UTC).isoformat(),
            }
        )
    ]
    incident_repo = AsyncMock()
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    diagnosis_repo.save_with_outbox.assert_not_awaited()


async def test_missing_incident_increments_and_commits() -> None:
    incident_id = uuid4()
    msgs = [
        _msg(
            {
                "event": "incident.enriched",
                "event_id": str(uuid4()),
                "incident_id": str(incident_id),
                "fingerprint": "f",
                "source": "s",
                "ts": datetime.now(UTC).isoformat(),
            }
        )
    ]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = None
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    assert consumer._consumer.committed == 1


async def test_happy_path_persists_and_emits() -> None:
    incident_id = uuid4()
    upstream_event_id = uuid4()
    msgs = [
        _msg(
            {
                "event": "incident.enriched",
                "event_id": str(upstream_event_id),
                "incident_id": str(incident_id),
                "fingerprint": "f",
                "source": "s",
                "ts": datetime.now(UTC).isoformat(),
            }
        )
    ]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = SimpleNamespace(
        id=incident_id,
        source="generic",
        external_id="ext-1",
        service="payments-api",
        severity="SEV1",
        status="open",
        title="t",
        fingerprint="f",
        opened_at=datetime.now(UTC),
    )
    ctx = make_context(incident_id=incident_id)
    incident_repo.get_enrichment_context.return_value = StoredEnrichmentContext(
        context=ctx,
        assembled_at=ctx.assembled_at,
        version=1,
        last_event_id=uuid4(),
    )
    diagnosis_repo = AsyncMock()
    diagnosis_repo.save_with_outbox.return_value = (uuid4(), "new")
    agent_fn = AsyncMock()
    agent_fn.return_value = PersistedDiagnosis(
        hypothesis="h",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
        latency_ms=10,
        token_usage={},
    )
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_awaited_once()
    diagnosis_repo.save_with_outbox.assert_awaited_once()
    assert consumer._consumer.committed == 1


def _enriched_msg(incident_id: Any | None = None) -> Any:
    return _msg(
        {
            "event": "incident.enriched",
            "event_id": str(uuid4()),
            "incident_id": str(incident_id or uuid4()),
            "fingerprint": "f",
            "source": "s",
            "ts": datetime.now(UTC).isoformat(),
        }
    )


def _stub_incident(incident_id: Any) -> Any:
    return SimpleNamespace(
        id=incident_id,
        source="generic",
        external_id="ext-1",
        service="payments-api",
        severity="SEV1",
        status="open",
        title="t",
        fingerprint="f",
        opened_at=datetime.now(UTC),
    )


async def test_invalid_json_envelope_increments_and_commits() -> None:
    from sentinel.observability.metrics import diagnosis_invalid_events_total

    before = diagnosis_invalid_events_total._value.get()
    msgs = [_msg(raw=b"not json")]
    incident_repo = AsyncMock()
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    incident_repo.get.assert_not_awaited()
    assert consumer._consumer.committed == 1
    assert diagnosis_invalid_events_total._value.get() == before + 1


async def test_invalid_envelope_schema_increments_and_commits() -> None:
    """Event-type filter passes (`incident.enriched`) but envelope schema
    validation fails — Pydantic rejects the missing required fields. Counter
    must increment and the offset must commit (poison-pill committed)."""
    from sentinel.observability.metrics import diagnosis_invalid_events_total

    before = diagnosis_invalid_events_total._value.get()
    msgs = [
        _msg(
            {
                "event": "incident.enriched",
                # missing: event_id, incident_id, ts
            }
        )
    ]
    incident_repo = AsyncMock()
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    incident_repo.get.assert_not_awaited()
    assert consumer._consumer.committed == 1
    assert diagnosis_invalid_events_total._value.get() == before + 1


async def test_missing_context_increments_and_commits() -> None:
    """Incident row exists but `get_enrichment_context` returns None — the
    enricher never wrote a context for this incident. Committed and counted."""
    from sentinel.observability.metrics import diagnosis_failures_total

    incident_id = uuid4()
    msgs = [_enriched_msg(incident_id)]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = _stub_incident(incident_id)
    incident_repo.get_enrichment_context.return_value = None
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()

    before = diagnosis_failures_total.labels(reason="missing_context")._value.get()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    assert consumer._consumer.committed == 1
    assert diagnosis_failures_total.labels(reason="missing_context")._value.get() == before + 1


async def test_diagnosis_invalid_increments_schema_and_commits() -> None:
    """Agent raises DiagnosisInvalid (schema-invalid after retry). Commit the
    offset — no infinite retry on a permanently bad prompt+context combo —
    and bump the `schema` failure reason."""
    from sentinel.diagnosis.errors import DiagnosisInvalid
    from sentinel.observability.metrics import diagnosis_failures_total

    incident_id = uuid4()
    msgs = [_enriched_msg(incident_id)]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = _stub_incident(incident_id)
    ctx = make_context(incident_id=incident_id)
    incident_repo.get_enrichment_context.return_value = StoredEnrichmentContext(
        context=ctx, assembled_at=ctx.assembled_at, version=1, last_event_id=uuid4()
    )
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock(side_effect=DiagnosisInvalid("schema bad"))

    before = diagnosis_failures_total.labels(reason="schema")._value.get()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_awaited_once()
    diagnosis_repo.save_with_outbox.assert_not_awaited()
    assert consumer._consumer.committed == 1
    assert diagnosis_failures_total.labels(reason="schema")._value.get() == before + 1


@pytest.mark.parametrize(
    "error_type_name,expected_reason",
    [
        ("LLMTimeout", "timeout"),
        ("LLMTransport", "transport"),
        ("LLMNoToolCall", "no_tool_call"),
    ],
)
async def test_llm_error_increments_reason_and_does_not_commit(
    error_type_name: str, expected_reason: str
) -> None:
    """LLM* errors are transient/transport-shaped — counter increments with the
    right reason, but the offset is NOT committed so Kafka redelivers."""
    from sentinel.diagnosis import errors as diag_errors
    from sentinel.observability.metrics import diagnosis_failures_total

    error_cls = getattr(diag_errors, error_type_name)
    incident_id = uuid4()
    msgs = [_enriched_msg(incident_id)]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = _stub_incident(incident_id)
    ctx = make_context(incident_id=incident_id)
    incident_repo.get_enrichment_context.return_value = StoredEnrichmentContext(
        context=ctx, assembled_at=ctx.assembled_at, version=1, last_event_id=uuid4()
    )
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock(side_effect=error_cls("boom"))

    before = diagnosis_failures_total.labels(reason=expected_reason)._value.get()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_awaited_once()
    diagnosis_repo.save_with_outbox.assert_not_awaited()
    # Critical: no commit on LLM transient errors — Kafka must redeliver.
    assert consumer._consumer.committed == 0
    assert diagnosis_failures_total.labels(reason=expected_reason)._value.get() == before + 1


async def test_unexpected_exception_increments_exception_reason_and_does_not_commit() -> None:
    """Anything not caught by a named branch (e.g., a repository raising an
    unexpected error) is caught by the outer try/except and counted under
    `reason='exception'`. Offset NOT committed → Kafka redelivers (#19)."""
    from sentinel.observability.metrics import diagnosis_failures_total

    incident_id = uuid4()
    msgs = [_enriched_msg(incident_id)]
    incident_repo = AsyncMock()
    # Sabotage the repository: an unexpected error type (not DiagnosisInvalid,
    # not LLMError) bubbles out of _handle into run()'s outer catch.
    incident_repo.get.side_effect = RuntimeError("db dropped")
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()

    before = diagnosis_failures_total.labels(reason="exception")._value.get()
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    assert consumer._consumer.committed == 0
    assert diagnosis_failures_total.labels(reason="exception")._value.get() == before + 1


async def test_duplicate_status_still_commits() -> None:
    incident_id = uuid4()
    msgs = [
        _msg(
            {
                "event": "incident.enriched",
                "event_id": str(uuid4()),
                "incident_id": str(incident_id),
                "fingerprint": "f",
                "source": "s",
                "ts": datetime.now(UTC).isoformat(),
            }
        )
    ]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = SimpleNamespace(
        id=incident_id,
        source="generic",
        external_id="e",
        service="s",
        severity="SEV2",
        status="open",
        title="t",
        fingerprint="f",
        opened_at=datetime.now(UTC),
    )
    ctx = make_context(incident_id=incident_id)
    incident_repo.get_enrichment_context.return_value = StoredEnrichmentContext(
        context=ctx,
        assembled_at=ctx.assembled_at,
        version=1,
        last_event_id=uuid4(),
    )
    diagnosis_repo = AsyncMock()
    diagnosis_repo.save_with_outbox.return_value = (uuid4(), "duplicate")
    agent_fn = AsyncMock()
    agent_fn.return_value = PersistedDiagnosis(
        hypothesis="h",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
        latency_ms=10,
        token_usage={},
    )
    consumer = DiagnosisConsumer(
        consumer=_FakeKafkaConsumer(msgs),
        deps=_deps_namespace(incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    assert consumer._consumer.committed == 1
