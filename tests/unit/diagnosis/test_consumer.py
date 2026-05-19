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
