# tests/integration/diagnosis/test_end_to_end.py
"""End-to-end: publish incident.enriched → DiagnosisConsumer writes diagnosis row + incident.diagnosed outbox event.

Real Postgres + real Kafka; the Anthropic call is faked via a recorded tool_input fixture.
Migrations are applied once per session by the ``_migrate`` autouse fixture in conftest.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.deps import ConsumerDeps
from sentinel.diagnosis.llm_client import LLMResult
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.observability.llm_audit import LLMAuditLogger
from sentinel.persistence.models import DiagnosisModel, OutboxEventModel
from sentinel.persistence.repositories import (
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.context import DeployItem, FetcherResult, IncidentContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parent / "fixtures" / "anthropic_response_ok.json"


class _FakeAnthropicClient:
    """Stand-in for AnthropicClient — returns the recorded tool input without any live API call."""

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


def _make_context(incident_id: Any, assembled_at: datetime, deploy: DeployItem) -> IncidentContext:
    def fr_ok(data: list[Any] | None = None) -> FetcherResult[Any]:
        return FetcherResult(status="ok", data=data or [], error=None, fetched_at=assembled_at)

    return IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        recent_deploys=fr_ok([deploy]),
        related_alerts=fr_ok(),
        similar_incidents=fr_ok(),
        runbooks=fr_ok(),
        recent_logs=fr_ok(),
        active_alerts=fr_ok(),
    )


@pytest.mark.asyncio
async def test_end_to_end_publish_enriched_emits_diagnosed(
    pg_dsn: str,
    kafka_brokers: str,
) -> None:
    """Publish incident.enriched → consumer writes diagnosis row + incident.diagnosed outbox event."""
    topic = "sentinel.incidents"
    engine = create_async_engine(pg_dsn)
    try:
        sf = make_session_factory(engine)
        incident_repo = PostgresIncidentRepository(sf)
        diagnosis_repo = PostgresDiagnosisRepository(sf)

        # Seed incident — use unique external_id to avoid the source+external_id unique constraint
        # when tests share the same container (the _reset_state fixture TRUNCATEs between tests,
        # but we use a uuid suffix to be safe).
        alert = NormalizedAlert(
            source="generic",
            external_id=f"ext-diag-e2e-{uuid4().hex[:8]}",
            service="payments-api",
            severity="SEV1",
            title="boom",
            raw_payload={},
            received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(
            alert, fingerprint=f"fp-diag-e2e-{uuid4().hex[:8]}"
        )

        # Seed the enrichment context.  The fake fixture's evidence cites id="deploy:abc123".
        # deploy_id(sha) returns f"deploy:{sha}", so sha="abc123" → id="deploy:abc123". ✓
        assembled_at = datetime.now(UTC)
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
        ctx = _make_context(incident_id, assembled_at, deploy)
        await incident_repo.write_enrichment_context(
            incident_id=incident_id,
            event_id=uuid4(),
            context=ctx,
            assembled_at=assembled_at,
            outbox_event=None,
        )

        # Wire up the consumer with the fake LLM (no live Anthropic calls).
        tool_input = json.loads(_FIXTURE.read_text(encoding="utf-8"))
        kafka_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_brokers,
            group_id=f"diag-e2e-{uuid4()}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await kafka_consumer.start()

        audit_path = Path(tempfile.gettempdir()) / f"audit-{uuid4().hex}.log"
        deps = ConsumerDeps(
            llm=_FakeAnthropicClient(tool_input),  # type: ignore[arg-type]
            prompt=PromptBundle(version="v1", system_text="sys", sha256_hex="0" * 64),
            max_input_tokens=12_000,
            incident_repo=incident_repo,
            diagnosis_repo=diagnosis_repo,
            audit_logger=LLMAuditLogger(audit_path),
        )
        diagnoser = DiagnosisConsumer(
            consumer=kafka_consumer,
            deps=deps,
            agent_fn=diagnose,
            topic=topic,
            downstream_topic=topic,
        )

        # Publish incident.enriched
        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        try:
            event_id = uuid4()
            await producer.send_and_wait(
                topic,
                key=str(incident_id).encode(),
                value=json.dumps(
                    {
                        "event_id": str(event_id),
                        "event": "incident.enriched",
                        "incident_id": str(incident_id),
                        "fingerprint": "fp-diag-e2e",
                        "source": "generic",
                        "ts": datetime.now(UTC).isoformat(),
                    }
                ).encode(),
            )
        finally:
            await producer.stop()

        runner = asyncio.create_task(diagnoser.run())
        try:
            # Poll for the diagnosis row (max ~12 s).
            diag_id = None
            for _ in range(120):
                async with sf() as session:
                    row = (
                        (
                            await session.execute(
                                select(DiagnosisModel).where(
                                    DiagnosisModel.incident_id == incident_id
                                )
                            )
                        )
                        .scalars()
                        .first()
                    )
                if row is not None:
                    diag_id = row.id
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("diagnosis row was never written within 12 s")

            assert diag_id is not None

            # Verify the outbox event for incident.diagnosed was written atomically.
            async with sf() as session:
                rows = (
                    (
                        await session.execute(
                            select(OutboxEventModel).where(OutboxEventModel.topic == topic)
                        )
                    )
                    .scalars()
                    .all()
                )

            diagnosed = [r for r in rows if r.payload.get("event") == "incident.diagnosed"]
            assert len(diagnosed) == 1, (
                f"expected 1 incident.diagnosed outbox event, got {len(diagnosed)}; "
                f"all events: {[r.payload.get('event') for r in rows]}"
            )
            assert diagnosed[0].payload["diagnosis_id"] == str(diag_id)
            assert diagnosed[0].payload["incident_id"] == str(incident_id)
        finally:
            diagnoser.stop()
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
            await kafka_consumer.stop()
    finally:
        await engine.dispose()
