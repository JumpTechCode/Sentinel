# tests/unit/diagnosis/fakes.py
"""Test-only builders and fakes for the diagnosis module."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


def fetcher_ok(data: list[Any]) -> FetcherResult[Any]:
    return FetcherResult(status="ok", data=data, error=None, fetched_at=datetime.now(UTC))


def fetcher_degraded(reason: str = "not_configured") -> FetcherResult[Any]:
    return FetcherResult(status="degraded", data=[], error=reason, fetched_at=datetime.now(UTC))


def make_deploy(sha: str = "abc123", service: str = "payments-api") -> DeployItem:
    return DeployItem(
        id=f"deploy:{sha}",
        service=service,
        sha=sha,
        pr_number=4421,
        pr_title="Switch idempotency store to Redis Cluster",
        pr_diff_summary="src/idempotency/{store.py,redis_cluster.py}",
        deployed_at=datetime.now(UTC),
        deployed_by="alice",
    )


def make_log(idx: int) -> LogLine:
    return LogLine(
        id=f"log:{idx}",
        timestamp=datetime.now(UTC),
        level="error",
        service="payments-api",
        message=f"failed to acquire connection (attempt {idx})",
    )


def make_similar(uid: UUID | None = None, cosine: float = 0.8) -> SimilarIncidentItem:
    uid = uid or uuid4()
    return SimilarIncidentItem(
        id=f"similar:{uid}",
        title="5xx after Redis client upgrade",
        root_cause="connection pool exhausted on TLS rotation",
        remediation="downgrade redis-py to 5.0.x",
        cosine_similarity=cosine,
    )


def make_runbook(uid: UUID | None = None, cosine: float = 0.7) -> RunbookItem:
    uid = uid or uuid4()
    return RunbookItem(
        id=f"runbook:{uid}",
        title="Redis pool exhaustion",
        content="1. Check pool metrics. 2. Restart with larger pool. 3. Validate TLS rotation.",
        cosine_similarity=cosine,
    )


def make_related(uid: UUID | None = None) -> RelatedAlertItem:
    uid = uid or uuid4()
    return RelatedAlertItem(
        id=f"related:{uid}",
        service="payments-api",
        severity="SEV2",
        title="elevated latency",
        opened_at=datetime.now(UTC),
    )


def make_context(
    *,
    deploys: list[DeployItem] | None = None,
    similar: list[SimilarIncidentItem] | None = None,
    runbooks: list[RunbookItem] | None = None,
    logs: list[LogLine] | None = None,
    related: list[RelatedAlertItem] | None = None,
    active: list[RelatedAlertItem] | None = None,
    incident_id: UUID | None = None,
) -> IncidentContext:
    return IncidentContext(
        incident_id=incident_id or uuid4(),
        assembled_at=datetime.now(UTC),
        recent_deploys=fetcher_ok(deploys or []),
        related_alerts=fetcher_ok(related or []),
        similar_incidents=fetcher_ok(similar or []),
        runbooks=fetcher_ok(runbooks or []),
        recent_logs=fetcher_ok(logs or []),
        active_alerts=fetcher_ok(active or []),
    )
