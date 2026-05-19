# tests/unit/schemas/test_context.py
"""FetcherResult is generic per the spec; IncidentContext groups six sections."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


def _now() -> datetime:
    return datetime(2026, 5, 18, 18, 0, tzinfo=UTC)


def test_ok_result_requires_data() -> None:
    r = FetcherResult[DeployItem](
        status="ok",
        data=[
            DeployItem(
                id="deploy:abc",
                service="checkout-api",
                sha="abc",
                pr_title="x",
                deployed_at=_now(),
            )
        ],
        fetched_at=_now(),
    )
    assert r.status == "ok"
    assert r.error is None


def test_failed_result_requires_error_message() -> None:
    with pytest.raises(ValidationError, match="error"):
        FetcherResult[DeployItem](status="failed", data=[], fetched_at=_now())


def test_empty_context_is_valid() -> None:
    ctx = IncidentContext(
        incident_id=uuid4(),
        assembled_at=_now(),
        recent_deploys=FetcherResult[DeployItem](status="ok", data=[], fetched_at=_now()),
        related_alerts=FetcherResult[RelatedAlertItem](status="ok", data=[], fetched_at=_now()),
        similar_incidents=FetcherResult[SimilarIncidentItem](
            status="ok", data=[], fetched_at=_now()
        ),
        runbooks=FetcherResult[RunbookItem](status="ok", data=[], fetched_at=_now()),
        recent_logs=FetcherResult[LogLine](status="ok", data=[], fetched_at=_now()),
        active_alerts=FetcherResult[RelatedAlertItem](status="ok", data=[], fetched_at=_now()),
    )
    assert ctx.recent_deploys.status == "ok"


def test_degraded_result_with_data_no_error() -> None:
    """degraded does not require an error message — partial data is acceptable."""
    r = FetcherResult[DeployItem](
        status="degraded",
        data=[
            DeployItem(
                id="deploy:abc",
                service="checkout-api",
                sha="abc",
                pr_title="partial rollout",
                deployed_at=_now(),
            )
        ],
        fetched_at=_now(),
    )
    assert r.status == "degraded"
    assert r.error is None
    assert len(r.data) == 1


def test_degraded_result_with_error_message() -> None:
    """degraded may carry a partial-failure description alongside data."""
    r = FetcherResult[DeployItem](
        status="degraded",
        data=[],
        error="partial - some IDs timed out",
        fetched_at=_now(),
    )
    assert r.status == "degraded"
    assert r.error == "partial - some IDs timed out"


def test_context_ids_use_stable_prefix() -> None:
    deploy = DeployItem(
        id="deploy:abc",
        service="x",
        sha="abc",
        pr_title="t",
        deployed_at=_now(),
    )
    assert deploy.id.startswith("deploy:")
