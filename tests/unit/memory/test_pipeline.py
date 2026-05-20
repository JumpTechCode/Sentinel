# tests/unit/memory/test_pipeline.py
"""MemoryPipeline — text composition for embedding inputs."""

from __future__ import annotations

from uuid import uuid4

from sentinel.memory.pipeline import MemoryPipeline
from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData


def _row(*, service: str = "api", title: str = "5xx surge") -> MemoryIncidentRow:
    return MemoryIncidentRow(
        id=uuid4(),
        service=service,
        title=title,
        status="open",
        resolution=None,
    )


def test_compose_initial_is_service_then_title() -> None:
    pipeline = MemoryPipeline()
    row = _row(service="checkout", title="latency p99 above SLO")
    assert pipeline.compose_initial(row) == "checkout latency p99 above SLO"


def test_compose_resolved_joins_title_root_cause_remediation_with_newlines() -> None:
    pipeline = MemoryPipeline()
    row = _row(title="db deadlock")
    resolution = ResolutionData(
        root_cause="long transaction on users table during migration",
        remediation="killed migration; switched to online schema change",
        diagnosis_was_correct=True,
    )
    expected = (
        "db deadlock\n"
        "long transaction on users table during migration\n"
        "killed migration; switched to online schema change"
    )
    assert pipeline.compose_resolved(row, resolution) == expected


def test_compose_initial_strips_no_input() -> None:
    """We do not strip — empty fields produce visible empties (caller's responsibility)."""
    pipeline = MemoryPipeline()
    row = _row(service="", title="")
    assert pipeline.compose_initial(row) == " "
