# tests/unit/diagnosis/test_truncation.py
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sentinel.diagnosis.truncation import truncate_for_budget

from tests.unit.diagnosis.fakes import (
    make_context,
    make_deploy,
    make_log,
    make_runbook,
    make_similar,
)


def test_under_budget_passes_through() -> None:
    ctx = make_context(deploys=[make_deploy()], logs=[make_log(0)])
    out, stats = truncate_for_budget(ctx, max_input_tokens=100_000)
    assert out == ctx
    assert stats.total_dropped() == 0


def test_over_budget_drops_logs() -> None:
    ctx = make_context(
        deploys=[make_deploy()],
        logs=[make_log(i) for i in range(100)],
        runbooks=[make_runbook()],
        similar=[make_similar()],
    )
    out, stats = truncate_for_budget(ctx, max_input_tokens=500)
    assert stats.dropped.get("recent_logs", 0) > 0
    # At least one log preserved
    assert len(out.recent_logs.data) >= 1


def test_preserves_at_least_one_per_nonempty_section() -> None:
    ctx = make_context(
        deploys=[make_deploy("d1"), make_deploy("d2"), make_deploy("d3")],
        logs=[make_log(i) for i in range(50)],
        runbooks=[make_runbook(), make_runbook()],
        similar=[make_similar(), make_similar()],
    )
    out, _ = truncate_for_budget(ctx, max_input_tokens=200)
    for section in (out.recent_deploys, out.recent_logs, out.runbooks, out.similar_incidents):
        if section.data:
            assert len(section.data) >= 1


def test_deploys_section_always_keeps_newest() -> None:
    now = datetime.now(UTC)
    older = make_deploy("old").model_copy(update={"deployed_at": now - timedelta(hours=1)})
    newer = make_deploy("new").model_copy(update={"deployed_at": now})
    ctx = make_context(
        deploys=[older, newer],
        logs=[make_log(i) for i in range(500)],
    )
    out, _ = truncate_for_budget(ctx, max_input_tokens=200)
    deploy_ids = [d.id for d in out.recent_deploys.data]
    assert "deploy:new" in deploy_ids


def test_whole_blob_serialized_at_most_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Truncation must not re-serialize the entire context once per dropped item
    (issue #70 — that was O(n^2)). Assert the full blob is dumped at most once,
    regardless of how many items get dropped. Item-sized dumps are fine."""
    ctx = make_context(
        deploys=[make_deploy()],
        logs=[make_log(i) for i in range(120)],
        runbooks=[make_runbook(), make_runbook()],
        similar=[make_similar(), make_similar()],
    )
    real_dumps = json.dumps
    blob_dumps = 0

    def counting_dumps(obj: object) -> str:
        nonlocal blob_dumps
        # The whole-context blob is the dict carrying the section keys. Require two
        # distinct sections so a future rename of one can't silently turn this
        # discriminator into a no-op (which would pass <= 1 as a false green).
        if isinstance(obj, dict) and "recent_logs" in obj and "recent_deploys" in obj:
            blob_dumps += 1
        return real_dumps(obj)

    # truncate_for_budget calls json.dumps with a single positional arg, so a
    # one-arg wrapper covers every call made during the patched window.
    monkeypatch.setattr(json, "dumps", counting_dumps)
    out, stats = truncate_for_budget(ctx, max_input_tokens=500)

    assert stats.dropped.get("recent_logs", 0) > 50  # genuinely dropped many
    assert len(out.recent_logs.data) >= 1
    assert blob_dumps <= 1, f"whole blob serialized {blob_dumps}x (expected <=1)"


def test_stats_to_dict_carries_per_section_counts() -> None:
    ctx = make_context(
        logs=[make_log(i) for i in range(100)],
        deploys=[make_deploy()],
    )
    _, stats = truncate_for_budget(ctx, max_input_tokens=200)
    as_dict = stats.to_dict()
    assert as_dict.get("recent_logs", 0) == stats.dropped.get("recent_logs", 0)
