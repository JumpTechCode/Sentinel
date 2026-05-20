"""Unit tests for sentinel.evals.schema (GroundTruth, MetricSet, RegressionResult)."""

from __future__ import annotations

from typing import Any, cast

import pytest


def test_ground_truth_requires_category_in_acceptable() -> None:
    """The primary `category` must appear in `acceptable_categories`."""
    from sentinel.evals.schema import GroundTruth

    # Happy path
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config", "deploy"],
        root_cause="BGP misconfiguration",
        correct_actions=["Revert the change"],
    )
    assert gt.category == "config"

    # Primary category missing from acceptable set should raise
    with pytest.raises(ValueError, match="acceptable_categories"):
        GroundTruth(
            category="config",
            acceptable_categories=["deploy"],
            root_cause="x",
            correct_actions=["y"],
        )


def test_ground_truth_is_frozen() -> None:
    from sentinel.evals.schema import GroundTruth

    gt = GroundTruth(
        category="deploy",
        acceptable_categories=["deploy"],
        root_cause="x",
        correct_actions=["y"],
    )
    with pytest.raises(ValueError):  # pydantic frozen → ValidationError (subclass of ValueError)
        gt.category = "config"  # type: ignore[misc]


def test_ground_truth_rejects_empty_root_cause_or_actions() -> None:
    from sentinel.evals.schema import GroundTruth

    with pytest.raises(ValueError):
        GroundTruth(
            category="deploy",
            acceptable_categories=["deploy"],
            root_cause="",  # empty
            correct_actions=["y"],
        )
    # Empty correct_actions IS allowed (some incidents have no obvious remediation);
    # scoring handles it (returns 1.0 by spec).
    gt = GroundTruth(
        category="deploy",
        acceptable_categories=["deploy"],
        root_cause="x",
        correct_actions=[],
    )
    assert gt.correct_actions == []


def test_ground_truth_rejects_unknown_category() -> None:
    from sentinel.evals.schema import GroundTruth

    # We intentionally pass invalid CategoryType values to test validation.
    # This raises ValidationError (subclass of ValueError) as expected.
    with pytest.raises(ValueError):
        GroundTruth(
            category=cast(Any, "nonexistent"),
            acceptable_categories=cast(Any, ["nonexistent"]),
            root_cause="x",
            correct_actions=["y"],
        )


def test_metric_set_clamps_to_unit_interval() -> None:
    """All metrics in [0, 1]."""
    from sentinel.evals.schema import MetricSet

    ms = MetricSet(
        category_match=1.0,
        hypothesis_cosine=0.78,
        action_coverage=0.71,
        evidence_quality=0.94,
    )
    assert ms.category_match == 1.0

    with pytest.raises(ValueError):
        MetricSet(
            category_match=1.5,  # out of range
            hypothesis_cosine=0.5,
            action_coverage=0.5,
            evidence_quality=0.5,
        )


def test_metric_set_evidence_quality_can_be_none() -> None:
    """evidence_quality=None when no diagnosis was produced (schema_failed case).
    The other three metrics are 0 in that case; evidence_quality is N/A.
    """
    from sentinel.evals.schema import MetricSet

    ms = MetricSet(
        category_match=0.0,
        hypothesis_cosine=0.0,
        action_coverage=0.0,
        evidence_quality=None,
    )
    assert ms.evidence_quality is None


def test_regression_result_carries_ci_and_verdict() -> None:
    from sentinel.evals.schema import RegressionResult

    r = RegressionResult(
        metric_name="hypothesis_cosine",
        mean_diff=-0.08,
        ci_low=-0.12,
        ci_high=-0.02,
        is_regression=True,
        reason="CI excludes zero and mean drop exceeds 5% floor",
    )
    assert r.is_regression is True
    assert r.metric_name == "hypothesis_cosine"


def test_regression_verdict_aggregates_per_metric_results() -> None:
    from sentinel.evals.schema import RegressionResult, RegressionVerdict

    r1 = RegressionResult(
        metric_name="category_match",
        mean_diff=0.0,
        ci_low=-0.02,
        ci_high=0.02,
        is_regression=False,
        reason="within noise",
    )
    r2 = RegressionResult(
        metric_name="hypothesis_cosine",
        mean_diff=-0.08,
        ci_low=-0.12,
        ci_high=-0.02,
        is_regression=True,
        reason="CI excludes zero and mean drop exceeds 5% floor",
    )
    verdict = RegressionVerdict(per_metric=[r1, r2])
    assert verdict.has_regression is True
    assert verdict.regressed_metrics == ["hypothesis_cosine"]
    # All-clear verdict
    verdict_clean = RegressionVerdict(per_metric=[r1])
    assert verdict_clean.has_regression is False
    assert verdict_clean.regressed_metrics == []
