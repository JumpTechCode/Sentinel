"""Eval harness — corpus-driven scoring of diagnosis quality."""

from sentinel.evals.schema import (
    GroundTruth,
    MetricSet,
    RegressionResult,
    RegressionVerdict,
)
from sentinel.evals.scoring import (
    score_action_coverage,
    score_category,
    score_evidence_quality,
    score_hypothesis,
)
from sentinel.evals.stats import bootstrap_ci, regression_for_metric

__all__ = [
    "GroundTruth",
    "MetricSet",
    "RegressionResult",
    "RegressionVerdict",
    "bootstrap_ci",
    "regression_for_metric",
    "score_action_coverage",
    "score_category",
    "score_evidence_quality",
    "score_hypothesis",
]
