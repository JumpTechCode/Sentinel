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

__all__ = [
    "GroundTruth",
    "MetricSet",
    "RegressionResult",
    "RegressionVerdict",
    "score_action_coverage",
    "score_category",
    "score_evidence_quality",
    "score_hypothesis",
]
