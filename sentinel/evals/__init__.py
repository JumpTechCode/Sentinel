"""Eval harness — corpus-driven scoring of diagnosis quality."""

from sentinel.evals.schema import (
    AlertSeed,
    ContextSeed,
    CorpusCase,
    DeploySeed,
    GroundTruth,
    LogSeed,
    MetricSet,
    RegressionResult,
    RegressionVerdict,
    RelatedAlertSeed,
    RunbookSeed,
    SimilarIncidentSeed,
)
from sentinel.evals.scoring import (
    score_action_coverage,
    score_category,
    score_evidence_quality,
    score_hypothesis,
)
from sentinel.evals.stats import bootstrap_ci, regression_for_metric

__all__ = [
    "AlertSeed",
    "ContextSeed",
    "CorpusCase",
    "DeploySeed",
    "GroundTruth",
    "LogSeed",
    "MetricSet",
    "RegressionResult",
    "RegressionVerdict",
    "RelatedAlertSeed",
    "RunbookSeed",
    "SimilarIncidentSeed",
    "bootstrap_ci",
    "regression_for_metric",
    "score_action_coverage",
    "score_category",
    "score_evidence_quality",
    "score_hypothesis",
]
