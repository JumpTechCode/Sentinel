"""Eval harness — corpus-driven scoring of diagnosis quality."""

from sentinel.evals.cassette import (
    CassetteContext,
    CassetteMiss,
    CassetteTransport,
    compute_cassette_key,
)
from sentinel.evals.corpus_loader import (
    CorpusValidationError,
    load_case,
    load_corpus_dir,
)
from sentinel.evals.fetcher_override import (
    CorpusActiveAlertsFetcher,
    CorpusDeploysFetcher,
    CorpusRecentLogsFetcher,
    CorpusRelatedAlertsFetcher,
    CorpusRunbooksFetcher,
    CorpusSimilarIncidentsFetcher,
    corpus_fetchers,
)
from sentinel.evals.registry import REGISTRY, ActiveCaseRegistry
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
    "REGISTRY",
    "ActiveCaseRegistry",
    "AlertSeed",
    "CassetteContext",
    "CassetteMiss",
    "CassetteTransport",
    "ContextSeed",
    "CorpusActiveAlertsFetcher",
    "CorpusCase",
    "CorpusDeploysFetcher",
    "CorpusRecentLogsFetcher",
    "CorpusRelatedAlertsFetcher",
    "CorpusRunbooksFetcher",
    "CorpusSimilarIncidentsFetcher",
    "CorpusValidationError",
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
    "compute_cassette_key",
    "corpus_fetchers",
    "load_case",
    "load_corpus_dir",
    "regression_for_metric",
    "score_action_coverage",
    "score_category",
    "score_evidence_quality",
    "score_hypothesis",
]
