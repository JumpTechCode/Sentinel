"""Pydantic schemas for the eval harness scoring path.

This module contains only the types the pure-function scoring + stats layer
needs. CorpusCase / RunMetadata / per-shot result types live in a separate
schema module landing with PR 3 (the runner).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sentinel.schemas.enums import CategoryType


class GroundTruth(BaseModel):
    """Curator-supplied truth for one postmortem case.

    `category` is the single primary label; `acceptable_categories` is the
    defensible set (so e.g. a config bug shipped via deploy can score either
    'config' or 'deploy' without a penalty).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    category: CategoryType
    acceptable_categories: list[CategoryType] = Field(min_length=1)
    root_cause: str = Field(min_length=1)
    # Empty correct_actions is allowed — scoring returns 1.0 for action_coverage
    # in that case (no actions to be covered = vacuously perfect).
    correct_actions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _category_in_acceptable(self) -> GroundTruth:
        if self.category not in self.acceptable_categories:
            raise ValueError(
                f"category={self.category!r} must appear in acceptable_categories="
                f"{self.acceptable_categories!r}"
            )
        return self


class MetricSet(BaseModel):
    """Four eval metrics, all in [0, 1]. evidence_quality is None when the
    diagnosis could not be produced or parsed (case_status=schema_failed); the
    other three are 0 in that case."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    category_match: float = Field(ge=0.0, le=1.0)
    hypothesis_cosine: float = Field(ge=0.0, le=1.0)
    action_coverage: float = Field(ge=0.0, le=1.0)
    evidence_quality: float | None = Field(default=None, ge=0.0, le=1.0)


class RegressionResult(BaseModel):
    """One metric's regression verdict from the paired-bootstrap gate.

    is_regression = True iff:
      - CI excludes zero (statistical signal that the new run is worse), AND
      - mean(per-case diff) is worse than the practical floor (default 5%)

    The "worse direction" is metric-dependent: most metrics are better-when-
    higher, so worse = mean_diff < -practical_floor. The gate caller flips the
    sign for hallucinated_evidence_rate (lower is better).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    metric_name: str = Field(min_length=1)
    mean_diff: float
    ci_low: float
    ci_high: float
    is_regression: bool
    reason: str = Field(min_length=1)


class RegressionVerdict(BaseModel):
    """Aggregate of per-metric regression results — what the gate returns."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    per_metric: list[RegressionResult]

    @property
    def has_regression(self) -> bool:
        return any(r.is_regression for r in self.per_metric)

    @property
    def regressed_metrics(self) -> list[str]:
        return [r.metric_name for r in self.per_metric if r.is_regression]
