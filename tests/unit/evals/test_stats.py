"""Unit tests for sentinel.evals.stats (paired-bootstrap regression gate)."""

from __future__ import annotations

import pytest


def test_bootstrap_ci_returns_tuple_in_value_order() -> None:
    """ci_low <= mean <= ci_high; deterministic when seeded."""
    from sentinel.evals.stats import bootstrap_ci

    # 20 values clustered around 0.8 — CI should be tight around the mean
    values = [
        0.78,
        0.80,
        0.82,
        0.79,
        0.81,
        0.80,
        0.79,
        0.80,
        0.81,
        0.79,
        0.80,
        0.81,
        0.82,
        0.78,
        0.80,
        0.79,
        0.81,
        0.80,
        0.82,
        0.79,
    ]
    lo, hi = bootstrap_ci(values, n_resamples=2000, seed=42)
    assert lo < 0.80 < hi
    assert hi - lo < 0.05  # tight cluster → tight CI


def test_bootstrap_ci_is_seeded_deterministic() -> None:
    from sentinel.evals.stats import bootstrap_ci

    values = [0.5, 0.6, 0.7, 0.8, 0.9, 0.4, 0.5, 0.6]
    lo1, hi1 = bootstrap_ci(values, n_resamples=1000, seed=7)
    lo2, hi2 = bootstrap_ci(values, n_resamples=1000, seed=7)
    assert lo1 == lo2 and hi1 == hi2


def test_bootstrap_ci_empty_raises() -> None:
    from sentinel.evals.stats import bootstrap_ci

    with pytest.raises(ValueError, match="empty"):
        bootstrap_ci([], n_resamples=1000, seed=1)


def test_regression_for_metric_clean_when_runs_match() -> None:
    """No drift → no regression, regardless of variance."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.8, 0.85, 0.78, 0.82, 0.79, 0.81, 0.80, 0.83, 0.77, 0.80]
    current = [0.8, 0.84, 0.78, 0.82, 0.79, 0.81, 0.80, 0.83, 0.77, 0.81]
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    assert r.is_regression is False
    assert r.metric_name == "hypothesis_cosine"


def test_regression_for_metric_detects_clear_drop() -> None:
    """A 15% drop across all cases triggers the gate."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.85] * 10
    current = [0.70] * 10  # -0.15 per case
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    assert r.is_regression is True
    assert r.mean_diff < -0.05


def test_regression_for_metric_ignores_tiny_drop_below_practical_floor() -> None:
    """A 2% drop is below the practical floor — not a regression even if CI
    excludes zero. This avoids 'statistically significant but practically
    meaningless' false alarms."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.85] * 30
    current = [0.83] * 30  # -0.02 per case, very tight CI excludes zero
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
        practical_floor=0.05,
    )
    assert r.is_regression is False


def test_regression_for_metric_ignores_noise_even_when_mean_drops_a_lot() -> None:
    """A 10% drop in mean but huge variance — CI includes zero → no regression
    (paired-bootstrap correctly attributes the drop to noise)."""
    from sentinel.evals.stats import regression_for_metric

    # baseline and current paired so per-case diffs are noisy around 0
    baseline = [0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1]
    current = [0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9]
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    # Mean diff is 0; CI brackets zero → not a regression
    assert r.is_regression is False


def test_regression_for_metric_inverted_metric_fires_on_increase() -> None:
    """For inverted metrics (lower-is-better like hallucinated_evidence_rate),
    a 15% INCREASE is a regression."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.05] * 10
    current = [0.20] * 10  # +0.15 per case
    r = regression_for_metric(
        metric_name="hallucinated_evidence_rate",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
        higher_is_better=False,
    )
    assert r.is_regression is True
    assert r.mean_diff > 0.05


def test_regression_for_metric_length_mismatch_raises() -> None:
    from sentinel.evals.stats import regression_for_metric

    with pytest.raises(ValueError, match="length"):
        regression_for_metric(
            metric_name="x",
            current_per_case=[0.1, 0.2, 0.3],
            baseline_per_case=[0.1, 0.2],
            n_resamples=100,
            seed=1,
        )
