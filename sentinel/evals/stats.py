"""Statistical helpers for the eval regression gate.

Paired-difference bootstrap rather than naive flat-threshold: with n=10 cases
x 4 metrics, a flat ">5% drop" gate produces constant false positives from
LLM noise alone. See spec §5 + Cameron Wolfe's stats-for-LLM-evals analysis
referenced in plans/2026-05-20-eval-harness-design.md.

Pure NumPy, sync. No DB, no LLM, no async — just math.
"""

from __future__ import annotations

import numpy as np

from sentinel.evals.schema import RegressionResult


def bootstrap_ci(
    values: list[float],
    *,
    n_resamples: int,
    seed: int,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Percentile bootstrap CI for the mean of `values`.

    Args:
        values: per-case sample (e.g. per-case difference scores)
        n_resamples: number of bootstrap resamples (industry default: 10k+)
        seed: deterministic RNG seed (eval gate must be reproducible)
        alpha: significance level (0.05 → 95% CI)

    Returns:
        (ci_low, ci_high) such that the true mean is in the interval with
        100*(1-alpha)% confidence under the bootstrap assumptions.
    """
    if not values:
        raise ValueError("bootstrap_ci called with empty values")

    rng = np.random.default_rng(seed)
    arr = np.asarray(values, dtype=np.float64)
    # Resample with replacement n_resamples times; each resample has the same
    # length as the original sample. Take the mean of each resample.
    resample_means = rng.choice(arr, size=(n_resamples, arr.size), replace=True).mean(axis=1)
    lo = float(np.quantile(resample_means, alpha / 2))
    hi = float(np.quantile(resample_means, 1 - alpha / 2))
    return lo, hi


def regression_for_metric(
    *,
    metric_name: str,
    current_per_case: list[float],
    baseline_per_case: list[float],
    n_resamples: int = 10_000,
    seed: int,
    practical_floor: float = 0.05,
    higher_is_better: bool = True,
) -> RegressionResult:
    """Paired-bootstrap regression gate for one metric.

    Computes per-case difference d_i = current[i] - baseline[i], bootstraps a
    95% CI on the mean difference, and returns is_regression=True iff BOTH:
      - CI excludes zero in the "worse" direction (statistical signal), AND
      - mean(d_i) is worse than the practical_floor (default 5%)

    For higher-is-better metrics: "worse" = mean_d < -practical_floor AND ci_high < 0.
    For lower-is-better (e.g. hallucinated_evidence_rate): the sign flips.

    Both conditions are required so that:
      - random noise with no real drift doesn't trip the gate (CI requirement)
      - statistically significant but practically tiny drops don't either
        (floor requirement)
    """
    if len(current_per_case) != len(baseline_per_case):
        raise ValueError(
            f"length mismatch: current={len(current_per_case)} vs "
            f"baseline={len(baseline_per_case)}"
        )

    diffs = [c - b for c, b in zip(current_per_case, baseline_per_case, strict=True)]
    mean_d = float(sum(diffs) / len(diffs))
    ci_low, ci_high = bootstrap_ci(diffs, n_resamples=n_resamples, seed=seed)

    if higher_is_better:
        ci_excludes_zero_worse = ci_high < 0  # entirely below zero = drop
        beyond_floor = mean_d < -practical_floor
    else:
        ci_excludes_zero_worse = ci_low > 0  # entirely above zero = rise
        beyond_floor = mean_d > practical_floor

    is_regression = ci_excludes_zero_worse and beyond_floor

    if is_regression:
        direction = "drop" if higher_is_better else "rise"
        reason = (
            f"95% CI excludes zero in the {direction} direction "
            f"(CI=[{ci_low:.4f}, {ci_high:.4f}]) and mean_diff={mean_d:.4f} "
            f"exceeds practical_floor={practical_floor}"
        )
    elif not ci_excludes_zero_worse and not beyond_floor:
        reason = (
            f"within noise: CI=[{ci_low:.4f}, {ci_high:.4f}] brackets zero "
            f"and mean_diff={mean_d:.4f} is within practical_floor={practical_floor}"
        )
    elif not ci_excludes_zero_worse:
        reason = (
            f"CI=[{ci_low:.4f}, {ci_high:.4f}] does not exclude zero — drift "
            f"indistinguishable from noise"
        )
    else:
        reason = (
            f"mean_diff={mean_d:.4f} is within practical_floor={practical_floor} — "
            f"statistically significant but practically immaterial"
        )

    return RegressionResult(
        metric_name=metric_name,
        mean_diff=mean_d,
        ci_low=ci_low,
        ci_high=ci_high,
        is_regression=is_regression,
        reason=reason,
    )
