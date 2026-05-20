"""JSON + Markdown report writer for eval run results.

Transforms a RunResult into two files:
  * ``<run_id>.json`` — machine-readable metrics blob (persisted to eval_runs.metrics JSONB)
  * ``<run_id>.md`` — operator-readable summary for README/GitHub comments

The JSON shape is stable (same blob gets stored in the DB); the Markdown format
is optimized for human consumption.
"""

from __future__ import annotations

import json
from pathlib import Path

from sentinel.evals.runner import RunResult
from sentinel.evals.schema import MetricSet


def write_report(
    *,
    run_result: RunResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write JSON and Markdown report files for a RunResult.

    Args:
        run_result: The aggregated result from run_corpus()
        output_dir: Directory to write files to (created if needed)

    Returns:
        (json_path, md_path) — both files named <run_id>.{json,md}
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / f"{run_result.run_id}.json"
    md_path = output_dir / f"{run_result.run_id}.md"

    # Write JSON
    json_blob = _run_result_to_json_blob(run_result)
    with open(json_path, "w") as f:
        json.dump(json_blob, f, indent=2)

    # Write Markdown
    md_text = _run_result_to_markdown(run_result)
    md_path.write_text(md_text)

    return json_path, md_path


# --- JSON shape ----------------------------------------------------------- #


def _run_result_to_json_blob(run_result: RunResult) -> dict[str, object]:
    """Convert RunResult to the JSON shape that goes in eval_runs.metrics JSONB.

    Shape:
    {
      "run_id": "...",
      "aggregate_metrics": {
        "category_match": 0.9,
        "hypothesis_cosine": 0.78,
        ...
      },
      "headline": {
        "pass_rate_strict": 0.7,
        "mean_stability": 0.08
      },
      "per_case": [
        {
          "case_id": "cloudflare-bgp",
          "metrics": {...},
          "stability": {...}
        }
      ]
    }
    """
    headline = _compute_headline(run_result)

    per_case_list = []
    for case_id in sorted(run_result.per_case_metrics.keys()):
        metrics = run_result.per_case_metrics[case_id]
        stability = run_result.stability.get(case_id, {})
        per_case_list.append(
            {
                "case_id": case_id,
                "metrics": _metric_set_to_dict(metrics),
                "stability": stability,
            }
        )

    return {
        "run_id": str(run_result.run_id),
        "aggregate_metrics": _metric_set_to_dict(run_result.aggregate_metrics),
        "headline": headline,
        "per_case": per_case_list,
    }


def _metric_set_to_dict(m: MetricSet) -> dict[str, float | None]:
    """Convert MetricSet to a flat dict."""
    return {
        "category_match": m.category_match,
        "hypothesis_cosine": m.hypothesis_cosine,
        "action_coverage": m.action_coverage,
        "evidence_quality": m.evidence_quality,
    }


def _compute_headline(run_result: RunResult) -> dict[str, float]:
    """Compute headline metrics from per-case results.

    pass_rate_strict: fraction of cases where ALL 4 metrics meet thresholds:
      - category_match = 1.0
      - hypothesis_cosine >= 0.7
      - action_coverage >= 0.6
      - evidence_quality >= 0.8 (or None counts as failed)

    mean_stability: mean of all stability values across all cases x metrics.
    """
    if not run_result.per_case_metrics:
        return {
            "pass_rate_strict": 0.0,
            "mean_stability": 0.0,
        }

    # Count cases that pass all thresholds
    passes = sum(
        1 for metrics in run_result.per_case_metrics.values() if _case_passes_strict(metrics)
    )
    pass_rate = passes / len(run_result.per_case_metrics)

    # Compute mean stability across all cases x metrics
    all_stability_values: list[float] = []
    for case_id in run_result.per_case_metrics:
        stability = run_result.stability.get(case_id, {})
        all_stability_values.extend(stability.values())

    mean_stability = (
        sum(all_stability_values) / len(all_stability_values) if all_stability_values else 0.0
    )

    return {
        "pass_rate_strict": pass_rate,
        "mean_stability": mean_stability,
    }


def _case_passes_strict(metrics: MetricSet) -> bool:
    """Check if all 4 metrics meet the strict thresholds."""
    return (
        metrics.category_match >= 1.0
        and metrics.hypothesis_cosine >= 0.7
        and metrics.action_coverage >= 0.6
        and metrics.evidence_quality is not None
        and metrics.evidence_quality >= 0.8
    )


# --- Markdown shape -------------------------------------------------------- #


def _run_result_to_markdown(run_result: RunResult) -> str:
    """Convert RunResult to operator-readable Markdown summary."""
    lines = []

    # Header
    lines.append(f"# Sentinel Eval Results — {run_result.run_id}")
    lines.append("")

    # Basic info
    num_cases = len(run_result.per_case_metrics)
    lines.append(f"**Cases:** {num_cases}")
    lines.append("")

    # Aggregate metrics table
    lines.append("## Aggregate Metrics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| category_match | {run_result.aggregate_metrics.category_match:.2f} |")
    lines.append(f"| hypothesis_cosine | {run_result.aggregate_metrics.hypothesis_cosine:.2f} |")
    lines.append(f"| action_coverage | {run_result.aggregate_metrics.action_coverage:.2f} |")
    ev = run_result.aggregate_metrics.evidence_quality
    evidence_str = f"{ev:.2f}" if ev is not None else "—"
    lines.append(f"| evidence_quality | {evidence_str} |")
    lines.append("")

    # Per-case table
    if num_cases > 0:
        lines.append("## Per-case")
        lines.append("")
        lines.append("| Case | category | hypothesis | actions | evidence | stability |")
        lines.append("|---|---|---|---|---|---|")

        for case_id in sorted(run_result.per_case_metrics.keys()):
            metrics = run_result.per_case_metrics[case_id]
            stability = run_result.stability.get(case_id, {})

            # Compute mean stability for this case (avg across 4 metrics)
            case_stability_vals = list(stability.values())
            case_stability_mean = (
                sum(case_stability_vals) / len(case_stability_vals) if case_stability_vals else 0.0
            )

            cat = f"{metrics.category_match:.2f}"
            hyp = f"{metrics.hypothesis_cosine:.2f}"
            act = f"{metrics.action_coverage:.2f}"
            ev_val = (
                f"{metrics.evidence_quality:.2f}" if metrics.evidence_quality is not None else "—"
            )
            stab = f"{case_stability_mean:.2f}"

            lines.append(f"| {case_id} | {cat} | {hyp} | {act} | {ev_val} | {stab} |")
        lines.append("")

    # Headline numbers
    headline = _compute_headline(run_result)
    lines.append("## Headline")
    lines.append("")
    pass_rate_pct = headline["pass_rate_strict"] * 100
    lines.append(f"- pass_rate_strict: {pass_rate_pct:.1f}%")
    lines.append(f"- mean_stability: {headline['mean_stability']:.3f}")
    lines.append("")

    return "\n".join(lines)


__all__ = ["write_report"]
