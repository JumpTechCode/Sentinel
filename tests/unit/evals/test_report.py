"""Tests for eval report generation — JSON + Markdown writer."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

from sentinel.evals.report import write_report
from sentinel.evals.runner import RunResult
from sentinel.evals.schema import MetricSet


class TestReportWriting:
    """Tests for write_report() file creation and format."""

    def test_writes_both_files(self, tmp_path: Path) -> None:
        """write_report creates exactly two files matching <run_id>.{json,md}."""
        run_id = uuid4()
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics={},
            aggregate_metrics=MetricSet(
                category_match=0.9,
                hypothesis_cosine=0.78,
                action_coverage=0.71,
                evidence_quality=0.94,
            ),
        )

        json_path, md_path = write_report(run_result=run_result, output_dir=tmp_path)

        # Verify file names match run_id
        assert json_path.name == f"{run_id}.json"
        assert md_path.name == f"{run_id}.md"

        # Verify files exist
        assert json_path.exists()
        assert md_path.exists()

        # Verify they're in the output directory
        assert json_path.parent == tmp_path
        assert md_path.parent == tmp_path

    def test_json_round_trips_run_result(self, tmp_path: Path) -> None:
        """Parse the JSON back; all metrics and per-case match input RunResult."""
        run_id = uuid4()
        per_case_metrics = {
            "cloudflare-bgp": MetricSet(
                category_match=1.0,
                hypothesis_cosine=0.85,
                action_coverage=0.72,
                evidence_quality=1.0,
            ),
            "github-incident": MetricSet(
                category_match=0.8,
                hypothesis_cosine=0.70,
                action_coverage=0.60,
                evidence_quality=0.9,
            ),
        }
        aggregate = MetricSet(
            category_match=0.9,
            hypothesis_cosine=0.775,
            action_coverage=0.66,
            evidence_quality=0.95,
        )
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics=per_case_metrics,
            aggregate_metrics=aggregate,
            stability={
                "cloudflare-bgp": {
                    "category_match": 0.0,
                    "hypothesis_cosine": 0.04,
                    "action_coverage": 0.08,
                    "evidence_quality": 0.0,
                },
                "github-incident": {
                    "category_match": 0.1,
                    "hypothesis_cosine": 0.05,
                    "action_coverage": 0.02,
                    "evidence_quality": 0.01,
                },
            },
        )

        json_path, _ = write_report(run_result=run_result, output_dir=tmp_path)

        # Parse JSON back
        with open(json_path) as f:
            parsed = json.load(f)

        # Verify run_id
        assert UUID(parsed["run_id"]) == run_id

        # Verify aggregate metrics
        assert parsed["aggregate_metrics"]["category_match"] == 0.9
        assert parsed["aggregate_metrics"]["hypothesis_cosine"] == 0.775
        assert parsed["aggregate_metrics"]["action_coverage"] == 0.66
        assert parsed["aggregate_metrics"]["evidence_quality"] == 0.95

        # Verify per-case entries
        per_case_dict = {pc["case_id"]: pc for pc in parsed["per_case"]}
        assert "cloudflare-bgp" in per_case_dict
        assert "github-incident" in per_case_dict

        bgp = per_case_dict["cloudflare-bgp"]
        assert bgp["metrics"]["category_match"] == 1.0
        assert bgp["metrics"]["hypothesis_cosine"] == 0.85
        assert bgp["stability"]["category_match"] == 0.0

    def test_markdown_includes_aggregate_table(self, tmp_path: Path) -> None:
        """Markdown includes an aggregate metrics table with expected header."""
        run_id = uuid4()
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics={},
            aggregate_metrics=MetricSet(
                category_match=0.90,
                hypothesis_cosine=0.78,
                action_coverage=0.71,
                evidence_quality=0.94,
            ),
        )

        _, md_path = write_report(run_result=run_result, output_dir=tmp_path)

        md_text = md_path.read_text()

        # Check for aggregate table markers
        assert "| Metric | Value |" in md_text
        assert "category_match" in md_text
        assert "hypothesis_cosine" in md_text
        assert "action_coverage" in md_text
        assert "evidence_quality" in md_text

    def test_markdown_includes_per_case_table(self, tmp_path: Path) -> None:
        """Markdown includes per-case table with case_id."""
        run_id = uuid4()
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics={
                "cloudflare-bgp": MetricSet(
                    category_match=1.0,
                    hypothesis_cosine=0.85,
                    action_coverage=0.72,
                    evidence_quality=1.0,
                ),
                "github-incident": MetricSet(
                    category_match=0.8,
                    hypothesis_cosine=0.70,
                    action_coverage=0.60,
                    evidence_quality=0.9,
                ),
            },
            aggregate_metrics=MetricSet(
                category_match=0.9,
                hypothesis_cosine=0.775,
                action_coverage=0.66,
                evidence_quality=0.95,
            ),
            stability={
                "cloudflare-bgp": {
                    "category_match": 0.0,
                    "hypothesis_cosine": 0.04,
                    "action_coverage": 0.08,
                    "evidence_quality": 0.0,
                },
                "github-incident": {
                    "category_match": 0.1,
                    "hypothesis_cosine": 0.05,
                    "action_coverage": 0.02,
                    "evidence_quality": 0.01,
                },
            },
        )

        _, md_path = write_report(run_result=run_result, output_dir=tmp_path)

        md_text = md_path.read_text()

        # Check for per-case table header
        assert "cloudflare-bgp" in md_text
        assert "github-incident" in md_text

    def test_handles_zero_cases_gracefully(self, tmp_path: Path) -> None:
        """Empty per_case_metrics doesn't crash; produces valid JSON/MD."""
        run_id = uuid4()
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics={},
            aggregate_metrics=MetricSet(
                category_match=0.0,
                hypothesis_cosine=0.0,
                action_coverage=0.0,
                evidence_quality=None,
            ),
        )

        json_path, md_path = write_report(run_result=run_result, output_dir=tmp_path)

        # JSON parses without error
        with open(json_path) as f:
            parsed = json.load(f)
        assert parsed["per_case"] == []

        # Markdown reads without error
        md_text = md_path.read_text()
        assert "Sentinel Eval Results" in md_text

    def test_headline_numbers_computed_correctly(self, tmp_path: Path) -> None:
        """Verify pass_rate_strict and mean_stability computation."""
        # Test case:
        # - 3 cases total
        # - case 1: all thresholds met → contributes 1.0 to pass_rate
        # - case 2: hypothesis < 0.7 → contributes 0.0 to pass_rate
        # - case 3: all thresholds met → contributes 1.0 to pass_rate
        # - Expected pass_rate_strict = 2/3 ≈ 0.667
        #
        # Mean stability:
        # - Sum all stability values across all cases x metrics
        # - Divide by (num_cases x num_metrics)
        run_id = uuid4()
        run_result = RunResult(
            run_id=run_id,
            per_case_metrics={
                "case-1": MetricSet(
                    category_match=1.0,
                    hypothesis_cosine=0.75,
                    action_coverage=0.6,
                    evidence_quality=0.8,
                ),
                "case-2": MetricSet(
                    category_match=1.0,
                    hypothesis_cosine=0.65,  # fails hypothesis threshold (< 0.7)
                    action_coverage=0.6,
                    evidence_quality=0.8,
                ),
                "case-3": MetricSet(
                    category_match=1.0,
                    hypothesis_cosine=0.75,
                    action_coverage=0.7,
                    evidence_quality=0.9,
                ),
            },
            aggregate_metrics=MetricSet(
                category_match=1.0,
                hypothesis_cosine=0.717,
                action_coverage=0.633,
                evidence_quality=0.833,
            ),
            stability={
                "case-1": {
                    "category_match": 0.0,
                    "hypothesis_cosine": 0.02,
                    "action_coverage": 0.05,
                    "evidence_quality": 0.01,
                },
                "case-2": {
                    "category_match": 0.0,
                    "hypothesis_cosine": 0.03,
                    "action_coverage": 0.04,
                    "evidence_quality": 0.02,
                },
                "case-3": {
                    "category_match": 0.0,
                    "hypothesis_cosine": 0.02,
                    "action_coverage": 0.03,
                    "evidence_quality": 0.01,
                },
            },
        )

        json_path, _ = write_report(run_result=run_result, output_dir=tmp_path)

        with open(json_path) as f:
            parsed = json.load(f)

        headline = parsed["headline"]

        # pass_rate_strict: 2 out of 3 cases pass all thresholds
        # case-1: category=1.0, hypothesis=0.75, actions=0.6, evidence=0.8 → all pass ✓
        # case-2: category=1.0, hypothesis=0.65, actions=0.6, evidence=0.8 → hypothesis fails ✗
        # case-3: category=1.0, hypothesis=0.75, actions=0.7, evidence=0.9 → all pass ✓
        expected_pass_rate = 2 / 3
        assert abs(headline["pass_rate_strict"] - expected_pass_rate) < 0.01

        # mean_stability: mean of all stability values
        # Sum: 0+0.02+0.05+0.01 + 0+0.03+0.04+0.02 + 0+0.02+0.03+0.01 = 0.23
        # Count: 3 cases x 4 metrics = 12
        # Mean: 0.23 / 12 ≈ 0.0192
        expected_mean_stability = 0.23 / 12
        assert abs(headline["mean_stability"] - expected_mean_stability) < 0.001
