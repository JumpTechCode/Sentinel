"""End-to-end integration test for the eval runner.

DEFERRED to PR 3c — see plan plans/2026-05-20-eval-harness-pr3b-runner-plan.md
Task 7 and the broader Work Area K design at plans/2026-05-20-eval-harness-design.md.

Why this test is skipped on PR 3b:

1. PR 3b uses `_StubShotPersister` (in-memory list) for eval-result persistence
   because `PostgresEvalRunRepository` from PR 1 (#42) is on a parallel branch
   not yet in this chain. A real e2e test that asserted on the eval_runs +
   eval_case_results tables would need PR 1 merged into PR 3b's base first.

2. Hand-crafting a cassette requires a known-good Anthropic API response shape
   matching what AnthropicClient + DiagnosisConsumer parse. Recording one
   against the live API requires an API key and burns ~$0.01 — out of scope
   for a unit-test PR. PR 3c records cassettes against the 10 drafted corpus
   YAMLs as part of its scope.

3. The runner's unit tests (tests/unit/evals/test_runner_unit.py) cover the
   orchestration logic, failure modes, and aggregation math against fakes.
   The e2e test is needed for confidence in the wiring of:
       webhook -> Kafka -> enrichment consumer -> diagnosis consumer ->
       cassette transport -> persistence -> scoring -> report
   ...but every leg of that chain has its own integration coverage in
   tests/integration/* today.

PR 3c will replace this file with the real test against a one-case fixture
corpus + recorded cassette + the merged eval_runs schema.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skip(
    reason="deferred to PR 3c — needs PR 1's PostgresEvalRunRepository merged "
    "and a recorded Anthropic cassette; see module docstring for full rationale"
)
def test_eval_runner_e2e_with_cassette_replay() -> None:
    """End-to-end: load a fixture corpus YAML, set up the runner with a
    cassette-replay AnthropicClient, fire the webhook via ASGITransport,
    poll the diagnosis, score it, persist to the DB, write the report.

    Assertions to land in PR 3c:
        - eval_runs row exists with status='ok'
        - eval_case_results has shots_per_case rows for the case
        - persisted metrics include category_match=1.0 (cassette diagnosis
          matches the fixture's ground_truth)
        - evals/results/<run_id>.{json,md} files written
    """
