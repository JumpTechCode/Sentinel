# Eval Harness PR 3a — Harness Pieces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Land the three harness pieces the eval runner depends on, with full unit-test coverage — but no runner orchestration yet. Specifically: extend `sentinel/evals/schema.py` with corpus-shape Pydantic models, add `corpus_loader.py` (load + validate YAMLs), `fetcher_override.py` (corpus-replay equivalents of the 6 production fetchers + Protocol adapters for similar/runbook/log/active-alerts), and `cassette.py` (VCR-style record/replay layer for the Anthropic HTTP client).

**Architecture:** All three modules are pure infrastructure. `corpus_loader` is sync I/O (file read + YAML parse + Pydantic validate). `fetcher_override` provides drop-in implementations that match the existing `Fetcher` Protocol (and the four adapter Protocols in `enrichment/protocols.py`) but read from a per-process `ActiveCaseRegistry` instead of the database. `cassette` wraps `httpx.AsyncClient` so the eval runner can construct an `AsyncAnthropic` that records or replays HTTP requests to `api.anthropic.com` by `(prompt_version, model_id, case_id, shot_index)` key.

No FastAPI lifespan changes, no settings flags, no Makefile changes — those land in PR 3b (the runner). The pieces here are testable in isolation.

**Tech Stack:** Python 3.12, Pydantic v2, PyYAML, httpx, anthropic SDK ≥0.39.

**Spec reference:** `plans/2026-05-20-eval-harness-design.md` §1 (Module layout), §2 (Corpus YAML), §3 (Runner — fetcher swap mechanism), §7 (Cassette mechanism). PR 3a covers the harness-piece slice; PR 3b ships the runner + report + CLI; PR 3c ships the 10 corpus YAMLs.

**Dependency:** stacks on top of `feat/eval-harness-pr2-scoring` (PR 2). Uses `GroundTruth` from `sentinel.evals.schema`. When PR 2 merges, rebase this branch onto main.

---

## Out of scope (deferred)

- `runner.py`, `report.py`, `cli.py`, `Makefile` targets, README integration — **PR 3b**
- 10 hand-drafted postmortem YAMLs + recorded cassettes — **PR 3c**
- CI workflows (smoke / nightly / weekly baseline) — **PR 4**
- `SENTINEL_EVAL_MODE` settings flag + `lifespan()` branch in `sentinel/api/app.py` — **PR 3b** (paired with the runner that needs it)

---

## File Structure

**New / extended:**
- Modify: `sentinel/evals/schema.py` (extend with corpus-shape models)
- Create: `sentinel/evals/corpus_loader.py`
- Create: `sentinel/evals/fetcher_override.py`
- Create: `sentinel/evals/cassette.py`
- Modify: `sentinel/evals/__init__.py` (export the new types)

**Tests:**
- Create: `tests/unit/evals/test_schema_corpus.py` (schema validation for corpus models)
- Create: `tests/unit/evals/test_corpus_loader.py`
- Create: `tests/unit/evals/test_fetcher_override.py`
- Create: `tests/unit/evals/test_cassette.py`
- Create: `tests/unit/evals/fixtures/` directory (real YAML files for loader tests)

---

## Task 0: Confirm branch state

- [ ] **Step 1: Verify branch**

Run: `git branch --show-current && git log --oneline -5`
Expected: on `feat/eval-harness-pr3a-harness`; latest commits are from PR 2.

---

## Task 1: Extend `sentinel/evals/schema.py` with corpus-shape models

**Files:**
- Modify: `sentinel/evals/schema.py` (append new models — preserve existing GroundTruth/MetricSet/RegressionResult/RegressionVerdict)
- Modify: `sentinel/evals/__init__.py` (export new types)
- Create: `tests/unit/evals/test_schema_corpus.py`

The corpus YAML shape (from design §2) maps to four Pydantic models nested inside a top-level `CorpusCase`:
- `AlertSeed` — what's POSTed to the webhook
- `ContextSeed` — what enrichment would have returned (6 lists per the production IncidentContext)
- `GroundTruth` already exists from PR 2 — reused
- `CorpusCase` — top-level wrapper

The seed sub-items (DeploySeed, RelatedAlertSeed, etc.) are intentionally **separate from the production schema types** — they have only the fields a corpus YAML can provide (no DB-generated UUIDs, no timestamps the fetchers compute). The `fetcher_override` module is responsible for translating seed → production type at fetch time.

### Step 1: Write failing schema tests

```python
# tests/unit/evals/test_schema_corpus.py
"""Unit tests for CorpusCase + nested seed models in sentinel.evals.schema."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest


def test_corpus_case_minimal_happy_path() -> None:
    """Minimal valid corpus case loads cleanly."""
    from sentinel.evals.schema import (
        AlertSeed,
        ContextSeed,
        CorpusCase,
        GroundTruth,
    )

    case = CorpusCase(
        id="cloudflare-2022-06-21-bgp",
        corpus_version=1,
        source_url="https://blog.cloudflare.com/cloudflare-outage-on-june-21-2022/",
        sources_consulted=["https://blog.cloudflare.com/cloudflare-outage-on-june-21-2022/"],
        notes="Picked because BGP/config + clear remediation",
        alert=AlertSeed(
            source="generic",
            service="edge-network",
            severity="SEV1",
            title="Elevated 5xx errors across multiple POPs",
            timestamp=datetime(2022, 6, 21, 6, 27, tzinfo=UTC),
            raw_payload={},
        ),
        context_seed=ContextSeed(),  # all defaults are empty lists
        ground_truth=GroundTruth(
            category="config",
            acceptable_categories=["config", "deploy"],
            root_cause="BGP misconfiguration during planned change",
            correct_actions=["Revert the BGP configuration change"],
        ),
    )
    assert case.id == "cloudflare-2022-06-21-bgp"
    assert case.corpus_version == 1
    assert case.context_seed.deploys == []


def test_corpus_case_is_frozen() -> None:
    from sentinel.evals.schema import AlertSeed, ContextSeed, CorpusCase, GroundTruth

    case = CorpusCase(
        id="x",
        corpus_version=1,
        source_url="https://example.com",
        sources_consulted=["https://example.com"],
        alert=AlertSeed(
            source="generic",
            service="svc",
            severity="SEV2",
            title="t",
            timestamp=datetime.now(UTC),
            raw_payload={},
        ),
        context_seed=ContextSeed(),
        ground_truth=GroundTruth(
            category="deploy",
            acceptable_categories=["deploy"],
            root_cause="x",
            correct_actions=[],
        ),
    )
    with pytest.raises(ValueError):
        case.id = "y"  # type: ignore[misc]


def test_corpus_case_rejects_unknown_alert_source() -> None:
    from sentinel.evals.schema import AlertSeed

    with pytest.raises(ValueError):
        AlertSeed(
            source="splunk",  # type: ignore[arg-type]  # not in the allowed set
            service="svc",
            severity="SEV2",
            title="t",
            timestamp=datetime.now(UTC),
            raw_payload={},
        )


def test_context_seed_with_deploys_and_logs() -> None:
    """ContextSeed accepts populated sub-lists with stable IDs."""
    from sentinel.evals.schema import (
        ContextSeed,
        DeploySeed,
        LogSeed,
    )

    seed = ContextSeed(
        deploys=[
            DeploySeed(
                id="deploy:abc123",
                service="edge-network",
                sha="abc123",
                pr_title="Update BGP route propagation policy",
                pr_diff_summary="Modifies prefix-list filter ordering",
                deployed_at=datetime(2022, 6, 21, 6, 25, tzinfo=UTC),
            )
        ],
        recent_logs=[
            LogSeed(
                id="log:1",
                timestamp=datetime(2022, 6, 21, 6, 27, 12, tzinfo=UTC),
                level="error",
                service="edge-network",
                message="BGP session reset peer 1.2.3.4",
            )
        ],
    )
    assert len(seed.deploys) == 1
    assert seed.deploys[0].id == "deploy:abc123"
    assert len(seed.recent_logs) == 1


def test_deploy_seed_requires_id_with_deploy_prefix_convention() -> None:
    """Stable IDs follow `[kind]:[id]` per design §2 / spec invariant 4.
    Schema doesn't validate the prefix at the type level (it's just `str`),
    but a missing colon would break the evidence-quality scorer's lookup.
    Verifying the ID is non-empty + a string is enough — the convention is
    a corpus-curation discipline, not a schema constraint.
    """
    from sentinel.evals.schema import DeploySeed

    # Convention compliance — these are valid
    DeploySeed(
        id="deploy:abc123",
        service="x",
        sha="abc123",
        deployed_at=datetime.now(UTC),
    )

    # Non-empty constraint still applies
    with pytest.raises(ValueError):
        DeploySeed(
            id="",
            service="x",
            sha="abc123",
            deployed_at=datetime.now(UTC),
        )


def test_alert_seed_severity_validated() -> None:
    from sentinel.evals.schema import AlertSeed

    with pytest.raises(ValueError):
        AlertSeed(
            source="generic",
            service="svc",
            severity="SEV99",  # type: ignore[arg-type]  # not in SeverityType
            title="t",
            timestamp=datetime.now(UTC),
            raw_payload={},
        )
```

### Step 2: Run — confirm fail

Run: `.venv/bin/pytest tests/unit/evals/test_schema_corpus.py -v --no-cov`
Expected: FAIL — ImportError on the new types.

### Step 3: Append the new models to `sentinel/evals/schema.py`

Append at the end of the existing `schema.py` (do NOT touch GroundTruth / MetricSet / RegressionResult / RegressionVerdict):

```python
# --- Corpus YAML shape (PR 3a) ---
# Models below describe the corpus YAML structure. They're intentionally
# separate from the production schema types in sentinel/schemas/context.py:
# corpus seeds have only the fields a curator can supply (no DB-generated
# UUIDs, no fetcher-computed timestamps). The fetcher_override module
# translates seed → production type at fetch time.

from datetime import datetime
from typing import Any, Literal

from sentinel.schemas.enums import SeverityType


_AlertSource = Literal["generic", "sentry", "pagerduty", "datadog"]


class AlertSeed(BaseModel):
    """Synthetic webhook payload — what gets POSTed to /webhooks/{source}."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: _AlertSource
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    timestamp: datetime
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class DeploySeed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str = Field(min_length=1)  # convention: `deploy:<sha>`
    service: str = Field(min_length=1)
    sha: str = Field(min_length=1)
    pr_number: int | None = None
    pr_title: str | None = None
    pr_diff_summary: str | None = None
    deployed_at: datetime
    deployed_by: str | None = None


class RelatedAlertSeed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str = Field(min_length=1)  # convention: `related:<uuid>`
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    opened_at: datetime


class SimilarIncidentSeed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str = Field(min_length=1)  # convention: `similar:<uuid>`
    title: str = Field(min_length=1)
    root_cause: str | None = None
    remediation: str | None = None
    resolved_at: datetime | None = None
    cosine_similarity: float = Field(default=0.0, ge=0.0, le=1.0)


class RunbookSeed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str = Field(min_length=1)  # convention: `runbook:<uuid>`
    title: str = Field(min_length=1)
    content: str = Field(min_length=1)


class LogSeed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str = Field(min_length=1)  # convention: `log:<n>`
    timestamp: datetime
    level: Literal["debug", "info", "warning", "error", "critical"]
    service: str = Field(min_length=1)
    message: str = Field(min_length=1)


class ContextSeed(BaseModel):
    """What enrichment would have returned — the CorpusFetcher replays this."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    deploys: list[DeploySeed] = Field(default_factory=list)
    related_alerts: list[RelatedAlertSeed] = Field(default_factory=list)
    similar_incidents: list[SimilarIncidentSeed] = Field(default_factory=list)
    runbooks: list[RunbookSeed] = Field(default_factory=list)
    recent_logs: list[LogSeed] = Field(default_factory=list)
    active_alerts: list[RelatedAlertSeed] = Field(default_factory=list)


class CorpusCase(BaseModel):
    """One postmortem case, loaded from a YAML file under sentinel/evals/corpus/."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(min_length=1)
    corpus_version: int = Field(ge=1)
    source_url: str = Field(min_length=1)
    sources_consulted: list[str] = Field(min_length=1)
    notes: str = ""  # curator's free-text rationale; not scored
    alert: AlertSeed
    context_seed: ContextSeed
    ground_truth: GroundTruth
```

### Step 4: Update `sentinel/evals/__init__.py` exports

Add to the imports (alphabetical) — append below the existing `from sentinel.evals.schema import (...)` block additions:

```python
    AlertSeed,
    ContextSeed,
    CorpusCase,
    DeploySeed,
    LogSeed,
    RelatedAlertSeed,
    RunbookSeed,
    SimilarIncidentSeed,
```

And add the same names to `__all__` in alphabetical order.

### Step 5: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_schema_corpus.py -v --no-cov`
Expected: all 6 tests PASS.

### Step 6: mypy strict

Run: `mypy --strict sentinel/evals/ tests/unit/evals/test_schema_corpus.py`
Expected: no errors.

### Step 7: Commit

```bash
git add sentinel/evals/schema.py sentinel/evals/__init__.py tests/unit/evals/test_schema_corpus.py
git commit -m "$(cat <<'EOF'
feat(evals): CorpusCase + nested seed models for the harness corpus YAML

Extends sentinel/evals/schema.py (PR 2 base) with the corpus-shape models.
Seeds are intentionally separate from sentinel/schemas/context.py production
types — they carry only fields a curator can supply (no DB-generated UUIDs,
no fetcher-computed timestamps). fetcher_override (next task) translates
seed → production type at fetch time.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `corpus_loader.py` — load + validate YAMLs

**Files:**
- Create: `sentinel/evals/corpus_loader.py`
- Create: `tests/unit/evals/fixtures/cloudflare-bgp.yaml` (valid sample for loader tests)
- Create: `tests/unit/evals/fixtures/broken-missing-ground-truth.yaml` (invalid sample for error paths)
- Create: `tests/unit/evals/test_corpus_loader.py`

### Step 1: Write the fixture YAMLs

Create `tests/unit/evals/fixtures/cloudflare-bgp.yaml`:

```yaml
id: cloudflare-bgp-test-fixture
corpus_version: 1
source_url: https://example.com/cloudflare-bgp-2022
sources_consulted:
  - https://example.com/cloudflare-bgp-2022
notes: Test fixture — not a real curated case
alert:
  source: generic
  service: edge-network
  severity: SEV1
  title: Elevated 5xx errors across multiple POPs
  timestamp: "2022-06-21T06:27:00Z"
  raw_payload:
    error_rate: 0.42
context_seed:
  deploys:
    - id: deploy:abc123
      service: edge-network
      sha: abc123
      pr_title: Update BGP route propagation policy
      pr_diff_summary: Modifies prefix-list filter ordering
      deployed_at: "2022-06-21T06:25:00Z"
  recent_logs:
    - id: log:1
      timestamp: "2022-06-21T06:27:12Z"
      level: error
      service: edge-network
      message: BGP session reset peer 1.2.3.4
ground_truth:
  category: config
  acceptable_categories:
    - config
    - deploy
  root_cause: BGP misconfiguration during a planned network change caused route propagation failures
  correct_actions:
    - Revert the BGP configuration change
    - Roll back the deploy
```

Create `tests/unit/evals/fixtures/broken-missing-ground-truth.yaml`:

```yaml
id: broken-case
corpus_version: 1
source_url: https://example.com/x
sources_consulted:
  - https://example.com/x
alert:
  source: generic
  service: svc
  severity: SEV2
  title: t
  timestamp: "2022-06-21T06:27:00Z"
  raw_payload: {}
context_seed: {}
# ground_truth intentionally missing — should raise a clear validation error
```

### Step 2: Write failing loader tests

```python
# tests/unit/evals/test_corpus_loader.py
"""Unit tests for sentinel.evals.corpus_loader."""

from __future__ import annotations

from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"


def test_load_case_round_trips_a_valid_yaml() -> None:
    from sentinel.evals.corpus_loader import load_case

    case = load_case(_FIXTURES / "cloudflare-bgp.yaml")
    assert case.id == "cloudflare-bgp-test-fixture"
    assert case.alert.service == "edge-network"
    assert case.alert.severity == "SEV1"
    assert len(case.context_seed.deploys) == 1
    assert case.context_seed.deploys[0].id == "deploy:abc123"
    assert case.ground_truth.category == "config"


def test_load_case_raises_on_missing_required_section() -> None:
    from sentinel.evals.corpus_loader import CorpusValidationError, load_case

    with pytest.raises(CorpusValidationError) as exc:
        load_case(_FIXTURES / "broken-missing-ground-truth.yaml")
    assert "ground_truth" in str(exc.value).lower()
    # The exception carries the source path for operator clarity
    assert "broken-missing-ground-truth.yaml" in str(exc.value)


def test_load_case_raises_on_missing_file() -> None:
    from sentinel.evals.corpus_loader import load_case

    with pytest.raises(FileNotFoundError):
        load_case(_FIXTURES / "does-not-exist.yaml")


def test_load_corpus_dir_returns_sorted_by_id(tmp_path: Path) -> None:
    """load_corpus_dir loads every *.yaml under a directory, sorted by case.id.
    Sorted order matters for the deterministic 5-case smoke subset (design §7).
    """
    from sentinel.evals.corpus_loader import load_corpus_dir

    # Copy the fixture into a fresh dir + a renamed copy to verify ordering
    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "z-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "z-case"))
    (tmp_path / "a-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "a-case"))
    (tmp_path / "m-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "m-case"))

    cases = load_corpus_dir(tmp_path)
    assert [c.id for c in cases] == ["a-case", "m-case", "z-case"]


def test_load_corpus_dir_raises_on_duplicate_ids(tmp_path: Path) -> None:
    from sentinel.evals.corpus_loader import CorpusValidationError, load_corpus_dir

    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "one.yaml").write_text(src)  # id: cloudflare-bgp-test-fixture
    (tmp_path / "two.yaml").write_text(src)  # same id

    with pytest.raises(CorpusValidationError, match="duplicate"):
        load_corpus_dir(tmp_path)


def test_load_corpus_dir_skips_non_yaml(tmp_path: Path) -> None:
    """README.md and other non-YAML files in the corpus directory are ignored."""
    from sentinel.evals.corpus_loader import load_corpus_dir

    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "case.yaml").write_text(src)
    (tmp_path / "README.md").write_text("# Corpus README")
    (tmp_path / "notes.txt").write_text("ignore me")

    cases = load_corpus_dir(tmp_path)
    assert len(cases) == 1
```

### Step 3: Run — confirm fail

Run: `.venv/bin/pytest tests/unit/evals/test_corpus_loader.py -v --no-cov`
Expected: FAIL — ImportError.

### Step 4: Create `sentinel/evals/corpus_loader.py`

```python
# sentinel/evals/corpus_loader.py
"""Load + validate corpus YAML files.

Fail-loud philosophy: a broken YAML breaks `make evals` immediately at
import-time, not three minutes into a run. Returns frozen Pydantic models;
duplicate case IDs across the directory raise CorpusValidationError so a
fat-finger curator copy/paste doesn't silently produce a "two cases, same
metrics" bug.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from sentinel.evals.schema import CorpusCase


class CorpusValidationError(Exception):
    """Raised when a corpus file or directory fails validation.

    Carries the source path in the message so operators can find the offending
    YAML without grepping the stack trace.
    """


def load_case(path: Path) -> CorpusCase:
    """Load one YAML file as a CorpusCase. Raises FileNotFoundError on absent
    file, CorpusValidationError on parse/validation failure (with the path
    embedded in the message).
    """
    text = path.read_text(encoding="utf-8")  # raises FileNotFoundError if missing
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise CorpusValidationError(f"{path}: YAML parse error — {e}") from e

    if not isinstance(raw, dict):
        raise CorpusValidationError(
            f"{path}: top-level YAML must be a mapping, got {type(raw).__name__}"
        )

    try:
        return CorpusCase.model_validate(raw)
    except ValidationError as e:
        raise CorpusValidationError(f"{path}: schema validation failed — {e}") from e


def load_corpus_dir(dir_path: Path) -> list[CorpusCase]:
    """Load all *.yaml files under a directory; returns cases sorted by case.id.

    Non-YAML files (README, .txt, hidden) are silently skipped.
    Duplicate case IDs across files raise CorpusValidationError.
    """
    cases: list[CorpusCase] = []
    yaml_files = sorted(dir_path.glob("*.yaml"))

    for path in yaml_files:
        cases.append(load_case(path))

    # Guard against duplicate IDs — would silently produce duplicate metric rows
    # in eval_case_results and corrupt the regression baseline.
    seen: dict[str, Path] = {}
    for path, case in zip(yaml_files, cases, strict=True):
        if case.id in seen:
            raise CorpusValidationError(
                f"duplicate case id {case.id!r}: {seen[case.id]} and {path}"
            )
        seen[case.id] = path

    return sorted(cases, key=lambda c: c.id)
```

### Step 5: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_corpus_loader.py -v --no-cov`
Expected: all 6 tests PASS.

### Step 6: Confirm PyYAML is available

Run: `.venv/bin/python -c "import yaml; print(yaml.__version__)"`
Expected: prints a version (likely 6.x). PyYAML ships with many deps — but if it's missing, add `"pyyaml>=6.0,<7.0"` to the `dependencies` in `pyproject.toml` and `pip install -e .`. Then add `"yaml.*"` to the existing mypy `ignore_missing_imports` override block if mypy complains.

### Step 7: mypy strict

Run: `mypy --strict sentinel/evals/corpus_loader.py tests/unit/evals/test_corpus_loader.py`
Expected: clean.

### Step 8: Wire export

Add to `sentinel/evals/__init__.py` (alphabetical):

```python
from sentinel.evals.corpus_loader import CorpusValidationError, load_case, load_corpus_dir
```

And in `__all__`:

```python
"CorpusValidationError",
"load_case",
"load_corpus_dir",
```

### Step 9: Commit

```bash
git add sentinel/evals/corpus_loader.py sentinel/evals/__init__.py tests/unit/evals/test_corpus_loader.py tests/unit/evals/fixtures/
git commit -m "$(cat <<'EOF'
feat(evals): corpus_loader — fail-loud YAML load + per-directory dedup

Loads one YAML as a frozen CorpusCase; loads a directory's worth as a
deterministic id-sorted list (sort matters for the 5-case smoke subset
per design §7). Duplicate case IDs across the directory raise
CorpusValidationError so a copy-paste fat-finger doesn't silently
corrupt the regression baseline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `fetcher_override.py` — corpus-replay drop-ins for the 6 production fetchers + 4 Protocol adapters

**Files:**
- Create: `sentinel/evals/fetcher_override.py`
- Create: `tests/unit/evals/test_fetcher_override.py`

The production enrichment pipeline (see `sentinel/enrichment/orchestrator.py:52`) has a `Fetcher` Protocol:
```python
class Fetcher(Protocol):
    name: str
    timeout_s: float
    async def fetch(self, incident, deps) -> FetcherResult[Any]: ...
```

Six concrete fetchers map to six sections (`_SECTION_TO_FIELD` in orchestrator.py:42 — deploys/related_alerts/similar_incidents/runbooks/recent_logs/active_alerts).

For eval mode we need **drop-in equivalents** that read from a per-process registry holding the **active case**, ignore the `deps` argument, and translate seed types → production context types. The runner (PR 3b) sets the active case before firing each webhook.

The four adapter Protocols in `enrichment/protocols.py` (`SimilarIncidentRetrieval`, `RunbookRetrieval`, `LogSearchAdapter`, `ActiveAlertsAdapter`) are constructed-and-passed into `EnrichmentDeps` and called by the corresponding fetchers. **In eval mode the fetchers don't call the adapters** — they read straight from the registry — so we don't need corpus adapters. We do still need to construct an `EnrichmentDeps` object the orchestrator can hold; the easiest path is to pass production no-op adapters (already exist in `sentinel/enrichment/defaults.py`).

### Step 1: Write failing tests

```python
# tests/unit/evals/test_fetcher_override.py
"""Unit tests for sentinel.evals.fetcher_override (corpus-replay fetchers)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from sentinel.evals.schema import (
    AlertSeed,
    ContextSeed,
    CorpusCase,
    DeploySeed,
    GroundTruth,
    LogSeed,
    RelatedAlertSeed,
    RunbookSeed,
    SimilarIncidentSeed,
)


def _build_case(case_id: str = "test-case") -> CorpusCase:
    now = datetime.now(UTC)
    return CorpusCase(
        id=case_id,
        corpus_version=1,
        source_url="https://example.com",
        sources_consulted=["https://example.com"],
        alert=AlertSeed(
            source="generic",
            service="svc",
            severity="SEV2",
            title="t",
            timestamp=now,
            raw_payload={},
        ),
        context_seed=ContextSeed(
            deploys=[
                DeploySeed(
                    id="deploy:abc",
                    service="svc",
                    sha="abc",
                    deployed_at=now,
                )
            ],
            related_alerts=[
                RelatedAlertSeed(
                    id="related:1",
                    service="svc",
                    severity="SEV3",
                    title="related",
                    opened_at=now,
                )
            ],
            similar_incidents=[
                SimilarIncidentSeed(
                    id="similar:1",
                    title="prior incident",
                    cosine_similarity=0.85,
                )
            ],
            runbooks=[
                RunbookSeed(
                    id="runbook:1",
                    title="rb",
                    content="do x",
                )
            ],
            recent_logs=[
                LogSeed(
                    id="log:1",
                    timestamp=now,
                    level="error",
                    service="svc",
                    message="boom",
                )
            ],
            active_alerts=[
                RelatedAlertSeed(
                    id="active:1",
                    service="svc",
                    severity="SEV2",
                    title="active",
                    opened_at=now,
                )
            ],
        ),
        ground_truth=GroundTruth(
            category="deploy",
            acceptable_categories=["deploy"],
            root_cause="x",
            correct_actions=[],
        ),
    )


def test_registry_default_is_empty() -> None:
    from sentinel.evals.fetcher_override import ActiveCaseRegistry

    reg = ActiveCaseRegistry()
    assert reg.get() is None


def test_registry_set_and_get() -> None:
    from sentinel.evals.fetcher_override import ActiveCaseRegistry

    reg = ActiveCaseRegistry()
    case = _build_case()
    reg.set(case)
    assert reg.get() is case


def test_registry_clear() -> None:
    from sentinel.evals.fetcher_override import ActiveCaseRegistry

    reg = ActiveCaseRegistry()
    reg.set(_build_case())
    reg.clear()
    assert reg.get() is None


@pytest.mark.asyncio
async def test_deploys_fetcher_returns_translated_seed_data() -> None:
    """CorpusDeploysFetcher reads from the registry and produces the production
    FetcherResult[DeployItem] shape."""
    from sentinel.evals.fetcher_override import (
        ActiveCaseRegistry,
        CorpusDeploysFetcher,
    )

    reg = ActiveCaseRegistry()
    reg.set(_build_case())
    f = CorpusDeploysFetcher(reg)
    result = await f.fetch(incident=None, deps=None)
    assert result.status == "ok"
    assert len(result.data) == 1
    assert result.data[0].id == "deploy:abc"
    assert result.data[0].service == "svc"
    assert f.name == "deploys"
    assert f.timeout_s > 0


@pytest.mark.asyncio
async def test_deploys_fetcher_raises_on_no_active_case() -> None:
    """An unset registry is a programming bug — the runner should set the active
    case before firing the webhook. Raise loudly rather than return degraded."""
    from sentinel.evals.fetcher_override import (
        ActiveCaseRegistry,
        CorpusDeploysFetcher,
    )

    f = CorpusDeploysFetcher(ActiveCaseRegistry())
    with pytest.raises(RuntimeError, match="no active corpus case"):
        await f.fetch(incident=None, deps=None)


@pytest.mark.asyncio
async def test_all_six_fetchers_return_expected_section_data() -> None:
    """Smoke test that each of the 6 corpus fetchers returns the right section."""
    from sentinel.evals.fetcher_override import (
        ActiveCaseRegistry,
        CorpusActiveAlertsFetcher,
        CorpusDeploysFetcher,
        CorpusRecentLogsFetcher,
        CorpusRelatedAlertsFetcher,
        CorpusRunbooksFetcher,
        CorpusSimilarIncidentsFetcher,
    )

    reg = ActiveCaseRegistry()
    reg.set(_build_case())

    pairs: list[tuple[str, int]] = []
    for fetcher_cls, expected_name, expected_first_id in [
        (CorpusDeploysFetcher, "deploys", "deploy:abc"),
        (CorpusRelatedAlertsFetcher, "related_alerts", "related:1"),
        (CorpusSimilarIncidentsFetcher, "similar_incidents", "similar:1"),
        (CorpusRunbooksFetcher, "runbooks", "runbook:1"),
        (CorpusRecentLogsFetcher, "recent_logs", "log:1"),
        (CorpusActiveAlertsFetcher, "active_alerts", "active:1"),
    ]:
        f = fetcher_cls(reg)
        assert f.name == expected_name
        result = await f.fetch(incident=None, deps=None)
        assert result.status == "ok"
        assert len(result.data) == 1
        assert result.data[0].id == expected_first_id
        pairs.append((expected_name, len(result.data)))
    assert len(pairs) == 6


def test_corpus_fetchers_returns_all_six() -> None:
    """The convenience constructor returns the production-shaped tuple."""
    from sentinel.evals.fetcher_override import (
        ActiveCaseRegistry,
        corpus_fetchers,
    )

    reg = ActiveCaseRegistry()
    fetchers = corpus_fetchers(reg)
    assert len(fetchers) == 6
    names = {f.name for f in fetchers}
    assert names == {
        "deploys",
        "related_alerts",
        "similar_incidents",
        "runbooks",
        "recent_logs",
        "active_alerts",
    }


@pytest.mark.asyncio
async def test_similar_incident_id_maps_to_uuid_or_passes_through_string() -> None:
    """SimilarIncidentItem in production has id: str (kind:uuid) — corpus uses
    the same format. Verify the translation preserves the string form."""
    from sentinel.evals.fetcher_override import (
        ActiveCaseRegistry,
        CorpusSimilarIncidentsFetcher,
    )

    reg = ActiveCaseRegistry()
    reg.set(_build_case())
    f = CorpusSimilarIncidentsFetcher(reg)
    result = await f.fetch(incident=None, deps=None)
    assert result.data[0].id == "similar:1"
```

### Step 2: Run — confirm fail

Run: `.venv/bin/pytest tests/unit/evals/test_fetcher_override.py -v --no-cov`
Expected: FAIL — ImportError.

### Step 3: Create `sentinel/evals/fetcher_override.py`

**IMPORTANT:** Before writing the fetcher classes, check `sentinel/schemas/context.py` for the EXACT field shape of `DeployItem`, `RelatedAlertItem`, `SimilarIncidentItem`, `RunbookItem`, `LogLine` — the translation must produce valid production types. If any seed field is missing in production (or vice versa), adjust the translation. Run:

```bash
grep -A 10 "class DeployItem\|class RelatedAlertItem\|class SimilarIncidentItem\|class RunbookItem\|class LogLine" sentinel/schemas/context.py
```

Use the actual field list when translating, NOT the assumptions in this plan.

```python
# sentinel/evals/fetcher_override.py
"""Corpus-replay drop-ins for the 6 production Fetcher implementations.

In eval mode the orchestrator runs the same parallel-fetch logic, but each
fetcher reads from an ActiveCaseRegistry (set by the runner before firing
each synthetic webhook) instead of hitting the DB / external systems.

Translation: the corpus YAML uses *Seed types (sentinel/evals/schema.py) —
strict subsets carrying only curator-supplied fields. These fetchers
translate seed → production context type (sentinel/schemas/context.py) at
fetch time, supplying defaults for fetcher-computed fields (e.g.
fetched_at).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sentinel.evals.schema import CorpusCase
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


_FETCHER_TIMEOUT_S = 0.5  # arbitrary; CorpusFetchers never block on I/O


class ActiveCaseRegistry:
    """Per-process holder for the currently-active corpus case.

    The runner sets the active case before POSTing each synthetic webhook;
    the fetchers read it during the enrichment fan-out. NOT thread-safe —
    the eval runner is single-threaded by design (the orchestrator fans out
    via asyncio, not threads).
    """

    def __init__(self) -> None:
        self._case: CorpusCase | None = None

    def get(self) -> CorpusCase | None:
        return self._case

    def set(self, case: CorpusCase) -> None:
        self._case = case

    def clear(self) -> None:
        self._case = None


def _now() -> datetime:
    return datetime.now(UTC)


def _require_active(registry: ActiveCaseRegistry) -> CorpusCase:
    case = registry.get()
    if case is None:
        raise RuntimeError(
            "no active corpus case — the eval runner must set the registry "
            "before triggering enrichment"
        )
    return case


# --- Six concrete fetchers, one per production Fetcher ---


class CorpusDeploysFetcher:
    name = "deploys"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[DeployItem]:
        case = _require_active(self._registry)
        items = [
            DeployItem(
                id=d.id,
                service=d.service,
                sha=d.sha,
                pr_number=d.pr_number,
                pr_title=d.pr_title,
                pr_diff_summary=d.pr_diff_summary,
                deployed_at=d.deployed_at,
                deployed_by=d.deployed_by,
            )
            for d in case.context_seed.deploys
        ]
        return FetcherResult[DeployItem](status="ok", data=items, fetched_at=_now())


class CorpusRelatedAlertsFetcher:
    name = "related_alerts"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RelatedAlertItem]:
        case = _require_active(self._registry)
        items = [
            RelatedAlertItem(
                id=a.id,
                service=a.service,
                severity=a.severity,
                title=a.title,
                opened_at=a.opened_at,
            )
            for a in case.context_seed.related_alerts
        ]
        return FetcherResult[RelatedAlertItem](status="ok", data=items, fetched_at=_now())


class CorpusSimilarIncidentsFetcher:
    name = "similar_incidents"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[SimilarIncidentItem]:
        case = _require_active(self._registry)
        items = [
            SimilarIncidentItem(
                id=s.id,
                title=s.title,
                root_cause=s.root_cause,
                remediation=s.remediation,
                resolved_at=s.resolved_at,
                cosine_similarity=s.cosine_similarity,
            )
            for s in case.context_seed.similar_incidents
        ]
        return FetcherResult[SimilarIncidentItem](status="ok", data=items, fetched_at=_now())


class CorpusRunbooksFetcher:
    name = "runbooks"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RunbookItem]:
        case = _require_active(self._registry)
        items = [
            RunbookItem(
                id=r.id,
                title=r.title,
                content=r.content,
            )
            for r in case.context_seed.runbooks
        ]
        return FetcherResult[RunbookItem](status="ok", data=items, fetched_at=_now())


class CorpusRecentLogsFetcher:
    name = "recent_logs"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[LogLine]:
        case = _require_active(self._registry)
        items = [
            LogLine(
                id=lg.id,
                timestamp=lg.timestamp,
                level=lg.level,
                service=lg.service,
                message=lg.message,
            )
            for lg in case.context_seed.recent_logs
        ]
        return FetcherResult[LogLine](status="ok", data=items, fetched_at=_now())


class CorpusActiveAlertsFetcher:
    name = "active_alerts"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RelatedAlertItem]:
        case = _require_active(self._registry)
        items = [
            RelatedAlertItem(
                id=a.id,
                service=a.service,
                severity=a.severity,
                title=a.title,
                opened_at=a.opened_at,
            )
            for a in case.context_seed.active_alerts
        ]
        return FetcherResult[RelatedAlertItem](status="ok", data=items, fetched_at=_now())


def corpus_fetchers(
    registry: ActiveCaseRegistry,
) -> tuple[
    CorpusDeploysFetcher,
    CorpusRelatedAlertsFetcher,
    CorpusSimilarIncidentsFetcher,
    CorpusRunbooksFetcher,
    CorpusRecentLogsFetcher,
    CorpusActiveAlertsFetcher,
]:
    """Build the standard tuple of all 6 corpus fetchers — drop-in replacement
    for sentinel.enrichment.defaults.default_fetchers() in eval mode."""
    return (
        CorpusDeploysFetcher(registry),
        CorpusRelatedAlertsFetcher(registry),
        CorpusSimilarIncidentsFetcher(registry),
        CorpusRunbooksFetcher(registry),
        CorpusRecentLogsFetcher(registry),
        CorpusActiveAlertsFetcher(registry),
    )
```

### Step 4: Adjust the translation if production types differ

Before re-running tests, run the grep from Step 3 above. If production `RelatedAlertItem`, `SimilarIncidentItem`, `RunbookItem`, or `LogLine` has fields the seed doesn't supply (or vice versa), update the translation. Common adjustments:
- `LogLine.timestamp` may want a UTC-normalized form
- `SimilarIncidentItem` may have extra fields (e.g., `incident_id`) that the seed doesn't carry — supply `None` or generate a UUID
- `RunbookItem` may want an `updated_at` field

Re-run the tests and adapt the fixture data + translation until everything maps cleanly.

### Step 5: Run

Run: `.venv/bin/pytest tests/unit/evals/test_fetcher_override.py -v --no-cov`
Expected: 9 tests PASS.

### Step 6: mypy strict

Run: `mypy --strict sentinel/evals/fetcher_override.py tests/unit/evals/test_fetcher_override.py`
Expected: clean. The `Any` types on `fetch(incident, deps)` match the production Protocol; no narrowing needed.

### Step 7: Wire exports

Add to `sentinel/evals/__init__.py` (alphabetical):

```python
from sentinel.evals.fetcher_override import (
    ActiveCaseRegistry,
    CorpusActiveAlertsFetcher,
    CorpusDeploysFetcher,
    CorpusRecentLogsFetcher,
    CorpusRelatedAlertsFetcher,
    CorpusRunbooksFetcher,
    CorpusSimilarIncidentsFetcher,
    corpus_fetchers,
)
```

And add all 8 names to `__all__` (alphabetical).

### Step 8: Commit

```bash
git add sentinel/evals/fetcher_override.py sentinel/evals/__init__.py tests/unit/evals/test_fetcher_override.py
git commit -m "$(cat <<'EOF'
feat(evals): fetcher_override — 6 corpus-replay fetchers + active-case registry

Drop-in equivalents for sentinel/enrichment/fetchers/* in eval mode. Each
fetcher reads from a per-process ActiveCaseRegistry (set by the runner
before firing each webhook) and translates Seed types → production
context types. Raises loudly if no active case is set — that's a runner
bug, not a degraded path.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `cassette.py` — VCR-style HTTP record/replay for the Anthropic client

**Files:**
- Create: `sentinel/evals/cassette.py`
- Create: `tests/unit/evals/test_cassette.py`
- Create: `tests/unit/evals/fixtures/sample_cassette.json` (one recorded interaction for the replay test)

The cassette injects a custom `httpx.AsyncClient` into `anthropic.AsyncAnthropic(http_client=...)` (SDK ≥0.39 supports this). The httpx client uses a custom `httpx.AsyncBaseTransport` that:

- **Record mode**: forwards to the network, captures the request + response, writes JSON to `cassette_dir/<key>.json`. Requires `ANTHROPIC_API_KEY` env var.
- **Replay mode**: matches the incoming request by `key`, returns the recorded response. Miss → raises `CassetteMiss` (loud failure, no fallback to network).

Key derivation: `sha256(canonical_json({"prompt_version": ..., "model_id": ..., "case_id": ..., "shot_index": ...}))[:16]`. The runner (PR 3b) supplies these via a `CassetteContext` set on the transport before each request.

### Step 1: Write failing tests

```python
# tests/unit/evals/test_cassette.py
"""Unit tests for sentinel.evals.cassette (VCR-style Anthropic HTTP layer)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest


def test_cassette_key_is_stable_across_runs() -> None:
    """Same context → same key (so the runner can find recorded responses)."""
    from sentinel.evals.cassette import compute_cassette_key

    k1 = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    k2 = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    assert k1 == k2
    assert len(k1) == 16  # 16-char prefix per the design


def test_cassette_key_changes_on_any_input_change() -> None:
    from sentinel.evals.cassette import compute_cassette_key

    base = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_prompt = compute_cassette_key(
        prompt_version="v2",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_model = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-opus-4-1",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_case = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="other-case",
        shot_index=0,
    )
    different_shot = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=1,
    )
    assert len({base, different_prompt, different_model, different_case, different_shot}) == 5


@pytest.mark.asyncio
async def test_replay_returns_recorded_response(tmp_path: Path) -> None:
    """A pre-written cassette file is replayed verbatim."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteTransport,
    )

    key = "deadbeefcafe1234"
    cassette_path = tmp_path / f"{key}.json"
    cassette_path.write_text(
        json.dumps(
            {
                "key": key,
                "request": {"method": "POST", "url": "https://api.anthropic.com/v1/messages"},
                "response": {
                    "status_code": 200,
                    "headers": [["content-type", "application/json"]],
                    "body_b64": "eyJpZCI6Im1zZ18xMjMifQ==",  # b64("{\"id\":\"msg_123\"}")
                },
            }
        )
    )

    transport = CassetteTransport(mode="replay", cassette_dir=tmp_path)
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )
    # Override key to the one we wrote (so the test doesn't depend on hash output)
    transport.override_next_key(key)

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    response = await transport.handle_async_request(request)
    assert response.status_code == 200
    body = await response.aread()
    assert body == b'{"id":"msg_123"}'


@pytest.mark.asyncio
async def test_replay_miss_raises_loudly(tmp_path: Path) -> None:
    """No matching cassette → CassetteMiss with the missing key in the message
    so the operator knows to re-record."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteMiss,
        CassetteTransport,
    )

    transport = CassetteTransport(mode="replay", cassette_dir=tmp_path)
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    with pytest.raises(CassetteMiss) as exc:
        await transport.handle_async_request(request)
    assert "re-record" in str(exc.value).lower() or "regenerate" in str(exc.value).lower()


def test_record_mode_requires_set_context_before_handle(tmp_path: Path) -> None:
    """Without a CassetteContext the transport can't compute a key — programming
    bug. Raise loudly on the first request, not silently overwrite."""
    from sentinel.evals.cassette import CassetteTransport

    transport = CassetteTransport(mode="record", cassette_dir=tmp_path)
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    with pytest.raises(RuntimeError, match="set_context"):
        # We don't actually call the network in this test — the no-context check
        # fires first. (handle_async_request is async, but the validation is sync.)
        import asyncio

        asyncio.run(transport.handle_async_request(request))


@pytest.mark.asyncio
async def test_record_mode_writes_cassette_then_returns_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify record-mode flow without hitting the real network: monkeypatch the
    inner transport's send to return a canned response."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteTransport,
    )

    canned_response = httpx.Response(
        status_code=200,
        headers={"content-type": "application/json"},
        content=b'{"id":"recorded_msg"}',
    )

    class _FakeInner(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return canned_response

    transport = CassetteTransport(
        mode="record", cassette_dir=tmp_path, inner_transport=_FakeInner()
    )
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={"q": 1})
    response = await transport.handle_async_request(request)
    assert response.status_code == 200

    # Cassette file written under tmp_path with the computed key
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["response"]["status_code"] == 200
```

### Step 2: Run — confirm fail

Run: `.venv/bin/pytest tests/unit/evals/test_cassette.py -v --no-cov`
Expected: FAIL — ImportError.

### Step 3: Create `sentinel/evals/cassette.py`

```python
# sentinel/evals/cassette.py
"""VCR-style HTTP record/replay layer for the Anthropic client.

The eval runner (PR 3b) constructs an AsyncAnthropic wrapping a custom
httpx.AsyncClient whose transport is a CassetteTransport. Before each
diagnosis call the runner sets a CassetteContext on the transport;
the transport computes a stable key from (prompt_version, model_id,
case_id, shot_index) and either records the live HTTP exchange (record
mode) or replays a previously-recorded one (replay mode).

Design choices per spec §7:
- Cassettes live under sentinel/evals/cassettes/<prompt_version>/<model_id>/
  with filename = <16-char key>.json.
- Replay-mode miss raises CassetteMiss with a regenerate guidance message;
  no silent fallback to the network.
- Record mode requires ANTHROPIC_API_KEY env var (the inner transport
  forwards to api.anthropic.com).
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx


CassetteMode = Literal["record", "replay"]


class CassetteMiss(Exception):
    """No recorded cassette matches the current key — replay cannot proceed.

    Carries the missing key and the cassette dir in the message so the
    operator knows exactly what to re-record.
    """


@dataclass(frozen=True, slots=True)
class CassetteContext:
    """Per-request context the transport uses to derive the cassette key."""

    prompt_version: str
    model_id: str
    case_id: str
    shot_index: int


def compute_cassette_key(
    *,
    prompt_version: str,
    model_id: str,
    case_id: str,
    shot_index: int,
) -> str:
    """sha256-derived 16-char hex key from the four context fields. Stable
    across runs and across machines; sensitive to any input change."""
    payload = json.dumps(
        {
            "prompt_version": prompt_version,
            "model_id": model_id,
            "case_id": case_id,
            "shot_index": shot_index,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


class CassetteTransport(httpx.AsyncBaseTransport):
    """httpx transport that records or replays HTTP exchanges by cassette key."""

    def __init__(
        self,
        *,
        mode: CassetteMode,
        cassette_dir: Path,
        inner_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._mode: CassetteMode = mode
        self._cassette_dir = cassette_dir
        self._cassette_dir.mkdir(parents=True, exist_ok=True)
        # The inner transport forwards record-mode requests to the network.
        # Default is httpx's standard AsyncHTTPTransport. Tests inject a fake
        # to avoid hitting the network.
        self._inner = inner_transport or httpx.AsyncHTTPTransport()
        self._context: CassetteContext | None = None
        self._override_key: str | None = None  # test-only escape hatch

    def set_context(self, ctx: CassetteContext) -> None:
        """Runner calls this before each diagnosis request."""
        self._context = ctx
        self._override_key = None

    def override_next_key(self, key: str) -> None:
        """Test-only — bypass key derivation for the next request.

        Not used by the runner; only by unit tests that need to match a
        hand-written cassette file.
        """
        self._override_key = key

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._context is None and self._override_key is None:
            raise RuntimeError(
                "CassetteTransport.set_context must be called before "
                "handle_async_request — the runner sets context per-request"
            )

        if self._override_key is not None:
            key = self._override_key
        else:
            assert self._context is not None  # narrow for mypy
            key = compute_cassette_key(
                prompt_version=self._context.prompt_version,
                model_id=self._context.model_id,
                case_id=self._context.case_id,
                shot_index=self._context.shot_index,
            )
        path = self._cassette_dir / f"{key}.json"

        if self._mode == "replay":
            if not path.exists():
                raise CassetteMiss(
                    f"no cassette at {path} (key={key}) — re-record via "
                    "`make evals-record` and commit the new cassette"
                )
            return _response_from_cassette(path)

        # record mode: forward to inner, capture, write, return
        response = await self._inner.handle_async_request(request)
        body = await response.aread()
        _write_cassette(path, key=key, request=request, response=response, body=body)
        # Return a fresh response with the captured body (the original stream
        # has been drained by .aread()).
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=body,
            request=request,
        )


# --- helpers ---


def _response_from_cassette(path: Path) -> httpx.Response:
    payload = json.loads(path.read_text())
    body = base64.b64decode(payload["response"]["body_b64"])
    return httpx.Response(
        status_code=payload["response"]["status_code"],
        headers=payload["response"]["headers"],
        content=body,
    )


def _write_cassette(
    path: Path,
    *,
    key: str,
    request: httpx.Request,
    response: httpx.Response,
    body: bytes,
) -> None:
    payload = {
        "key": key,
        "request": {
            "method": request.method,
            "url": str(request.url),
        },
        "response": {
            "status_code": response.status_code,
            "headers": [list(item) for item in response.headers.raw],
            "body_b64": base64.b64encode(body).decode("ascii"),
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
```

### Step 4: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_cassette.py -v --no-cov`
Expected: all 6 tests PASS. If `headers.raw` format trips a test, adapt the cassette JSON shape (the value of `headers` after JSON-roundtrip might be a list of [str, str] or [bytes, bytes] depending on httpx version).

### Step 5: mypy strict

Run: `mypy --strict sentinel/evals/cassette.py tests/unit/evals/test_cassette.py`
Expected: clean.

### Step 6: Wire exports

Add to `sentinel/evals/__init__.py` (alphabetical):

```python
from sentinel.evals.cassette import (
    CassetteContext,
    CassetteMiss,
    CassetteTransport,
    compute_cassette_key,
)
```

Add all 4 to `__all__` (alphabetical).

### Step 7: Commit

```bash
git add sentinel/evals/cassette.py sentinel/evals/__init__.py tests/unit/evals/test_cassette.py
git commit -m "$(cat <<'EOF'
feat(evals): cassette — VCR-style HTTP record/replay for the Anthropic client

httpx transport that records (forward to network + capture) or replays
(read from <16-char-key>.json) HTTP exchanges. Key derived from
sha256(prompt_version, model_id, case_id, shot_index)[:16] — sensitive
to any input change so a prompt edit invalidates cassettes cleanly.
Replay miss raises CassetteMiss with regenerate guidance; no silent
fallback to network.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Full sweep + code review + PR

- [ ] **Step 1:** `make lint && make typecheck && make test && make test-integration` — all green
- [ ] **Step 2:** Invoke `superpowers:requesting-code-review`:
  - Scope: PR 3a of Work Area K (harness pieces)
  - Specific concerns: corpus_loader fail-loud surface area, fetcher_override translation correctness (seed → production type), cassette key stability, the record-mode test's use of an inner-transport fake (cleaner than real network)
- [ ] **Step 3:** Address feedback, re-sweep
- [ ] **Step 4:** Push (`git push -u origin feat/eval-harness-pr3a-harness`) + open PR (`gh pr create`)

PR title: `feat(evals): corpus loader + fetcher override + cassette layer (PR 3a/4 of Work Area K)`

PR body notes:
- This PR is stacked on PR #43 (PR 2 — scoring + stats) — review/merge order matters: PR 2 first, then PR 3a
- Pure harness infrastructure — no runner, no DB writes, no LLM calls
- Why no cassette regeneration in CI: cassettes are committed artifacts; regeneration is a deliberate `make evals-record` PR — same expand-contract discipline as the baseline JSON
- What's next: PR 3b wires lifespan + Makefile + runner + report; PR 3c lands the 10 corpus YAMLs and recorded cassettes
