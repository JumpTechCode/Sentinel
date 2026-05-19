# Phase 5 — Diagnosis agent (Work Area G) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Work Area G end-to-end: aiokafka consumer on `sentinel.incidents` filtered to `incident.enriched` → single-shot Anthropic call with forced tool-use over the persisted `IncidentContext` → schema validation + evidence-citation gate → atomic persistence of the diagnosis row + `incident.diagnosed` outbox event → metric/log/trace coverage. Plus four sibling patches the design surfaced (migration `0004` adds `hallucinated_evidence` + uniq constraint; `hallucinated_evidence_rate` gauge → counters; `DiagnosisRepository` Protocol replaced; no `anthropic` dep change needed).

**Architecture:** New module `sentinel/diagnosis/` with `agent.py` (pure-async `diagnose(incident, context, deps) → PersistedDiagnosis`), `llm_client.py` (only file importing `anthropic`, streaming + forced tool-use + 30s timeout), `prompt.py` + `prompts/v1.md` + `prompts/v1.sha256` (hash-pinned versioned system prompt), `validation.py` (evidence-citation gate — the headline quality signal), `truncation.py` (input token cap, priority-based section drop), `consumer.py` (aiokafka `_handle` filtering `incident.enriched`, two-call context load `incident_repo.get(...)` + `incident_repo.get_enrichment_context(...)`, then agent + atomic save), `persisted.py` (`PersistedDiagnosis` dataclass without the `min_length=1` evidence constraint so 100%-hallucinated cases persist), `deps.py` (DI bundle). Persistence gets `PostgresDiagnosisRepository.save_with_outbox(...)` mirroring the `write_enrichment_context` atomic pattern. Lifespan starts the consumer alongside the existing OutboxDrainer + EnrichmentConsumer.

**Tech Stack:** Python 3.12, asyncio, Pydantic v2, SQLAlchemy 2.x async, asyncpg, alembic, aiokafka, `anthropic>=0.39,<0.40`, OpenTelemetry SDK, structlog + stdlib logging, testcontainers-postgres/-kafka, pytest.

**Spec:** `plans/2026-05-19-diagnosis-agent-design.md` — read the design before starting. All non-obvious decisions and rationale live there.

**Conventions for this repo (do not violate):**
- The user performs all `git commit`/`git push` — Claude must not commit. Tasks end with "stage and pause for review", never with `git commit`. One subagent code review (via `superpowers:requesting-code-review`) runs at the end of the whole plan before the user commits.
- `mypy --strict` and `ruff` are gating; both must be clean before review.
- New env vars go into `Settings`, `.env.example`, and `config/dev.yaml` in the same task that introduces them.
- No `dict[str, Any]` on Pydantic API boundaries. JSONB columns may be `dict[str, Any]` internally; that does not leak into wire types.
- Every external call has a documented timeout. LLM call timeout default 30.0s.
- ADRs go in `docs/adr/`; not required for G (the "single LLM provider, no circuit breaker yet" decision is noted in the design as a deferred ADR — write the placeholder ADR file in Task 22).
- Designs and plans go in `plans/`.

---

## File Structure

### Sibling patches (land in this PR)

| File | Responsibility |
|---|---|
| `migrations/versions/0004_diagnoses_idempotency.py` (new) | Add `hallucinated_evidence` boolean column (NOT NULL, default false) + unique constraint `(incident_id, prompt_version, model)`. Reversible. |
| `sentinel/persistence/models.py` (modify) | Add `hallucinated_evidence` column on `DiagnosisModel`; add the unique constraint to `__table_args__`. |
| `sentinel/observability/metrics.py` (modify) | Remove `hallucinated_evidence_rate` gauge; add five counters (`hallucinated_evidence_total`, `diagnoses_total{status}`, `diagnosis_failures_total{reason}`, `diagnosis_input_truncated_total{section}`, `diagnosis_llm_tokens_total{kind}`, `diagnosis_invalid_events_total`). Update `_NAME_TO_VAR`. |
| `sentinel/persistence/repositories.py` (modify) | Replace `DiagnosisRepository.save(...)` Protocol with `save_with_outbox(...)`. Add concrete `PostgresDiagnosisRepository`. |

### Diagnosis module (Work Area G)

| File | Responsibility |
|---|---|
| `sentinel/diagnosis/__init__.py` | Public exports: `diagnose`, `DiagnosisDeps`, `PersistedDiagnosis`, error classes. |
| `sentinel/diagnosis/persisted.py` | `PersistedDiagnosis` frozen dataclass (no `min_length` constraints; the boundary record between agent and repo). |
| `sentinel/diagnosis/errors.py` | `DiagnosisInvalid`, `LLMTimeout`, `LLMTransport`, `LLMNoToolCall`. |
| `sentinel/diagnosis/prompts/v1.md` | Versioned system prompt — role, rubric, citation rules, tool contract. |
| `sentinel/diagnosis/prompts/v1.sha256` | Baseline SHA-256 hex of v1.md, checked at startup. |
| `sentinel/diagnosis/prompt.py` | `PromptBundle.load(version)`: reads `prompts/<v>.md`, hashes it, compares to baseline; `serialize(incident, ctx) -> str` builds the user message with FetcherStatus headers. |
| `sentinel/diagnosis/truncation.py` | `truncate_for_budget(ctx, *, max_input_tokens)` + `TruncationStats` dataclass. Pure function. |
| `sentinel/diagnosis/validation.py` | `verify_evidence(diagnosis, ctx) -> EvidenceVerdict` (verified vs invented, kind-aware). |
| `sentinel/diagnosis/llm_client.py` | `AnthropicClient` wrapper — only file importing `anthropic`. Streaming + forced tool-use + 30s timeout + one short transport backoff. `LLMResult` dataclass. |
| `sentinel/diagnosis/agent.py` | `diagnose(incident, context, deps) -> PersistedDiagnosis` — truncate → call LLM → validate (twice) → verify evidence → cap confidence on hallucination → assemble `PersistedDiagnosis`. |
| `sentinel/diagnosis/deps.py` | `DiagnosisDeps` frozen dataclass: `llm`, `incident_repo`, `diagnosis_repo`, `prompt`, `max_input_tokens`. |
| `sentinel/diagnosis/consumer.py` | `DiagnosisConsumer` aiokafka consumer (filter `incident.enriched`, load two repos, run agent, save_with_outbox). |

### Observability extension

| File | Responsibility |
|---|---|
| `sentinel/observability/metrics.py` (modify) | New counters listed above. |
| `sentinel/observability/llm_audit.py` (new) | One-line-per-call JSON audit logger writing to `settings.llm_audit_log_path` (existing setting). |

### App wiring + settings

| File | Responsibility |
|---|---|
| `sentinel/api/app.py` (modify) | Construct `DiagnosisDeps`, start `DiagnosisConsumer` task in lifespan; orderly shutdown. |
| `sentinel/config/settings.py` (modify) | New fields: `diagnosis_consumer_enabled: bool = True`, `diagnosis_prompt_version: str = "v1"`, `diagnosis_max_input_tokens: int = 12_000`, `diagnosis_max_output_tokens: int = 2048`, `diagnosis_llm_timeout_seconds: float = 30.0`, `kafka_consumer_group_diagnoser: str = "sentinel-diagnoser"`. |
| `.env.example` (modify) | Document new env-mapped settings. |
| `config/dev.yaml` (modify) | Mirror defaults. |

### ADR placeholder

| File | Responsibility |
|---|---|
| `docs/adr/0003-single-llm-provider-no-breaker.md` (new) | Document the deliberate decision: only one LLM provider, no per-LLM circuit breaker in V1. Revisit when adding a second. |

### Tests

| File | Responsibility |
|---|---|
| `tests/unit/diagnosis/__init__.py` | Package marker. |
| `tests/unit/diagnosis/fakes.py` | `FakeAnthropicClient`, `FakeDiagnosisRepository`, `FakeIncidentRepository`, builders for `IncidentContext` / `IncidentDetailResponse` fixtures. |
| `tests/unit/diagnosis/test_persisted.py` | `PersistedDiagnosis` round-trips through equality, allows empty evidence. |
| `tests/unit/diagnosis/test_prompt.py` | `PromptBundle.load("v1")` succeeds; mismatched hash logs WARN; `serialize(...)` produces expected sections + IDs against a fixed fixture. |
| `tests/unit/diagnosis/test_truncation.py` | Under-budget pass-through; over-budget drop order; preserves ≥1 per non-empty section; deploys section always keeps newest; stats accurate. |
| `tests/unit/diagnosis/test_validation.py` | Verified, invented, wrong-kind cases; all-invented yields empty + flag. |
| `tests/unit/diagnosis/test_llm_client.py` | Timeout raises `LLMTimeout`; 5xx retried once with 0.5s backoff; 429 retried once; no-tool-call raises `LLMNoToolCall`; usage parsed from `MessageStopEvent`. |
| `tests/unit/diagnosis/test_agent.py` | Happy path; schema-invalid retry → success; double schema-invalid → `DiagnosisInvalid`; hallucinated → capped + flagged; timeout bubbles; tokens/cost accounting populated. |
| `tests/unit/diagnosis/test_consumer.py` | Accepts only `incident.enriched`; skips other types silently; missing incident/context counted; happy/duplicate paths; agent exception → metric bumped + offset not committed. |
| `tests/unit/persistence/test_diagnosis_repository.py` | `save_with_outbox` happy path inserts both rows in one txn; duplicate path returns `("duplicate", existing)` and skips outbox insert; outbox payload contains `diagnosis_id`. |
| `tests/unit/observability/test_diagnosis_metrics.py` | Removed gauge no longer registered; new counters present with right labelnames. |
| `tests/unit/observability/test_llm_audit.py` | One JSON line per call with required fields; survives rotation; non-blocking. |
| `tests/integration/diagnosis/__init__.py` | Package marker. |
| `tests/integration/diagnosis/test_migration_round_trip.py` | `alembic upgrade head` then `downgrade -1` cleanly; unique constraint fires on duplicate insert. |
| `tests/integration/diagnosis/test_end_to_end.py` | Real Postgres + real Kafka + `FakeAnthropicClient` recorded fixture: publish `incident.enriched` → diagnosis row + `incident.diagnosed` event appears. |

---

## Task 0: Read the design and confirm starting state

**Files:** none (orientation only).

- [ ] **Step 1: Read the design doc.** `plans/2026-05-19-diagnosis-agent-design.md`. Hold it in mind for every subsequent task — when a code snippet here looks underspecified, the design is the source of truth.

- [ ] **Step 2: Confirm starting state.**

```bash
make lint && make typecheck && make test
```

Expected: green. If not, stop and report — this plan assumes a clean baseline on `main`.

- [ ] **Step 3: Confirm pinned dep.**

```bash
grep -n '"anthropic' pyproject.toml
```

Expected: `anthropic>=0.39,<0.40` already present. No dep change in this plan.

---

## Task 1: Migration `0004_diagnoses_idempotency.py`

**Files:**
- Create: `migrations/versions/0004_diagnoses_idempotency.py`

- [ ] **Step 1: Write the migration.**

```python
"""diagnoses idempotency: hallucinated_evidence column + uniq(incident_id, prompt_version, model)

Revision ID: 0004
Revises: 0003
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "diagnoses",
        sa.Column(
            "hallucinated_evidence",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Drop the server default; future inserts pass the value explicitly.
    op.alter_column("diagnoses", "hallucinated_evidence", server_default=None)
    op.create_unique_constraint(
        "uq_diagnoses_incident_prompt_model",
        "diagnoses",
        ["incident_id", "prompt_version", "model"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_diagnoses_incident_prompt_model", "diagnoses", type_="unique"
    )
    op.drop_column("diagnoses", "hallucinated_evidence")
```

- [ ] **Step 2: Apply against a fresh DB and reverse it.**

```bash
make compose-up
make migrate              # alembic upgrade head — applies 0004
make migrate-down         # alembic downgrade -1 — reverses 0004
make migrate              # back to head
```

Expected: all three commands exit 0; the second prints `Running downgrade 0004 -> 0003`.

- [ ] **Step 3: Stage.**

```bash
git add migrations/versions/0004_diagnoses_idempotency.py
```

---

## Task 2: Update `DiagnosisModel`

**Files:**
- Modify: `sentinel/persistence/models.py` (lines around 110–145, the `DiagnosisModel` class)

- [ ] **Step 1: Add the column and constraint.**

In `DiagnosisModel`, add after `token_usage`:

```python
    hallucinated_evidence: Mapped[bool] = mapped_column(
        Boolean, nullable=False
    )
```

In `__table_args__`, add the unique constraint:

```python
        UniqueConstraint(
            "incident_id", "prompt_version", "model",
            name="uq_diagnoses_incident_prompt_model",
        ),
```

Add the imports `Boolean, UniqueConstraint` from `sqlalchemy` if not already present.

- [ ] **Step 2: Run mypy + tests.**

```bash
make typecheck && make test
```

Expected: green.

- [ ] **Step 3: Stage.**

```bash
git add sentinel/persistence/models.py
```

---

## Task 3: Settings extension

**Files:**
- Modify: `sentinel/config/settings.py`
- Modify: `.env.example`
- Modify: `config/dev.yaml`

- [ ] **Step 1: Add new settings.**

In `sentinel/config/settings.py`, inside the `Settings` class (near the existing `anthropic_*` fields):

```python
    # Diagnosis agent (Work Area G)
    diagnosis_consumer_enabled: bool = True
    diagnosis_prompt_version: str = "v1"
    diagnosis_max_input_tokens: int = 12_000
    diagnosis_max_output_tokens: int = 2048
    diagnosis_llm_timeout_seconds: float = 30.0
    kafka_consumer_group_diagnoser: str = "sentinel-diagnoser"
```

- [ ] **Step 2: Document in `.env.example`.**

Append:

```
# Diagnosis (Work Area G)
DIAGNOSIS_CONSUMER_ENABLED=true
DIAGNOSIS_PROMPT_VERSION=v1
DIAGNOSIS_MAX_INPUT_TOKENS=12000
DIAGNOSIS_MAX_OUTPUT_TOKENS=2048
DIAGNOSIS_LLM_TIMEOUT_SECONDS=30
KAFKA_CONSUMER_GROUP_DIAGNOSER=sentinel-diagnoser
```

- [ ] **Step 3: Mirror in `config/dev.yaml`** under the existing application section. Use the same defaults.

- [ ] **Step 4: Typecheck + tests.**

```bash
make typecheck && make test
```

Expected: green.

- [ ] **Step 5: Stage.**

```bash
git add sentinel/config/settings.py .env.example config/dev.yaml
```

---

## Task 4: Metrics revisions

**Files:**
- Modify: `sentinel/observability/metrics.py`
- Create: `tests/unit/observability/test_diagnosis_metrics.py`

- [ ] **Step 1: Write the failing test.**

```python
# tests/unit/observability/test_diagnosis_metrics.py
"""Diagnosis metric registration — Work Area G."""
from __future__ import annotations

from prometheus_client import REGISTRY


def test_hallucinated_evidence_rate_gauge_is_removed() -> None:
    assert REGISTRY._names_to_collectors.get("sentinel_hallucinated_evidence_rate") is None


def test_new_diagnosis_counters_are_registered() -> None:
    expected = {
        "sentinel_hallucinated_evidence_total",
        "sentinel_diagnoses_total",
        "sentinel_diagnosis_failures_total",
        "sentinel_diagnosis_input_truncated_total",
        "sentinel_diagnosis_llm_tokens_total",
        "sentinel_diagnosis_invalid_events_total",
    }
    registered = set(REGISTRY._names_to_collectors.keys())
    missing = expected - registered
    assert not missing, f"missing diagnosis counters: {missing}"


def test_diagnoses_total_has_status_label() -> None:
    from sentinel.observability.metrics import diagnoses_total
    diagnoses_total.labels(status="new").inc(0)
    diagnoses_total.labels(status="duplicate").inc(0)


def test_diagnosis_failures_total_has_reason_label() -> None:
    from sentinel.observability.metrics import diagnosis_failures_total
    for reason in ("schema", "timeout", "transport", "no_tool_call", "missing_context", "missing_incident"):
        diagnosis_failures_total.labels(reason=reason).inc(0)


def test_diagnosis_input_truncated_total_has_section_label() -> None:
    from sentinel.observability.metrics import diagnosis_input_truncated_total
    diagnosis_input_truncated_total.labels(section="recent_logs").inc(0)


def test_diagnosis_llm_tokens_total_has_kind_label() -> None:
    from sentinel.observability.metrics import diagnosis_llm_tokens_total
    diagnosis_llm_tokens_total.labels(kind="input").inc(0)
    diagnosis_llm_tokens_total.labels(kind="output").inc(0)
```

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/observability/test_diagnosis_metrics.py -v
```

Expected: failure (gauge still registered; counters not yet defined).

- [ ] **Step 3: Edit `sentinel/observability/metrics.py`.**

Remove the `hallucinated_evidence_rate` gauge block. Replace with:

```python
hallucinated_evidence_total = _safe_counter(
    "sentinel_hallucinated_evidence_total",
    "Diagnoses with at least one invented citation",
    [],
)
diagnoses_total = _safe_counter(
    "sentinel_diagnoses_total",
    "Diagnoses persisted, by outcome status",
    ["status"],
)
diagnosis_failures_total = _safe_counter(
    "sentinel_diagnosis_failures_total",
    "Diagnosis attempts that failed before persistence",
    ["reason"],
)
diagnosis_input_truncated_total = _safe_counter(
    "sentinel_diagnosis_input_truncated_total",
    "Per-section count of context items dropped to fit input budget",
    ["section"],
)
diagnosis_llm_tokens_total = _safe_counter(
    "sentinel_diagnosis_llm_tokens_total",
    "Tokens consumed by the diagnosis LLM call",
    ["kind"],
)
diagnosis_invalid_events_total = _safe_counter(
    "sentinel_diagnosis_invalid_events_total",
    "Diagnosis Kafka envelopes that failed JSON/schema parse",
    [],
)
```

Update the `_NAME_TO_VAR` mapping: remove the `"sentinel_hallucinated_evidence_rate"` entry; add the six new ones (`"<metric_name>": "<python_var>"`).

Note: `_safe_counter` is defined with `labelnames: list[str]` (no default). Pass `[]` for unlabeled counters; the helper already supports this.

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/observability/test_diagnosis_metrics.py -v
make typecheck
```

Expected: all green.

- [ ] **Step 5: Stage.**

```bash
git add sentinel/observability/metrics.py tests/unit/observability/test_diagnosis_metrics.py
```

---

## Task 5: `PersistedDiagnosis` dataclass + `errors`

**Files:**
- Create: `sentinel/diagnosis/__init__.py`
- Create: `sentinel/diagnosis/persisted.py`
- Create: `sentinel/diagnosis/errors.py`
- Create: `tests/unit/diagnosis/__init__.py`
- Create: `tests/unit/diagnosis/test_persisted.py`

- [ ] **Step 1: Write the failing test.**

```python
# tests/unit/diagnosis/test_persisted.py
from __future__ import annotations

from decimal import Decimal

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction


def _make(**overrides: object) -> PersistedDiagnosis:
    base: dict[str, object] = dict(
        hypothesis="x",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
        latency_ms=100,
        token_usage={"input": 10, "output": 5},
    )
    base.update(overrides)
    return PersistedDiagnosis(**base)  # type: ignore[arg-type]


def test_persisted_allows_empty_evidence() -> None:
    pd = _make(evidence=[])
    assert pd.evidence == []


def test_persisted_is_frozen() -> None:
    import dataclasses
    pd = _make()
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        pd.hypothesis = "y"  # type: ignore[misc]


def test_persisted_carries_optional_suggested_actions() -> None:
    sa = SuggestedAction(
        description="d", risk="low", rationale="r", requires_human_approval=True
    )
    pd = _make(suggested_actions=[sa])
    assert len(pd.suggested_actions) == 1
```

- [ ] **Step 2: Run — expected FAIL** (module not found).

```bash
pytest tests/unit/diagnosis/test_persisted.py -v
```

- [ ] **Step 3: Implement `errors.py`.**

```python
# sentinel/diagnosis/errors.py
"""Exception types for the diagnosis pipeline."""
from __future__ import annotations


class DiagnosisError(Exception):
    """Base class for diagnosis-layer failures."""


class DiagnosisInvalid(DiagnosisError):
    """LLM output failed schema validation twice in a row."""


class LLMError(DiagnosisError):
    """Base for LLM-transport errors raised by the client wrapper."""


class LLMTimeout(LLMError):
    """LLM call exceeded the configured timeout."""


class LLMTransport(LLMError):
    """LLM call failed with a transport error after the single retry."""


class LLMNoToolCall(LLMError):
    """LLM returned a message that did not include the required tool call."""
```

- [ ] **Step 4: Implement `persisted.py`.**

```python
# sentinel/diagnosis/persisted.py
"""Boundary record between the agent and the repository.

`Diagnosis` (the Pydantic schema) enforces `min_length=1` on evidence — correct
for the wire-level contract with the LLM. The persisted record drops that
constraint because a 100%-hallucinated diagnosis still needs to be written
(with the verified-evidence list empty and the hallucinated_evidence flag set).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction
from sentinel.schemas.enums import CategoryType


@dataclass(frozen=True, slots=True)
class PersistedDiagnosis:
    hypothesis: str
    confidence: Decimal
    reasoning: str
    evidence: list[EvidenceRef]
    suggested_actions: list[SuggestedAction]
    likely_category: CategoryType
    hallucinated_evidence: bool
    model: str
    prompt_version: str
    latency_ms: int
    token_usage: dict[str, Any]
```

- [ ] **Step 5: Implement `__init__.py`.**

```python
# sentinel/diagnosis/__init__.py
"""Diagnosis agent (Work Area G).

Public surface:
- `diagnose(incident, context, deps) -> PersistedDiagnosis`
- `DiagnosisDeps`
- `PersistedDiagnosis`
- Error types: `DiagnosisInvalid`, `LLMTimeout`, `LLMTransport`, `LLMNoToolCall`
"""
from __future__ import annotations

from sentinel.diagnosis.errors import (
    DiagnosisInvalid,
    LLMNoToolCall,
    LLMTimeout,
    LLMTransport,
)
from sentinel.diagnosis.persisted import PersistedDiagnosis

__all__ = [
    "DiagnosisInvalid",
    "LLMNoToolCall",
    "LLMTimeout",
    "LLMTransport",
    "PersistedDiagnosis",
]
```

- [ ] **Step 6: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_persisted.py -v
make typecheck
```

- [ ] **Step 7: Stage.**

```bash
git add sentinel/diagnosis/__init__.py sentinel/diagnosis/persisted.py sentinel/diagnosis/errors.py tests/unit/diagnosis/__init__.py tests/unit/diagnosis/test_persisted.py
```

---

## Task 6: Prompt v1 (system prompt) + hash baseline

**Files:**
- Create: `sentinel/diagnosis/prompts/v1.md`
- Create: `sentinel/diagnosis/prompts/v1.sha256`
- Create: `sentinel/diagnosis/__init__.py` already exists (skip)

- [ ] **Step 1: Write `v1.md`.**

```markdown
You are a senior SRE diagnosis assistant. You produce a single, structured diagnosis from a pre-assembled incident context. You do not fetch additional data. You do not speculate beyond the evidence you are given.

# Reasoning rules

1. Reason only over the supplied context. Do not invent services, deploys, runbooks, or log lines that are not in the context.
2. Every claim that supports your hypothesis must cite a context ID using the exact format the context uses: `[deploy:<sha>]`, `[similar:<uuid>]`, `[runbook:<uuid>]`, `[log:<idx>]`, `[related:<uuid>]`. Inventing an ID will cause your diagnosis to be flagged and your confidence capped at 0.4.
3. When a context section is marked `status=degraded` or `status=failed`, treat your hypothesis as less supported and lower your confidence accordingly.

# Confidence rubric

- 0.0–0.3 — speculation; no direct evidence in context.
- 0.4–0.6 — plausible hypothesis supported by indirect signals.
- 0.7–0.85 — strong evidence in context, but a causal chain is not directly demonstrated.
- 0.86–1.0 — direct causal link present in the context (e.g., a deploy whose diff matches the failure mode, plus log lines from that service after the deploy).

# Output contract

You MUST submit your diagnosis by calling the `submit_diagnosis` tool exactly once. Do not return free-text output. The tool input must conform to the schema you were given. Fields:

- `hypothesis` — one or two sentences naming the most likely root cause.
- `confidence` — float 0.0–1.0, following the rubric above.
- `reasoning` — short paragraph showing the chain of evidence; cite IDs inline.
- `evidence` — list of `{kind, id, note}` entries. Each `id` must appear in the context.
- `suggested_actions` — list of `{description, command?, risk, rationale, requires_human_approval}` entries; `requires_human_approval` defaults to true.
- `likely_category` — one of `deploy | config | dependency | capacity | data | external`.
```

- [ ] **Step 2: Generate and write `v1.sha256`.**

```bash
shasum -a 256 sentinel/diagnosis/prompts/v1.md | awk '{print $1}' > sentinel/diagnosis/prompts/v1.sha256
```

Expected: a 64-char hex line written to the file. Inspect the file to confirm.

- [ ] **Step 3: Stage.**

```bash
git add sentinel/diagnosis/prompts/v1.md sentinel/diagnosis/prompts/v1.sha256
```

---

## Task 7: `PromptBundle` loader + tests

**Files:**
- Create: `sentinel/diagnosis/prompt.py` (loader only in this task; `serialize` lands in Task 9)
- Create: `tests/unit/diagnosis/test_prompt.py`

- [ ] **Step 1: Write the failing loader test.**

```python
# tests/unit/diagnosis/test_prompt.py
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from sentinel.diagnosis.prompt import PromptBundle


def test_load_v1_succeeds() -> None:
    bundle = PromptBundle.load("v1")
    assert bundle.version == "v1"
    assert len(bundle.sha256_hex) == 64
    assert "submit_diagnosis" in bundle.system_text


def test_load_unknown_version_raises() -> None:
    with pytest.raises(FileNotFoundError):
        PromptBundle.load("v99")


def test_hash_mismatch_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Point the loader at a temp dir where the baseline doesn't match.
    monkeypatch.setattr("sentinel.diagnosis.prompt._PROMPTS_DIR", tmp_path)
    (tmp_path / "vX.md").write_text("hello")
    (tmp_path / "vX.sha256").write_text("0" * 64 + "\n")

    with caplog.at_level(logging.WARNING, logger="sentinel.diagnosis.prompt"):
        bundle = PromptBundle.load("vX")

    assert bundle.system_text == "hello"
    assert any("prompt_sha_mismatch" in rec.message for rec in caplog.records)
```

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_prompt.py -v
```

- [ ] **Step 3: Implement the loader.**

```python
# sentinel/diagnosis/prompt.py
"""Prompt versioning: load + hash-pin the system prompt.

`serialize(incident, ctx)` is also defined here (added in Task 9).
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

_LOG = logging.getLogger("sentinel.diagnosis.prompt")

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


@dataclass(frozen=True, slots=True)
class PromptBundle:
    version: str
    system_text: str
    sha256_hex: str

    @classmethod
    def load(cls, version: str) -> "PromptBundle":
        md = _PROMPTS_DIR / f"{version}.md"
        baseline_file = _PROMPTS_DIR / f"{version}.sha256"
        text = md.read_text()                              # raises FileNotFoundError if missing
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if baseline_file.exists():
            baseline = baseline_file.read_text().strip()
            if baseline != digest:
                _LOG.warning(
                    "prompt_sha_mismatch",
                    extra={
                        "version": version,
                        "expected": baseline,
                        "actual": digest,
                    },
                )
        else:
            _LOG.warning(
                "prompt_sha_baseline_missing", extra={"version": version}
            )
        return cls(version=version, system_text=text, sha256_hex=digest)
```

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_prompt.py -v
make typecheck
```

- [ ] **Step 5: Stage.**

```bash
git add sentinel/diagnosis/prompt.py tests/unit/diagnosis/test_prompt.py
```

---

## Task 8: `truncation.py` + tests (TDD)

**Files:**
- Create: `sentinel/diagnosis/truncation.py`
- Create: `tests/unit/diagnosis/test_truncation.py`
- Create: `tests/unit/diagnosis/fakes.py` (helper builders)

- [ ] **Step 1: Write the `fakes.py` builder helpers.**

```python
# tests/unit/diagnosis/fakes.py
"""Test-only builders and fakes for the diagnosis module."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


def fetcher_ok(data: list[Any]) -> FetcherResult[Any]:
    return FetcherResult(status="ok", data=data, error=None, fetched_at=datetime.now(UTC))


def fetcher_degraded(reason: str = "not_configured") -> FetcherResult[Any]:
    return FetcherResult(status="degraded", data=[], error=reason, fetched_at=datetime.now(UTC))


def make_deploy(sha: str = "abc123", service: str = "payments-api") -> DeployItem:
    return DeployItem(
        id=f"deploy:{sha}",
        service=service,
        sha=sha,
        pr_number=4421,
        pr_title="Switch idempotency store to Redis Cluster",
        pr_diff_summary="src/idempotency/{store.py,redis_cluster.py}",
        deployed_at=datetime.now(UTC),
        deployed_by="alice",
    )


def make_log(idx: int) -> LogLine:
    return LogLine(
        id=f"log:{idx}",
        timestamp=datetime.now(UTC),
        level="error",
        service="payments-api",
        message=f"failed to acquire connection (attempt {idx})",
    )


def make_similar(uid: UUID | None = None, cosine: float = 0.8) -> SimilarIncidentItem:
    uid = uid or uuid4()
    return SimilarIncidentItem(
        id=f"similar:{uid}",
        title="5xx after Redis client upgrade",
        root_cause="connection pool exhausted on TLS rotation",
        remediation="downgrade redis-py to 5.0.x",
        cosine_similarity=cosine,
    )


def make_runbook(uid: UUID | None = None, cosine: float = 0.7) -> RunbookItem:
    uid = uid or uuid4()
    return RunbookItem(
        id=f"runbook:{uid}",
        title="Redis pool exhaustion",
        content="1. Check pool metrics. 2. Restart with larger pool. 3. Validate TLS rotation.",
        cosine_similarity=cosine,
    )


def make_related(uid: UUID | None = None) -> RelatedAlertItem:
    uid = uid or uuid4()
    return RelatedAlertItem(
        id=f"related:{uid}",
        service="payments-api",
        severity="SEV2",
        title="elevated latency",
        opened_at=datetime.now(UTC),
    )


def make_context(
    *,
    deploys: list[DeployItem] | None = None,
    similar: list[SimilarIncidentItem] | None = None,
    runbooks: list[RunbookItem] | None = None,
    logs: list[LogLine] | None = None,
    related: list[RelatedAlertItem] | None = None,
    active: list[RelatedAlertItem] | None = None,
    incident_id: UUID | None = None,
) -> IncidentContext:
    return IncidentContext(
        incident_id=incident_id or uuid4(),
        assembled_at=datetime.now(UTC),
        recent_deploys=fetcher_ok(deploys or []),
        related_alerts=fetcher_ok(related or []),
        similar_incidents=fetcher_ok(similar or []),
        runbooks=fetcher_ok(runbooks or []),
        recent_logs=fetcher_ok(logs or []),
        active_alerts=fetcher_ok(active or []),
    )
```

- [ ] **Step 2: Write the failing truncation tests.**

```python
# tests/unit/diagnosis/test_truncation.py
from __future__ import annotations

from sentinel.diagnosis.truncation import truncate_for_budget
from tests.unit.diagnosis.fakes import (
    make_context, make_deploy, make_log, make_runbook, make_similar,
)


def test_under_budget_passes_through() -> None:
    ctx = make_context(deploys=[make_deploy()], logs=[make_log(0)])
    out, stats = truncate_for_budget(ctx, max_input_tokens=100_000)
    assert out == ctx
    assert stats.total_dropped() == 0


def test_over_budget_drops_logs_first_then_related_then_runbooks() -> None:
    # 100 logs is way over a small budget; nothing else gets dropped first.
    ctx = make_context(
        deploys=[make_deploy()],
        logs=[make_log(i) for i in range(100)],
        runbooks=[make_runbook()],
        similar=[make_similar()],
    )
    out, stats = truncate_for_budget(ctx, max_input_tokens=500)
    # Logs section is the highest-priority drop target.
    assert stats.dropped["recent_logs"] > 0
    # Some logs may survive — but at least one should (preserve ≥1 per non-empty section).
    assert len(out.recent_logs.data) >= 1


def test_preserves_at_least_one_per_nonempty_section() -> None:
    ctx = make_context(
        deploys=[make_deploy("d1"), make_deploy("d2"), make_deploy("d3")],
        logs=[make_log(i) for i in range(50)],
        runbooks=[make_runbook(), make_runbook()],
        similar=[make_similar(), make_similar()],
    )
    out, _ = truncate_for_budget(ctx, max_input_tokens=200)
    for section in (out.recent_deploys, out.recent_logs, out.runbooks, out.similar_incidents):
        if section.status == "ok" and section.data:
            assert len(section.data) >= 1


def test_deploys_section_always_keeps_newest() -> None:
    from datetime import UTC, datetime, timedelta
    now = datetime.now(UTC)
    older = make_deploy("old").model_copy(update={"deployed_at": now - timedelta(hours=1)})
    newer = make_deploy("new").model_copy(update={"deployed_at": now})
    ctx = make_context(
        deploys=[older, newer],
        logs=[make_log(i) for i in range(500)],
    )
    out, _ = truncate_for_budget(ctx, max_input_tokens=200)
    deploy_ids = [d.id for d in out.recent_deploys.data]
    assert "deploy:new" in deploy_ids


def test_stats_to_dict_carries_per_section_counts() -> None:
    ctx = make_context(
        logs=[make_log(i) for i in range(100)],
        deploys=[make_deploy()],
    )
    _, stats = truncate_for_budget(ctx, max_input_tokens=200)
    as_dict = stats.to_dict()
    assert "recent_logs" in as_dict
    assert as_dict["recent_logs"] == stats.dropped.get("recent_logs", 0)
```

- [ ] **Step 3: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_truncation.py -v
```

- [ ] **Step 4: Implement `truncation.py`.**

```python
# sentinel/diagnosis/truncation.py
"""Context truncation under an input-token budget.

Pure function — no I/O. The token count is a `len(text) // 4` heuristic with a
10% safety margin. The anthropic SDK at 0.39 does not expose a local tokenizer;
we observe truncation stats and tighten the margin if reality drifts.

Drop priority (highest first):
  recent_logs         — oldest first
  related_alerts      — oldest first
  active_alerts       — oldest first
  runbooks            — lowest cosine first
  similar_incidents   — lowest cosine first
  recent_deploys      — oldest first, but always preserve the newest

A section that had ≥1 item retains ≥1 after truncation, so the model sees the
section exists.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sentinel.schemas.context import (
    FetcherResult,
    IncidentContext,
)

_SAFETY_MARGIN = 0.10
_CHARS_PER_TOKEN = 4

_DROP_ORDER: tuple[str, ...] = (
    "recent_logs",
    "related_alerts",
    "active_alerts",
    "runbooks",
    "similar_incidents",
    "recent_deploys",
)


@dataclass(frozen=True, slots=True)
class TruncationStats:
    dropped: dict[str, int] = field(default_factory=dict)

    def total_dropped(self) -> int:
        return sum(self.dropped.values())

    def to_dict(self) -> dict[str, int]:
        return dict(self.dropped)


def _estimate_tokens(ctx: IncidentContext) -> int:
    # Cheap and stable: serialize the model and use char/4 + margin.
    blob = ctx.model_dump_json()
    return int(len(blob) / _CHARS_PER_TOKEN * (1 + _SAFETY_MARGIN))


def _sort_for_section(name: str, items: list[Any]) -> list[Any]:
    """Return items sorted so the LAST entry is the first to drop."""
    if name in {"recent_logs", "related_alerts", "active_alerts"}:
        # Newest first — drop from the tail (oldest end).
        return sorted(items, key=lambda x: x.timestamp if hasattr(x, "timestamp") else x.opened_at, reverse=True)
    if name in {"runbooks", "similar_incidents"}:
        # Highest cosine first — drop from the tail (lowest cosine end).
        return sorted(items, key=lambda x: x.cosine_similarity, reverse=True)
    if name == "recent_deploys":
        # Newest first — drop from the tail (oldest end). Always keep [0].
        return sorted(items, key=lambda x: x.deployed_at, reverse=True)
    return items


def _drop_one(ctx_dict: dict[str, Any], section: str) -> bool:
    """Drop the lowest-priority item from `section`. Returns True if dropped."""
    fr = ctx_dict[section]
    data = list(fr["data"])
    if len(data) <= 1:
        return False
    # data is already sorted highest-priority first; drop the tail.
    data.pop()
    ctx_dict[section] = {**fr, "data": data}
    return True


def truncate_for_budget(
    ctx: IncidentContext, *, max_input_tokens: int
) -> tuple[IncidentContext, TruncationStats]:
    estimated = _estimate_tokens(ctx)
    if estimated <= max_input_tokens:
        return ctx, TruncationStats()

    # Work on a dict copy so we can rebuild via model_validate at the end.
    blob = json.loads(ctx.model_dump_json())
    # Pre-sort each section so .pop() drops the lowest-priority item.
    for section in _DROP_ORDER:
        items = blob[section]["data"]
        sorted_items = _sort_for_section(section, list(_iter_models(ctx, section)))
        blob[section]["data"] = [m.model_dump(mode="json") for m in sorted_items]

    dropped: dict[str, int] = {}
    # Round-robin one drop per section per pass, until under budget or nothing more droppable.
    while True:
        progress = False
        for section in _DROP_ORDER:
            if _drop_one(blob, section):
                dropped[section] = dropped.get(section, 0) + 1
                progress = True
                new_ctx = IncidentContext.model_validate(blob)
                if _estimate_tokens(new_ctx) <= max_input_tokens:
                    return new_ctx, TruncationStats(dropped=dropped)
        if not progress:
            # Cannot drop further (every section is at its floor of 1 item).
            return IncidentContext.model_validate(blob), TruncationStats(dropped=dropped)


def _iter_models(ctx: IncidentContext, section: str) -> list[Any]:
    fr: FetcherResult[Any] = getattr(ctx, section)
    return list(fr.data)
```

- [ ] **Step 5: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_truncation.py -v
make typecheck
```

- [ ] **Step 6: Stage.**

```bash
git add sentinel/diagnosis/truncation.py tests/unit/diagnosis/fakes.py tests/unit/diagnosis/test_truncation.py
```

---

## Task 9: `validation.py` (evidence-citation gate) + tests (TDD)

**Files:**
- Create: `sentinel/diagnosis/validation.py`
- Create: `tests/unit/diagnosis/test_validation.py`

- [ ] **Step 1: Write the failing tests.**

```python
# tests/unit/diagnosis/test_validation.py
from __future__ import annotations

from uuid import uuid4

from sentinel.diagnosis.validation import verify_evidence
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction
from tests.unit.diagnosis.fakes import (
    make_context, make_deploy, make_log, make_runbook, make_similar, make_related,
)


def _diag(*refs: EvidenceRef) -> Diagnosis:
    return Diagnosis(
        hypothesis="h", confidence=0.7, reasoning="r",
        evidence=list(refs) or [EvidenceRef(kind="deploy", id="deploy:placeholder", note="n")],
        suggested_actions=[],
        likely_category="deploy",
    )


def test_all_verified_returns_no_hallucination() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    diag = _diag(EvidenceRef(kind="deploy", id="deploy:abc", note="touched code"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is False
    assert verdict.invented == []
    assert len(verdict.verified) == 1


def test_invented_id_is_flagged() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    diag = _diag(EvidenceRef(kind="deploy", id="deploy:never_existed", note="?"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.invented) == 1
    assert verdict.verified == []


def test_wrong_kind_counts_as_invented_even_when_id_exists_under_another_kind() -> None:
    log = make_log(0)
    ctx = make_context(logs=[log])
    diag = _diag(EvidenceRef(kind="deploy", id="log:0", note="wrong-kind ref"))
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.invented) == 1


def test_active_and_related_alerts_share_the_related_kind() -> None:
    related = make_related()
    active  = make_related()
    ctx = make_context(related=[related], active=[active])
    diag = _diag(
        EvidenceRef(kind="related_alert", id=related.id, note="r"),
        EvidenceRef(kind="related_alert", id=active.id,  note="a"),
    )
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is False
    assert len(verdict.verified) == 2


def test_mixed_some_verified_some_invented() -> None:
    deploy = make_deploy("abc")
    similar = make_similar()
    ctx = make_context(deploys=[deploy], similar=[similar])
    diag = _diag(
        EvidenceRef(kind="deploy",            id=deploy.id,  note="ok"),
        EvidenceRef(kind="similar_incident",  id="similar:nope", note="invented"),
    )
    verdict = verify_evidence(diag, ctx)
    assert verdict.hallucinated is True
    assert len(verdict.verified) == 1
    assert len(verdict.invented) == 1
```

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_validation.py -v
```

- [ ] **Step 3: Implement `validation.py`.**

```python
# sentinel/diagnosis/validation.py
"""Evidence-citation gate — the headline quality signal.

For every EvidenceRef in the diagnosis, check that its `id` appears in the
context **under its declared `kind`**. Wrong-kind references count as invented.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef


@dataclass(frozen=True, slots=True)
class EvidenceVerdict:
    verified: list[EvidenceRef]
    invented: list[EvidenceRef]

    @property
    def hallucinated(self) -> bool:
        return bool(self.invented)


def _index_context(ctx: IncidentContext) -> dict[str, set[str]]:
    """Bucket of {kind: {id, ...}} for fast membership tests.

    Note `related_alert` aggregates both related_alerts AND active_alerts.
    """
    return {
        "deploy":           {d.id for d in ctx.recent_deploys.data},
        "similar_incident": {s.id for s in ctx.similar_incidents.data},
        "runbook":          {r.id for r in ctx.runbooks.data},
        "log":              {l.id for l in ctx.recent_logs.data},
        "related_alert":    (
            {r.id for r in ctx.related_alerts.data}
            | {a.id for a in ctx.active_alerts.data}
        ),
    }


def verify_evidence(diagnosis: Diagnosis, context: IncidentContext) -> EvidenceVerdict:
    index = _index_context(context)
    verified: list[EvidenceRef] = []
    invented: list[EvidenceRef] = []
    for ref in diagnosis.evidence:
        bucket = index.get(ref.kind, set())
        (verified if ref.id in bucket else invented).append(ref)
    return EvidenceVerdict(verified=verified, invented=invented)
```

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_validation.py -v
make typecheck
```

- [ ] **Step 5: Stage.**

```bash
git add sentinel/diagnosis/validation.py tests/unit/diagnosis/test_validation.py
```

---

## Task 10: `prompt.serialize(incident, ctx)` + snapshot test

**Files:**
- Modify: `sentinel/diagnosis/prompt.py` (add `serialize`)
- Modify: `tests/unit/diagnosis/test_prompt.py` (add serialize tests)

- [ ] **Step 1: Add the failing test.**

Append to `tests/unit/diagnosis/test_prompt.py`:

```python
from datetime import UTC, datetime
from uuid import UUID

from sentinel.diagnosis.prompt import serialize
from sentinel.schemas.api import IncidentDetailResponse  # adjust import to actual location
from tests.unit.diagnosis.fakes import (
    fetcher_degraded, make_context, make_deploy, make_log, make_runbook, make_similar,
)


def _incident(incident_id: UUID) -> IncidentDetailResponse:
    return IncidentDetailResponse(
        id=incident_id,
        service="payments-api",
        severity="SEV1",
        title="Elevated 5xx errors",
        fingerprint="f1",
        source="sentry",
        status="open",
        opened_at=datetime(2026, 5, 19, 13, 42, tzinfo=UTC),
        # other required fields per the actual schema — confirm before running.
    )


def test_serialize_includes_incident_header_and_section_ids() -> None:
    incident_id = UUID("00000000-0000-0000-0000-000000000001")
    ctx = make_context(
        deploys=[make_deploy("abc123")],
        similar=[make_similar()],
        runbooks=[make_runbook()],
        logs=[make_log(0)],
        incident_id=incident_id,
    )
    text = serialize(_incident(incident_id), ctx)
    assert "service:  payments-api" in text
    assert "[deploy:abc123]" in text
    assert "[similar:" in text
    assert "[runbook:" in text
    assert "[log:0]" in text
    assert "status=ok" in text


def test_serialize_marks_degraded_sections() -> None:
    incident_id = UUID("00000000-0000-0000-0000-000000000002")
    ctx = make_context(incident_id=incident_id)
    # Replace similar_incidents with degraded.
    ctx = ctx.model_copy(update={"similar_incidents": fetcher_degraded("not_configured")})
    text = serialize(_incident(incident_id), ctx)
    assert "SIMILAR INCIDENTS" in text
    assert "status=degraded" in text
```

NOTE: Before implementing, run `grep -rn "class IncidentDetailResponse" sentinel/` to confirm the exact import path and the constructor signature. Fix the import and `_incident(...)` fixture if needed.

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_prompt.py -v
```

- [ ] **Step 3: Implement `serialize` (append to `prompt.py`).**

```python
# Append to sentinel/diagnosis/prompt.py

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.schemas.api import IncidentDetailResponse
    from sentinel.schemas.context import IncidentContext


def serialize(incident: "IncidentDetailResponse", ctx: "IncidentContext") -> str:
    parts: list[str] = []
    parts.append("INCIDENT")
    parts.append(f"  service:  {incident.service}")
    parts.append(f"  severity: {incident.severity}")
    parts.append(f'  title:    "{incident.title}"')
    parts.append(f"  opened:   {incident.opened_at.isoformat()}")
    parts.append(f"  fingerprint: {incident.fingerprint}")
    parts.append("")

    _section_deploys(parts, ctx)
    _section_similar(parts, ctx)
    _section_runbooks(parts, ctx)
    _section_related(parts, ctx)
    _section_active(parts, ctx)
    _section_logs(parts, ctx)
    return "\n".join(parts)


def _hdr(title: str, status: str, *, suffix: str = "") -> str:
    extra = f" — {suffix}" if suffix else ""
    return f"{title} — status={status}{extra}"


def _section_deploys(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.recent_deploys
    out.append(_hdr("DEPLOYS (recent)", fr.status))
    for d in fr.data:
        out.append(
            f"  [{d.id}] {d.service} @ {d.deployed_at.isoformat()}"
            + (f" by {d.deployed_by}" if d.deployed_by else "")
        )
        if d.pr_number is not None and d.pr_title:
            out.append(f'    PR #{d.pr_number} "{d.pr_title}"')
        if d.pr_diff_summary:
            out.append(f"    diff: {d.pr_diff_summary}")
    out.append("")


def _section_similar(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.similar_incidents
    out.append(_hdr("SIMILAR INCIDENTS (top by cosine)", fr.status))
    for s in fr.data:
        out.append(f"  [{s.id}] cosine={s.cosine_similarity:.2f}")
        out.append(f'    title: "{s.title}"')
        out.append(f'    root_cause: "{s.root_cause}"')
        out.append(f'    remediation: "{s.remediation}"')
    out.append("")


def _section_runbooks(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.runbooks
    out.append(_hdr("RUNBOOKS", fr.status))
    for r in fr.data:
        out.append(f"  [{r.id}] cosine={r.cosine_similarity:.2f} — {r.title}")
        out.append(f"    {r.content}")
    out.append("")


def _section_related(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.related_alerts
    out.append(_hdr("RELATED ALERTS (recent)", fr.status))
    for a in fr.data:
        out.append(
            f"  [{a.id}] {a.service} {a.severity} \"{a.title}\" opened={a.opened_at.isoformat()}"
        )
    out.append("")


def _section_active(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.active_alerts
    out.append(_hdr("ACTIVE ALERTS (currently open)", fr.status))
    for a in fr.data:
        out.append(
            f"  [{a.id}] {a.service} {a.severity} \"{a.title}\" opened={a.opened_at.isoformat()}"
        )
    out.append("")


def _section_logs(out: list[str], ctx: "IncidentContext") -> None:
    fr = ctx.recent_logs
    out.append(_hdr("RECENT LOGS", fr.status))
    for l in fr.data:
        out.append(f"  [{l.id}] {l.timestamp.isoformat()} {l.level} {l.service} :: {l.message}")
    out.append("")
```

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_prompt.py -v
make typecheck
```

- [ ] **Step 5: Stage.**

```bash
git add sentinel/diagnosis/prompt.py tests/unit/diagnosis/test_prompt.py
```

---

## Task 11: LLM audit logger

**Files:**
- Create: `sentinel/observability/llm_audit.py`
- Create: `tests/unit/observability/test_llm_audit.py`

- [ ] **Step 1: Write the failing test.**

```python
# tests/unit/observability/test_llm_audit.py
from __future__ import annotations

import json
from pathlib import Path

from sentinel.observability.llm_audit import LLMAuditLogger


def test_writes_one_json_line_per_call(tmp_path: Path) -> None:
    path = tmp_path / "audit.log"
    logger = LLMAuditLogger(path)
    logger.record(
        incident_id="i1",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        prompt_sha="abc",
        input_tokens=100,
        output_tokens=20,
        cost_usd="0.001",
        retry_attempt=1,
    )
    logger.record(
        incident_id="i2",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        prompt_sha="abc",
        input_tokens=200,
        output_tokens=40,
        cost_usd="0.002",
        retry_attempt=1,
    )
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    obj0 = json.loads(lines[0])
    assert obj0["incident_id"] == "i1"
    assert "ts" in obj0


def test_creates_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deep" / "audit.log"
    logger = LLMAuditLogger(path)
    logger.record(incident_id="i", model="m", prompt_version="v1", prompt_sha="s",
                  input_tokens=1, output_tokens=1, cost_usd="0", retry_attempt=1)
    assert path.exists()
```

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/observability/test_llm_audit.py -v
```

- [ ] **Step 3: Implement.**

```python
# sentinel/observability/llm_audit.py
"""One-line-per-call JSON audit log for every LLM invocation.

Spec invariant: every LLM call is audit-logged. Writes are line-buffered.
The path is configurable via `Settings.llm_audit_log_path`.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class LLMAuditLogger:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, **fields: Any) -> None:
        payload: dict[str, Any] = {"ts": datetime.now(UTC).isoformat(), **fields}
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
```

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/observability/test_llm_audit.py -v
make typecheck
```

- [ ] **Step 5: Stage.**

```bash
git add sentinel/observability/llm_audit.py tests/unit/observability/test_llm_audit.py
```

---

## Task 12: `AnthropicClient` wrapper + tests

**Files:**
- Create: `sentinel/diagnosis/llm_client.py`
- Modify: `tests/unit/diagnosis/fakes.py` (add `FakeAsyncAnthropic`)
- Create: `tests/unit/diagnosis/test_llm_client.py`

- [ ] **Step 1: Add `FakeAsyncAnthropic` to `fakes.py`.**

Append to `tests/unit/diagnosis/fakes.py`:

```python
# --- LLM fakes ---
from dataclasses import dataclass, field

@dataclass
class _FakeUsage:
    input_tokens: int = 100
    output_tokens: int = 50


@dataclass
class _FakeToolBlock:
    type: str = "tool_use"
    name: str = "submit_diagnosis"
    input: dict[str, Any] = field(default_factory=dict)


@dataclass
class _FakeMessage:
    content: list[Any]
    usage: _FakeUsage
    stop_reason: str = "tool_use"


class _FakeStream:
    """Minimal async context manager mimicking anthropic 0.39 streaming."""
    def __init__(self, final_message: _FakeMessage, raise_on_enter: Exception | None = None) -> None:
        self._final = final_message
        self._raise = raise_on_enter

    async def __aenter__(self) -> "_FakeStream":
        if self._raise:
            raise self._raise
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def get_final_message(self) -> _FakeMessage:
        return self._final


class FakeAsyncAnthropic:
    """Mimics `anthropic.AsyncAnthropic` for the surface we use."""
    def __init__(
        self,
        *,
        tool_input: dict[str, Any] | None = None,
        raise_on_call: Exception | None = None,
        delay_s: float = 0.0,
    ) -> None:
        self.tool_input = tool_input or {}
        self.raise_on_call = raise_on_call
        self.delay_s = delay_s
        self.call_count = 0

        class _Messages:
            def __init__(inner_self) -> None:
                pass

            def stream(inner_self, **_: Any) -> _FakeStream:
                self.call_count += 1
                if self.delay_s:
                    import asyncio
                    # The stream API is sync at construction; sleep happens in __aenter__.
                    async def _sleeper() -> None:
                        await asyncio.sleep(self.delay_s)
                    return _FakeStream(
                        _FakeMessage(
                            content=[_FakeToolBlock(input=self.tool_input)],
                            usage=_FakeUsage(),
                        ),
                        raise_on_enter=None,
                    )
                if self.raise_on_call:
                    return _FakeStream(
                        _FakeMessage(content=[], usage=_FakeUsage()),
                        raise_on_enter=self.raise_on_call,
                    )
                return _FakeStream(
                    _FakeMessage(
                        content=[_FakeToolBlock(input=self.tool_input)],
                        usage=_FakeUsage(),
                    )
                )

        self.messages = _Messages()
```

- [ ] **Step 2: Write the failing client tests.**

```python
# tests/unit/diagnosis/test_llm_client.py
from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr

from sentinel.diagnosis.errors import LLMNoToolCall, LLMTimeout, LLMTransport
from sentinel.diagnosis.llm_client import AnthropicClient
from tests.unit.diagnosis.fakes import (
    FakeAsyncAnthropic, _FakeMessage, _FakeUsage,
)


@pytest.mark.asyncio
async def test_happy_path_returns_tool_input_and_usage() -> None:
    fake = FakeAsyncAnthropic(tool_input={"hypothesis": "h", "confidence": 0.5})
    client = AnthropicClient(
        api_key=SecretStr("k"), model="claude-sonnet-4-5",
        timeout_s=5.0, max_output_tokens=2048, client=fake,
    )
    result = await client.diagnose_call(
        system="s", user="u",
        tool_schema={"name": "submit_diagnosis", "input_schema": {}},
        tool_name="submit_diagnosis",
    )
    assert result.tool_input == {"hypothesis": "h", "confidence": 0.5}
    assert result.input_tokens == 100
    assert result.output_tokens == 50
    assert result.latency_ms >= 0


@pytest.mark.asyncio
async def test_timeout_raises_LLMTimeout() -> None:
    fake = FakeAsyncAnthropic(tool_input={}, delay_s=0.5)
    # Patch the fake stream to actually sleep on __aenter__ — simpler: override via monkeypatch
    # Use a tiny timeout so the fake's no-op enter still triggers wait_for cancellation.
    # Replace stream with one that sleeps:
    import anthropic  # noqa: F401  (ensure the dep is importable)
    client = AnthropicClient(
        api_key=SecretStr("k"), model="claude-sonnet-4-5",
        timeout_s=0.01, max_output_tokens=2048, client=fake,
    )
    # The fake's stream completes immediately; force a real sleep by wrapping:
    async def _slow_call(*_a, **_k):
        await asyncio.sleep(0.5)
    client._stream_once = _slow_call  # type: ignore[assignment]
    with pytest.raises(LLMTimeout):
        await client.diagnose_call(
            system="s", user="u",
            tool_schema={"name": "submit_diagnosis", "input_schema": {}},
            tool_name="submit_diagnosis",
        )


@pytest.mark.asyncio
async def test_no_tool_call_raises_LLMNoToolCall() -> None:
    # Return a message with no tool_use block.
    fake = FakeAsyncAnthropic(tool_input={})
    # Override messages.stream to return a stream with non-tool content.
    class _Bare:
        async def __aenter__(self):  return self
        async def __aexit__(self, *a): return None
        async def get_final_message(self):
            return _FakeMessage(content=[{"type": "text", "text": "hi"}], usage=_FakeUsage())
    fake.messages.stream = lambda **_: _Bare()  # type: ignore[assignment]

    client = AnthropicClient(
        api_key=SecretStr("k"), model="claude-sonnet-4-5",
        timeout_s=5.0, max_output_tokens=2048, client=fake,
    )
    with pytest.raises(LLMNoToolCall):
        await client.diagnose_call(
            system="s", user="u",
            tool_schema={"name": "submit_diagnosis", "input_schema": {}},
            tool_name="submit_diagnosis",
        )


@pytest.mark.asyncio
async def test_transport_5xx_retried_once_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    # The client retries once internally on 5xx/429 with 0.5s backoff.
    import anthropic
    calls = {"n": 0}
    class _Boom:
        async def __aenter__(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise anthropic.APIStatusError("boom", response=None, body=None)  # type: ignore[arg-type]
            return self
        async def __aexit__(self, *a): return None
        async def get_final_message(self):
            return _FakeMessage(
                content=[__import__("tests.unit.diagnosis.fakes", fromlist=["_FakeToolBlock"])._FakeToolBlock(input={})],
                usage=_FakeUsage(),
            )

    fake = FakeAsyncAnthropic(tool_input={})
    fake.messages.stream = lambda **_: _Boom()  # type: ignore[assignment]

    client = AnthropicClient(
        api_key=SecretStr("k"), model="claude-sonnet-4-5",
        timeout_s=5.0, max_output_tokens=2048, client=fake,
    )
    # Speed up backoff for the test
    monkeypatch.setattr("sentinel.diagnosis.llm_client._BACKOFF_S", 0.0)
    result = await client.diagnose_call(
        system="s", user="u",
        tool_schema={"name": "submit_diagnosis", "input_schema": {}},
        tool_name="submit_diagnosis",
    )
    assert calls["n"] == 2
    assert result.tool_input == {}
```

- [ ] **Step 3: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_llm_client.py -v
```

- [ ] **Step 4: Implement `llm_client.py`.**

```python
# sentinel/diagnosis/llm_client.py
"""Anthropic streaming + forced tool-use wrapper. Only file importing `anthropic`.

Timeout: wraps the stream with `asyncio.wait_for(self.timeout_s)`.
Retry policy: one short-backoff retry on `APIStatusError` (5xx/429); no other retries.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import anthropic
from anthropic import AsyncAnthropic
from pydantic import SecretStr

from sentinel.diagnosis.errors import LLMNoToolCall, LLMTimeout, LLMTransport

_BACKOFF_S = 0.5


@dataclass(frozen=True, slots=True)
class LLMResult:
    tool_input: dict[str, Any]
    input_tokens: int
    output_tokens: int
    stop_reason: str
    latency_ms: int


class AnthropicClient:
    def __init__(
        self,
        *,
        api_key: SecretStr,
        model: str,
        timeout_s: float,
        max_output_tokens: int,
        client: AsyncAnthropic | None = None,
    ) -> None:
        self.model = model
        self.timeout_s = timeout_s
        self.max_output_tokens = max_output_tokens
        self._client = client or AsyncAnthropic(api_key=api_key.get_secret_value())

    async def diagnose_call(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
    ) -> LLMResult:
        try:
            return await asyncio.wait_for(
                self._call_with_retry(system=system, user=user,
                                      tool_schema=tool_schema, tool_name=tool_name),
                timeout=self.timeout_s,
            )
        except asyncio.TimeoutError as e:
            raise LLMTimeout(f"LLM call exceeded {self.timeout_s}s") from e

    async def _call_with_retry(
        self, *, system: str, user: str, tool_schema: dict[str, Any], tool_name: str
    ) -> LLMResult:
        try:
            return await self._stream_once(
                system=system, user=user, tool_schema=tool_schema, tool_name=tool_name
            )
        except anthropic.APIStatusError as e:
            await asyncio.sleep(_BACKOFF_S)
            try:
                return await self._stream_once(
                    system=system, user=user, tool_schema=tool_schema, tool_name=tool_name
                )
            except anthropic.APIStatusError as inner:
                raise LLMTransport(str(inner)) from inner

    async def _stream_once(
        self, *, system: str, user: str, tool_schema: dict[str, Any], tool_name: str
    ) -> LLMResult:
        start = time.monotonic()
        stream = self._client.messages.stream(
            model=self.model,
            max_tokens=self.max_output_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": tool_name},
        )
        async with stream as s:
            final = await s.get_final_message()
        latency_ms = int((time.monotonic() - start) * 1000)

        for block in final.content:
            kind = getattr(block, "type", None) or (block.get("type") if isinstance(block, dict) else None)
            name = getattr(block, "name", None) or (block.get("name") if isinstance(block, dict) else None)
            if kind == "tool_use" and name == tool_name:
                tool_input = getattr(block, "input", None) or (block.get("input") if isinstance(block, dict) else None) or {}
                usage = final.usage
                return LLMResult(
                    tool_input=tool_input,
                    input_tokens=getattr(usage, "input_tokens", 0),
                    output_tokens=getattr(usage, "output_tokens", 0),
                    stop_reason=getattr(final, "stop_reason", "tool_use"),
                    latency_ms=latency_ms,
                )
        raise LLMNoToolCall("model did not call the required tool")
```

- [ ] **Step 5: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_llm_client.py -v
make typecheck
```

If `pytest-asyncio` isn't already a dev dep and the existing tests use a different async style, mirror that style — check `tests/unit/enrichment/test_consumer.py` for the pattern this repo uses.

- [ ] **Step 6: Stage.**

```bash
git add sentinel/diagnosis/llm_client.py tests/unit/diagnosis/fakes.py tests/unit/diagnosis/test_llm_client.py
```

---

## Task 13: `DiagnosisDeps` + `agent.diagnose()` + tests

**Files:**
- Create: `sentinel/diagnosis/deps.py`
- Create: `sentinel/diagnosis/agent.py`
- Create: `tests/unit/diagnosis/test_agent.py`

- [ ] **Step 1: Write the failing agent tests.**

```python
# tests/unit/diagnosis/test_agent.py
from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.errors import DiagnosisInvalid, LLMTimeout
from sentinel.diagnosis.llm_client import LLMResult
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import PromptBundle
from tests.unit.diagnosis.fakes import make_context, make_deploy


# Minimal IncidentDetailResponse fixture — adjust constructor if upstream schema differs.
def _incident(uid):
    from datetime import UTC, datetime
    from sentinel.schemas.api import IncidentDetailResponse  # confirm path
    return IncidentDetailResponse(
        id=uid, service="payments-api", severity="SEV1",
        title="t", fingerprint="f", source="sentry", status="open",
        opened_at=datetime.now(UTC),
    )


def _bundle() -> PromptBundle:
    return PromptBundle(version="v1", system_text="sys", sha256_hex="0" * 64)


def _deps(llm, repo_get_returns=None) -> DiagnosisDeps:
    return DiagnosisDeps(
        llm=llm,
        prompt=_bundle(),
        max_input_tokens=12_000,
    )


@pytest.mark.asyncio
async def test_happy_path_returns_persisted_diagnosis() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    tool_input = dict(
        hypothesis="h", confidence=0.7, reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=tool_input, input_tokens=100, output_tokens=50,
        stop_reason="tool_use", latency_ms=123,
    )
    out = await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
    assert isinstance(out, PersistedDiagnosis)
    assert out.confidence == Decimal("0.7")
    assert out.hallucinated_evidence is False
    assert out.latency_ms == 123
    assert out.token_usage["input"] == 100


@pytest.mark.asyncio
async def test_hallucinated_caps_confidence_and_drops_invented() -> None:
    deploy = make_deploy("abc")
    ctx = make_context(deploys=[deploy])
    tool_input = dict(
        hypothesis="h", confidence=0.9, reasoning="r",
        evidence=[{"kind": "deploy", "id": "deploy:never", "note": "?"}],
        suggested_actions=[],
        likely_category="deploy",
    )
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=tool_input, input_tokens=10, output_tokens=5,
        stop_reason="tool_use", latency_ms=10,
    )
    out = await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
    assert out.hallucinated_evidence is True
    assert out.confidence == Decimal("0.4")
    assert out.evidence == []


@pytest.mark.asyncio
async def test_schema_invalid_then_valid_succeeds_after_one_retry() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    bad = dict(hypothesis="", confidence=0.5, reasoning="r",  # empty hypothesis fails min_length
               evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
               suggested_actions=[], likely_category="deploy")
    good = dict(hypothesis="h", confidence=0.5, reasoning="r",
                evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
                suggested_actions=[], likely_category="deploy")
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.side_effect = [
        LLMResult(tool_input=bad,  input_tokens=10, output_tokens=5, stop_reason="tool_use", latency_ms=5),
        LLMResult(tool_input=good, input_tokens=10, output_tokens=5, stop_reason="tool_use", latency_ms=5),
    ]
    out = await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
    assert out.hypothesis == "h"
    assert llm.diagnose_call.await_count == 2


@pytest.mark.asyncio
async def test_double_schema_invalid_raises_DiagnosisInvalid() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    bad = dict(hypothesis="", confidence=0.5, reasoning="r",
               evidence=[{"kind": "deploy", "id": "deploy:abc", "note": "n"}],
               suggested_actions=[], likely_category="deploy")
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.return_value = LLMResult(
        tool_input=bad, input_tokens=10, output_tokens=5, stop_reason="tool_use", latency_ms=5,
    )
    with pytest.raises(DiagnosisInvalid):
        await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))


@pytest.mark.asyncio
async def test_timeout_bubbles_LLMTimeout() -> None:
    ctx = make_context(deploys=[make_deploy("abc")])
    llm = AsyncMock()
    llm.model = "m"
    llm.diagnose_call.side_effect = LLMTimeout("t")
    with pytest.raises(LLMTimeout):
        await diagnose(_incident(ctx.incident_id), ctx, _deps(llm))
```

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_agent.py -v
```

- [ ] **Step 3: Implement `deps.py`.**

```python
# sentinel/diagnosis/deps.py
"""DI bundle for the diagnosis agent."""
from __future__ import annotations

from dataclasses import dataclass

from sentinel.diagnosis.llm_client import AnthropicClient
from sentinel.diagnosis.prompt import PromptBundle


@dataclass(frozen=True, slots=True)
class DiagnosisDeps:
    llm: AnthropicClient
    prompt: PromptBundle
    max_input_tokens: int
```

- [ ] **Step 4: Implement `agent.py`.**

```python
# sentinel/diagnosis/agent.py
"""Single-shot diagnosis agent.

Pure-async, no I/O beyond the LLM call. Consumer/Repo wiring lives in
`sentinel/diagnosis/consumer.py` and `sentinel/persistence/repositories.py`.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.errors import DiagnosisInvalid
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import serialize
from sentinel.diagnosis.truncation import truncate_for_budget
from sentinel.diagnosis.validation import verify_evidence
from sentinel.observability.cost import usd_cost
from sentinel.observability.metrics import (
    diagnosis_confidence,
    diagnosis_input_truncated_total,
    diagnosis_latency_seconds,
    diagnosis_llm_tokens_total,
    hallucinated_evidence_total,
    llm_cost_usd_total,
)
from sentinel.schemas.api import IncidentDetailResponse  # confirm import path
from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis

_LOG = logging.getLogger("sentinel.diagnosis.agent")

_TOOL_NAME = "submit_diagnosis"
_HALLUCINATION_CAP = Decimal("0.4")


def _tool_schema() -> dict[str, Any]:
    return {
        "name": _TOOL_NAME,
        "description": "Submit your structured diagnosis.",
        "input_schema": Diagnosis.model_json_schema(),
    }


async def diagnose(
    incident: IncidentDetailResponse,
    context: IncidentContext,
    deps: DiagnosisDeps,
) -> PersistedDiagnosis:
    ctx, trunc = truncate_for_budget(context, max_input_tokens=deps.max_input_tokens)
    for section, n in trunc.dropped.items():
        diagnosis_input_truncated_total.labels(section=section).inc(n)

    system = deps.prompt.system_text
    user   = serialize(incident, ctx)
    tool_schema = _tool_schema()

    diagnosis_obj: Diagnosis | None = None
    last_result = None
    last_error: ValidationError | None = None
    for attempt in (1, 2):
        last_result = await deps.llm.diagnose_call(
            system=system, user=user,
            tool_schema=tool_schema, tool_name=_TOOL_NAME,
        )
        try:
            diagnosis_obj = Diagnosis.model_validate(last_result.tool_input)
            break
        except ValidationError as e:
            last_error = e
            if attempt == 2:
                raise DiagnosisInvalid(str(e)) from e
            user = (
                user
                + f"\n\nYour previous response was invalid: {e}.\n"
                + f"Return a valid {_TOOL_NAME} call."
            )

    assert diagnosis_obj is not None
    assert last_result is not None

    verdict = verify_evidence(diagnosis_obj, ctx)
    confidence = Decimal(str(diagnosis_obj.confidence))
    if verdict.hallucinated:
        confidence = min(confidence, _HALLUCINATION_CAP)
        hallucinated_evidence_total.inc()

    diagnosis_latency_seconds.observe(last_result.latency_ms / 1000.0)
    diagnosis_confidence.observe(float(confidence))
    diagnosis_llm_tokens_total.labels(kind="input").inc(last_result.input_tokens)
    diagnosis_llm_tokens_total.labels(kind="output").inc(last_result.output_tokens)
    cost = usd_cost(deps.llm.model, last_result.input_tokens, last_result.output_tokens)
    llm_cost_usd_total.labels(model=deps.llm.model).inc(float(cost))

    return PersistedDiagnosis(
        hypothesis=diagnosis_obj.hypothesis,
        confidence=confidence,
        reasoning=diagnosis_obj.reasoning,
        evidence=verdict.verified,
        suggested_actions=list(diagnosis_obj.suggested_actions),
        likely_category=diagnosis_obj.likely_category,
        hallucinated_evidence=verdict.hallucinated,
        model=deps.llm.model,
        prompt_version=deps.prompt.version,
        latency_ms=last_result.latency_ms,
        token_usage={
            "input":  last_result.input_tokens,
            "output": last_result.output_tokens,
            "cost_usd": str(cost),
            "prompt_sha": deps.prompt.sha256_hex,
            "truncated": trunc.to_dict(),
        },
    )
```

- [ ] **Step 5: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_agent.py -v
make typecheck
```

- [ ] **Step 6: Stage.**

```bash
git add sentinel/diagnosis/deps.py sentinel/diagnosis/agent.py tests/unit/diagnosis/test_agent.py
```

---

## Task 14: Replace `DiagnosisRepository` Protocol + `PostgresDiagnosisRepository` + tests

**Files:**
- Modify: `sentinel/persistence/repositories.py` (Protocol section + new concrete class)
- Create: `tests/integration/persistence/test_diagnosis_repository.py`

(Note: real Postgres is needed for the `ON CONFLICT` semantics — testcontainers integration tier, not unit tier. The existing `tests/integration/persistence/conftest.py` provides `pg_dsn`; reuse the `repo` pattern from `test_enrichment_context_repo.py`.)

- [ ] **Step 1: Replace the Protocol stub.**

In `sentinel/persistence/repositories.py`, find the existing `class DiagnosisRepository(Protocol)` block (around line 745) and replace it with:

```python
class DiagnosisRepository(Protocol):
    async def save_with_outbox(
        self,
        *,
        incident_id: UUID,
        record: "PersistedDiagnosis",
        upstream_event_id: UUID,
        outbox_event: OutboxEvent,
    ) -> tuple[UUID, Literal["new", "duplicate"]]: ...
```

Add the necessary imports at the top of the file:

```python
from typing import Literal
from sentinel.diagnosis.persisted import PersistedDiagnosis
```

(Use a `TYPE_CHECKING` guard for `PersistedDiagnosis` if it creates an import cycle; the diagnosis module doesn't import from `persistence/repositories.py`, so a direct import should be fine — confirm.)

- [ ] **Step 2: Write the failing repo tests.**

```python
# tests/integration/persistence/test_diagnosis_repository.py
"""PostgresDiagnosisRepository — integration tests with real Postgres."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.persistence.models import OutboxEventModel
from sentinel.persistence.repositories import (
    OutboxEvent,
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.diagnosis import EvidenceRef
from sentinel.schemas.normalized_alert import NormalizedAlert

# Reuse the alembic + pg_dsn fixtures from this directory's conftest.py
# (provides `pg_dsn`, `migrated`).
from tests.integration.persistence.conftest import _alembic_cfg  # type: ignore[attr-defined]


@pytest.fixture()
async def repos(pg_dsn: str, migrated: None) -> AsyncIterator[tuple[PostgresIncidentRepository, PostgresDiagnosisRepository]]:
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    try:
        yield PostgresIncidentRepository(sf), PostgresDiagnosisRepository(sf)
    finally:
        await engine.dispose()


def _alert(svc: str = "api", title: str = "boom") -> NormalizedAlert:
    return NormalizedAlert(
        source="generic",
        external_id=f"ext-{uuid4().hex[:8]}",
        service=svc,
        severity="SEV2",
        title=title,
        raw_payload={},
        received_at=datetime.now(UTC),
    )


def _record(
    *, model: str = "claude-sonnet-4-5", prompt_version: str = "v1",
    evidence: list[EvidenceRef] | None = None,
) -> PersistedDiagnosis:
    return PersistedDiagnosis(
        hypothesis="h",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=evidence if evidence is not None else [EvidenceRef(kind="deploy", id="deploy:abc", note="n")],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model=model,
        prompt_version=prompt_version,
        latency_ms=100,
        token_usage={"input": 10, "output": 5, "cost_usd": "0.0001", "prompt_sha": "0" * 64, "truncated": {}},
    )


def _outbox(incident_id: str) -> OutboxEvent:
    eid = uuid4()
    return OutboxEvent(
        id=eid,
        topic="sentinel.incidents",
        key=str(incident_id),
        payload={
            "event_id":    str(eid),
            "event":       "incident.diagnosed",
            "incident_id": str(incident_id),
            "ts":          datetime.now(UTC).isoformat(),
        },
        attempts=0,
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_save_with_outbox_inserts_both_atomically(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
    pg_dsn: str,
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(_alert(), fingerprint=f"fp-{uuid4().hex[:8]}")
    outbox_template = _outbox(str(incident_id))
    diag_id, status = await diag_repo.save_with_outbox(
        incident_id=incident_id, record=_record(),
        upstream_event_id=uuid4(), outbox_event=outbox_template,
    )
    assert status == "new"

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            row = (await conn.execute(
                text("SELECT payload FROM outbox_events WHERE id = :id"),
                {"id": outbox_template.id},
            )).one()
            assert row[0]["diagnosis_id"] == str(diag_id)
            assert row[0]["event"] == "incident.diagnosed"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_returns_existing_and_skips_outbox(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
    pg_dsn: str,
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(_alert(), fingerprint=f"fp-{uuid4().hex[:8]}")
    first  = _outbox(str(incident_id))
    second = _outbox(str(incident_id))

    diag_id_1, status_1 = await diag_repo.save_with_outbox(
        incident_id=incident_id, record=_record(),
        upstream_event_id=uuid4(), outbox_event=first,
    )
    diag_id_2, status_2 = await diag_repo.save_with_outbox(
        incident_id=incident_id, record=_record(),
        upstream_event_id=uuid4(), outbox_event=second,
    )
    assert status_1 == "new"
    assert status_2 == "duplicate"
    assert diag_id_1 == diag_id_2

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT count(*) FROM outbox_events WHERE key = :k AND payload->>'event' = 'incident.diagnosed'"),
                {"k": str(incident_id)},
            )).scalar_one()
            assert count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_different_prompt_version_produces_new_row(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(_alert(), fingerprint=f"fp-{uuid4().hex[:8]}")
    await diag_repo.save_with_outbox(
        incident_id=incident_id, record=_record(prompt_version="v1"),
        upstream_event_id=uuid4(), outbox_event=_outbox(str(incident_id)),
    )
    _, status = await diag_repo.save_with_outbox(
        incident_id=incident_id, record=_record(prompt_version="v2"),
        upstream_event_id=uuid4(), outbox_event=_outbox(str(incident_id)),
    )
    assert status == "new"
```

Confirm before running: imports for `NormalizedAlert` and `make_session_factory` (run `grep -rn "class NormalizedAlert\|def make_session_factory" sentinel/`). Adjust paths if needed.

- [ ] **Step 3: Run — expected FAIL.**

```bash
pytest tests/unit/persistence/test_diagnosis_repository.py -v
```

- [ ] **Step 4: Implement `PostgresDiagnosisRepository`.**

Append to `sentinel/persistence/repositories.py` (placed near the existing `PostgresOutboxRepository`):

```python
class PostgresDiagnosisRepository:
    """Concrete `DiagnosisRepository` — INSERT ... ON CONFLICT DO NOTHING + atomic outbox."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_with_outbox(
        self,
        *,
        incident_id: UUID,
        record: PersistedDiagnosis,
        upstream_event_id: UUID,
        outbox_event: OutboxEvent,
    ) -> tuple[UUID, Literal["new", "duplicate"]]:
        from sqlalchemy.dialects.postgresql import insert as pg_insert  # local: avoid surprise reorder
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                pg_insert(DiagnosisModel)
                .values(
                    incident_id=incident_id,
                    hypothesis=record.hypothesis,
                    confidence=record.confidence,
                    reasoning=record.reasoning,
                    evidence=[ref.model_dump(mode="json") for ref in record.evidence],
                    suggested_actions=[a.model_dump(mode="json") for a in record.suggested_actions],
                    likely_category=record.likely_category,
                    hallucinated_evidence=record.hallucinated_evidence,
                    model=record.model,
                    prompt_version=record.prompt_version,
                    latency_ms=record.latency_ms,
                    token_usage=record.token_usage,
                )
                .on_conflict_do_nothing(constraint="uq_diagnoses_incident_prompt_model")
                .returning(DiagnosisModel.id)
            )
            diagnosis_id = result.scalar_one_or_none()
            if diagnosis_id is None:
                existing = await session.execute(
                    select(DiagnosisModel.id).where(
                        DiagnosisModel.incident_id == incident_id,
                        DiagnosisModel.prompt_version == record.prompt_version,
                        DiagnosisModel.model == record.model,
                    )
                )
                return existing.scalar_one(), "duplicate"

            payload = {**outbox_event.payload, "diagnosis_id": str(diagnosis_id)}
            await session.execute(
                pg_insert(OutboxEventModel).values(
                    id=outbox_event.id,
                    topic=outbox_event.topic,
                    key=outbox_event.key,
                    payload=payload,
                    attempts=0,
                    created_at=outbox_event.created_at,
                )
            )
            return diagnosis_id, "new"
```

Add `PostgresDiagnosisRepository` to `__all__`.

- [ ] **Step 5: Run — expected PASS.**

```bash
make compose-up
make test-integration -- tests/integration/persistence/test_diagnosis_repository.py
make typecheck
```

- [ ] **Step 6: Stage.**

```bash
git add sentinel/persistence/repositories.py tests/integration/persistence/test_diagnosis_repository.py
```

---

## Task 15: `DiagnosisConsumer` + tests

**Files:**
- Create: `sentinel/diagnosis/consumer.py`
- Create: `tests/unit/diagnosis/test_consumer.py`

- [ ] **Step 1: Write the failing consumer tests.**

```python
# tests/unit/diagnosis/test_consumer.py
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.persistence.repositories import StoredEnrichmentContext
from tests.unit.diagnosis.fakes import make_context

pytestmark = pytest.mark.asyncio


def _msg(payload: dict | str | None = None, *, raw: bytes | None = None):
    if raw is not None:
        value = raw
    else:
        value = json.dumps(payload).encode() if isinstance(payload, dict) else payload.encode() if isinstance(payload, str) else b""
    return SimpleNamespace(value=value, offset=0, partition=0)


class _FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.committed = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def commit(self):
        self.committed += 1


@pytest.fixture
def deps_factory():
    def _make(*, agent_fn, incident_repo, diagnosis_repo):
        return SimpleNamespace(
            llm=SimpleNamespace(model="m"),
            prompt=PromptBundle(version="v1", system_text="s", sha256_hex="0"*64),
            max_input_tokens=12_000,
            incident_repo=incident_repo,
            diagnosis_repo=diagnosis_repo,
            agent_fn=agent_fn,
        )
    return _make


async def test_handle_accepts_only_incident_enriched(deps_factory) -> None:
    incident_id = uuid4()
    msgs = [
        _msg({"event": "incident.opened", "event_id": str(uuid4()), "incident_id": str(incident_id),
              "fingerprint": "f", "source": "s", "ts": datetime.now(UTC).isoformat()}),
    ]
    incident_repo = AsyncMock()
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeConsumer(msgs),
        deps=deps_factory(agent_fn=agent_fn, incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    await consumer.run()
    agent_fn.assert_not_awaited()
    diagnosis_repo.save_with_outbox.assert_not_awaited()


async def test_missing_incident_increments_and_commits(deps_factory) -> None:
    incident_id = uuid4()
    msgs = [
        _msg({"event": "incident.enriched", "event_id": str(uuid4()), "incident_id": str(incident_id),
              "fingerprint": "f", "source": "s", "ts": datetime.now(UTC).isoformat()}),
    ]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = None
    diagnosis_repo = AsyncMock()
    agent_fn = AsyncMock()
    consumer = DiagnosisConsumer(
        consumer=_FakeConsumer(msgs),
        deps=deps_factory(agent_fn=agent_fn, incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    fake_consumer = consumer._consumer
    await consumer.run()
    agent_fn.assert_not_awaited()
    assert fake_consumer.committed == 1


async def test_happy_path_persists_and_emits(deps_factory) -> None:
    incident_id = uuid4()
    upstream_event_id = uuid4()
    msgs = [
        _msg({"event": "incident.enriched", "event_id": str(upstream_event_id),
              "incident_id": str(incident_id), "fingerprint": "f", "source": "s",
              "ts": datetime.now(UTC).isoformat()}),
    ]
    incident_repo = AsyncMock()
    incident_repo.get.return_value = SimpleNamespace(
        id=incident_id, service="payments-api", severity="SEV1",
        title="t", fingerprint="f", source="sentry", status="open",
        opened_at=datetime.now(UTC),
    )
    ctx = make_context(incident_id=incident_id)
    incident_repo.get_enrichment_context.return_value = StoredEnrichmentContext(
        context=ctx, assembled_at=ctx.assembled_at, version=1, last_event_id=uuid4(),
    )
    diagnosis_repo = AsyncMock()
    diagnosis_repo.save_with_outbox.return_value = (uuid4(), "new")
    agent_fn = AsyncMock()
    agent_fn.return_value = PersistedDiagnosis(
        hypothesis="h", confidence=__import__("decimal").Decimal("0.5"), reasoning="r",
        evidence=[], suggested_actions=[], likely_category="deploy",
        hallucinated_evidence=False, model="m", prompt_version="v1",
        latency_ms=10, token_usage={},
    )

    consumer = DiagnosisConsumer(
        consumer=_FakeConsumer(msgs),
        deps=deps_factory(agent_fn=agent_fn, incident_repo=incident_repo, diagnosis_repo=diagnosis_repo),
        agent_fn=agent_fn,
    )
    fake_consumer = consumer._consumer
    await consumer.run()
    agent_fn.assert_awaited_once()
    diagnosis_repo.save_with_outbox.assert_awaited_once()
    assert fake_consumer.committed == 1
```

NOTE: The `DiagnosisDeps` real dataclass doesn't carry `incident_repo`/`diagnosis_repo`/`agent_fn` — the consumer takes those separately to keep the agent's deps pure. The test fixture uses `SimpleNamespace` to dodge typing in tests; the real signature lands in Step 3 below.

- [ ] **Step 2: Run — expected FAIL.**

```bash
pytest tests/unit/diagnosis/test_consumer.py -v
```

- [ ] **Step 3: Implement `consumer.py`.**

```python
# sentinel/diagnosis/consumer.py
"""aiokafka consumer for `incident.enriched` events.

Same shape as EnrichmentConsumer (deliberate — repeating a known-good pattern).
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any, Callable, Coroutine
from uuid import UUID, uuid4

from pydantic import ValidationError

from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.errors import DiagnosisInvalid, LLMError
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.observability.metrics import (
    diagnoses_total,
    diagnosis_failures_total,
    diagnosis_invalid_events_total,
)
from sentinel.persistence.repositories import (
    DiagnosisRepository,
    IncidentRepository,
    OutboxEvent,
)
from sentinel.schemas.enrichment_event import IncidentEvent
from sentinel.schemas.api import IncidentDetailResponse  # confirm import path

_LOG = logging.getLogger("sentinel.diagnosis.consumer")

_ACCEPTED_EVENTS = frozenset({"incident.enriched"})

DiagnoseFn = Callable[
    [IncidentDetailResponse, "Any", DiagnosisDeps],
    Coroutine[Any, Any, PersistedDiagnosis],
]


class DiagnosisConsumer:
    def __init__(
        self,
        *,
        consumer: Any,
        deps: Any,                       # carries llm/prompt/repos/agent_fn — see Note
        agent_fn: DiagnoseFn,
        topic: str = "sentinel.incidents",
        downstream_topic: str = "sentinel.incidents",
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._agent = agent_fn
        self._topic = topic
        self._downstream_topic = downstream_topic
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop_event.is_set():
                break
            try:
                committed = await self._handle(msg)
            except Exception:
                _LOG.exception(
                    "diagnoser_event_failed",
                    extra={
                        "offset": getattr(msg, "offset", None),
                        "partition": getattr(msg, "partition", None),
                    },
                )
                diagnosis_failures_total.labels(reason="exception").inc()
                continue   # do NOT commit — let Kafka redeliver
            if committed:
                await self._consumer.commit()

    async def _handle(self, msg: Any) -> bool:
        """Returns True iff the offset should be committed."""
        try:
            payload = json.loads(msg.value)
        except ValueError:
            _LOG.error("diagnoser_invalid_envelope", extra={"offset": getattr(msg, "offset", None)})
            diagnosis_invalid_events_total.inc()
            return True

        event_type = payload.get("event") if isinstance(payload, dict) else None
        if event_type not in _ACCEPTED_EVENTS:
            _LOG.debug("diagnoser_skip_event_type", extra={"event": event_type})
            return True

        try:
            envelope = IncidentEvent.model_validate(payload)
        except ValidationError:
            _LOG.error("diagnoser_invalid_envelope", extra={"offset": getattr(msg, "offset", None)})
            diagnosis_invalid_events_total.inc()
            return True

        incident = await self._deps.incident_repo.get(envelope.incident_id)
        if incident is None:
            _LOG.warning("diagnoser_missing_incident", extra={"incident_id": str(envelope.incident_id)})
            diagnosis_failures_total.labels(reason="missing_incident").inc()
            return True

        stored = await self._deps.incident_repo.get_enrichment_context(envelope.incident_id)
        if stored is None:
            _LOG.warning("diagnoser_missing_context", extra={"incident_id": str(envelope.incident_id)})
            diagnosis_failures_total.labels(reason="missing_context").inc()
            return True

        try:
            record = await self._agent(incident, stored.context, self._deps)
        except DiagnosisInvalid as e:
            _LOG.warning("diagnoser_schema_invalid", extra={"error": str(e)[:200]})
            diagnosis_failures_total.labels(reason="schema").inc()
            return True
        except LLMError as e:
            _LOG.error("diagnoser_llm_error", extra={"error": type(e).__name__, "msg": str(e)[:200]})
            reason = {
                "LLMTimeout": "timeout",
                "LLMTransport": "transport",
                "LLMNoToolCall": "no_tool_call",
            }.get(type(e).__name__, "transport")
            diagnosis_failures_total.labels(reason=reason).inc()
            return False  # let Kafka redeliver

        outbox_id = uuid4()
        outbox_event = OutboxEvent(
            id=outbox_id,
            topic=self._downstream_topic,
            key=str(envelope.incident_id),
            payload={
                "event_id":    str(outbox_id),
                "event":       "incident.diagnosed",
                "incident_id": str(envelope.incident_id),
                "ts":          datetime.now(UTC).isoformat(),
            },
            attempts=0,
            created_at=datetime.now(UTC),
        )
        diagnosis_id, status = await self._deps.diagnosis_repo.save_with_outbox(
            incident_id=envelope.incident_id,
            record=record,
            upstream_event_id=envelope.event_id,
            outbox_event=outbox_event,
        )
        diagnoses_total.labels(status=status).inc()
        _LOG.info(
            "diagnoser_completed",
            extra={
                "incident_id":   str(envelope.incident_id),
                "diagnosis_id":  str(diagnosis_id),
                "status":        status,
                "confidence":    str(record.confidence),
                "hallucinated":  record.hallucinated_evidence,
                "latency_ms":    record.latency_ms,
            },
        )
        return True
```

NOTE on the deps shape: the consumer needs `incident_repo`, `diagnosis_repo` (and uses `prompt`/`llm`/`max_input_tokens` indirectly via the agent). To avoid making `DiagnosisDeps` carry repository references (which leaks I/O into the agent's surface), the consumer takes a **wider** deps object. Define a `ConsumerDeps` dataclass adjacent to `DiagnosisDeps`:

Add to `sentinel/diagnosis/deps.py`:

```python
from sentinel.persistence.repositories import DiagnosisRepository, IncidentRepository


@dataclass(frozen=True, slots=True)
class ConsumerDeps:
    llm: AnthropicClient
    prompt: PromptBundle
    max_input_tokens: int
    incident_repo: IncidentRepository
    diagnosis_repo: DiagnosisRepository

    def agent_deps(self) -> "DiagnosisDeps":
        return DiagnosisDeps(
            llm=self.llm, prompt=self.prompt, max_input_tokens=self.max_input_tokens,
        )
```

Update the consumer to type `deps: ConsumerDeps` and call `self._agent(incident, ctx, self._deps.agent_deps())`. Adjust the tests' `SimpleNamespace` shim accordingly or import `ConsumerDeps` and use it directly.

- [ ] **Step 4: Run — expected PASS.**

```bash
pytest tests/unit/diagnosis/test_consumer.py -v
make typecheck
```

- [ ] **Step 5: Stage.**

```bash
git add sentinel/diagnosis/consumer.py sentinel/diagnosis/deps.py tests/unit/diagnosis/test_consumer.py
```

---

## Task 16: Lifespan wiring + worker registration

**Files:**
- Modify: `sentinel/api/app.py`

- [ ] **Step 1: Read the current lifespan.**

```bash
sed -n '40,130p' sentinel/api/app.py
```

Familiarize yourself with how `EnrichmentConsumer` is constructed and its task tracked. The diagnosis consumer mirrors that pattern.

- [ ] **Step 2: Add imports.**

At the top of `sentinel/api/app.py`:

```python
from sentinel.diagnosis.agent import diagnose as diagnose_fn
from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.deps import ConsumerDeps
from sentinel.diagnosis.llm_client import AnthropicClient
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.persistence.repositories import PostgresDiagnosisRepository
```

- [ ] **Step 3: Construct and start the consumer in `lifespan`.**

Inside the lifespan body, after the EnrichmentConsumer is started, add (guarded by the new setting):

```python
diagnosis_task: asyncio.Task[None] | None = None
if settings.diagnosis_consumer_enabled:
    diag_kafka_consumer = AIOKafkaConsumer(
        "sentinel.incidents",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_diagnoser,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await diag_kafka_consumer.start()

    llm_client = AnthropicClient(
        api_key=settings.anthropic_api_key,
        model=settings.anthropic_model,
        timeout_s=settings.diagnosis_llm_timeout_seconds,
        max_output_tokens=settings.diagnosis_max_output_tokens,
    )
    prompt_bundle = PromptBundle.load(settings.diagnosis_prompt_version)
    diagnosis_repo = PostgresDiagnosisRepository(session_factory)  # use the same session_factory the enrichment consumer uses

    consumer_deps = ConsumerDeps(
        llm=llm_client,
        prompt=prompt_bundle,
        max_input_tokens=settings.diagnosis_max_input_tokens,
        incident_repo=incident_repo,                                 # same instance as enrichment
        diagnosis_repo=diagnosis_repo,
    )
    diagnoser = DiagnosisConsumer(
        consumer=diag_kafka_consumer,
        deps=consumer_deps,
        agent_fn=diagnose_fn,
    )
    diagnosis_task = asyncio.create_task(diagnoser.run(), name="diagnosis-consumer")
```

In the shutdown block, mirror the enrichment shutdown pattern (call `diagnoser.stop()`; await the task with a 5s timeout; stop the Kafka consumer in a finally clause; log on failure).

- [ ] **Step 4: Typecheck + tests.**

```bash
make typecheck && make test
```

Expected: green. (Lifespan wiring is exercised via `make test-integration` in Task 18.)

- [ ] **Step 5: Stage.**

```bash
git add sentinel/api/app.py
```

---

## Task 17: ADR placeholder

**Files:**
- Create: `docs/adr/0003-single-llm-provider-no-breaker.md`

- [ ] **Step 1: Write the ADR.**

```markdown
# ADR 0003 — Single LLM provider, no per-LLM circuit breaker (V1)

## Status

Accepted, 2026-05-19. Revisit when a second LLM provider is added.

## Context

The spec's runtime invariant 1 says "every external call has a timeout and a circuit breaker." Diagnosis is an external call: it talks to Anthropic. In V1, Anthropic is the only LLM provider — and is so by design (spec §non-goals).

A circuit breaker between Sentinel and Anthropic, in V1, would do one useful thing (fail fast during a sustained Anthropic outage) and one less-useful thing (delay recovery once Anthropic comes back, while we wait for cooldown). With one provider and no fallback, the breaker mostly converts "the LLM is down" into "diagnosis is down faster" — both are total failures.

## Decision

V1 ships diagnosis with:

- A hard 30s timeout per call (`asyncio.wait_for`).
- One short-backoff retry inside the client on HTTP 5xx/429.
- No per-LLM circuit breaker.
- Failures fall through to Kafka redelivery (the consumer does not commit on `LLMTimeout` / `LLMTransport`).

When a second LLM provider lands (none planned in V1), introduce a breaker at that point and wire fallback. Per-provider breakers will be one of the first things added when that happens.

## Consequences

- During an Anthropic outage, Kafka will retry diagnosis events. Eventually they hit the DLQ. Operators see `sentinel_diagnosis_failures_total{reason="timeout"|"transport"}` rising and have to mute the incident pipeline or wait.
- This is acceptable for V1 — Sentinel is suggest-only, and a diagnosis delay does not block remediation.
- The omission is intentional and tracked here so future reviewers don't have to re-derive the reasoning.
```

- [ ] **Step 2: Stage.**

```bash
git add docs/adr/0003-single-llm-provider-no-breaker.md
```

---

## Task 18: Integration test — migration round-trip + uniq fires

**Files:**
- Create: `tests/integration/diagnosis/__init__.py`
- Create: `tests/integration/diagnosis/test_migration_round_trip.py`

- [ ] **Step 1: Write the test.**

```python
# tests/integration/diagnosis/test_migration_round_trip.py
"""Migration 0004 — upgrade/downgrade round-trip + unique constraint fires."""
from __future__ import annotations

import subprocess

import pytest

pytestmark = pytest.mark.integration


def _alembic(*args: str) -> None:
    subprocess.run(["alembic", *args], check=True)


def test_upgrade_then_downgrade_round_trip() -> None:
    _alembic("upgrade", "head")
    _alembic("downgrade", "-1")
    _alembic("upgrade", "head")


@pytest.mark.asyncio
async def test_unique_constraint_fires_on_duplicate(pg_dsn: str) -> None:
    """Insert two diagnoses with the same (incident_id, prompt_version, model) — the second raises."""
    from datetime import UTC, datetime
    from uuid import uuid4
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import create_async_engine

    from sentinel.persistence.session import make_session_factory
    from sentinel.persistence.repositories import PostgresIncidentRepository
    from sentinel.schemas.normalized_alert import NormalizedAlert

    engine = create_async_engine(pg_dsn)
    try:
        sf = make_session_factory(engine)
        incident_repo = PostgresIncidentRepository(sf)
        alert = NormalizedAlert(
            source="generic", external_id=f"ext-{uuid4().hex[:8]}",
            service="api", severity="SEV2", title="boom",
            raw_payload={}, received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint=f"fp-{uuid4().hex[:8]}")

        async def _insert_one() -> None:
            async with sf() as s, s.begin():
                await s.execute(text("""
                    INSERT INTO diagnoses
                      (incident_id, hypothesis, confidence, reasoning, evidence,
                       suggested_actions, likely_category, model, prompt_version,
                       latency_ms, token_usage, hallucinated_evidence)
                    VALUES
                      (:iid, 'h', 0.5, 'r', '[]'::jsonb, '[]'::jsonb,
                       'deploy', 'claude-sonnet-4-5', 'v1', 1, '{}'::jsonb, false)
                """), {"iid": incident_id})

        await _insert_one()
        with pytest.raises(IntegrityError):
            await _insert_one()
    finally:
        await engine.dispose()
```

- [ ] **Step 2: Run.**

```bash
make compose-up
make test-integration -- -k "migration_round_trip"
```

Expected: green (after filling in the fixture helpers).

- [ ] **Step 3: Stage.**

```bash
git add tests/integration/diagnosis/__init__.py tests/integration/diagnosis/test_migration_round_trip.py
```

---

## Task 19: Integration test — end-to-end (Kafka → diagnosis row → outbox event)

**Files:**
- Create: `tests/integration/diagnosis/test_end_to_end.py`
- Create: `tests/integration/diagnosis/fixtures/anthropic_response_ok.json` (recorded tool input)

- [ ] **Step 1: Record the Anthropic fixture.**

The fixture is the tool input we want the `FakeAnthropicClient` to return. Hand-author it to match a known-good context fixture:

```json
{
  "hypothesis": "Recent deploy to payments-api changed the idempotency store backend, exhausting Redis connections during normal traffic.",
  "confidence": 0.75,
  "reasoning": "[deploy:abc123] changed the idempotency store from local Redis to Redis Cluster. [log:0] shows connection acquisition failures starting immediately after the deploy at 13:38Z. [similar:...] documents an identical failure mode after a prior Redis client change.",
  "evidence": [
    {"kind": "deploy",           "id": "deploy:abc123", "note": "the change that introduced the new backend"},
    {"kind": "log",              "id": "log:0",         "note": "post-deploy connection acquire failures"},
    {"kind": "similar_incident", "id": "similar:<UUID>", "note": "prior incident with the same root cause"}
  ],
  "suggested_actions": [
    {
      "description": "Roll back the deploy and re-test with the previous idempotency backend.",
      "command": "git revert abc123 && deploy payments-api",
      "risk": "medium",
      "rationale": "Restores the prior known-good state while a forward fix is prepared.",
      "requires_human_approval": true
    }
  ],
  "likely_category": "deploy"
}
```

Save to `tests/integration/diagnosis/fixtures/anthropic_response_ok.json`. Replace `similar:<UUID>` after the test seeds the similar incident — the test will substitute it dynamically.

- [ ] **Step 2: Write the test.**

The fixtures `pg_dsn` and `kafka_brokers` are reused from the enrichment integration conftest. The shape mirrors `tests/integration/enrichment/test_end_to_end.py`:

```python
# tests/integration/diagnosis/test_end_to_end.py
"""End-to-end: publish incident.enriched → diagnosis row + incident.diagnosed outbox event."""
from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.deps import ConsumerDeps
from sentinel.diagnosis.llm_client import LLMResult
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.persistence.models import DiagnosisModel, OutboxEventModel
from sentinel.persistence.repositories import (
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.context import IncidentContext, FetcherResult

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parent / "fixtures" / "anthropic_response_ok.json"


class _FakeAnthropicClient:
    """Stand-in for AnthropicClient — returns a pre-recorded tool input."""
    model = "claude-sonnet-4-5"

    def __init__(self, tool_input: dict[str, Any]) -> None:
        self._tool_input = tool_input

    async def diagnose_call(self, **_: Any) -> LLMResult:
        return LLMResult(
            tool_input=self._tool_input,
            input_tokens=200, output_tokens=80,
            stop_reason="tool_use", latency_ms=42,
        )


def _empty_ctx(incident_id, assembled_at) -> IncidentContext:
    fr_ok = lambda: FetcherResult(status="ok", data=[], error=None, fetched_at=assembled_at)
    return IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        recent_deploys=fr_ok(), related_alerts=fr_ok(),
        similar_incidents=fr_ok(), runbooks=fr_ok(),
        recent_logs=fr_ok(), active_alerts=fr_ok(),
    )


@pytest.mark.asyncio
async def test_end_to_end_publish_enriched_emits_diagnosed(
    pg_dsn: str, kafka_brokers: str,
) -> None:
    topic = "sentinel.incidents"
    engine = create_async_engine(pg_dsn)
    try:
        sf = make_session_factory(engine)
        incident_repo = PostgresIncidentRepository(sf)
        diagnosis_repo = PostgresDiagnosisRepository(sf)

        # Seed incident + persisted enrichment context.
        alert = NormalizedAlert(
            source="generic", external_id=f"ext-{uuid4().hex[:8]}",
            service="api", severity="SEV2", title="boom",
            raw_payload={}, received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint=f"fp-{uuid4().hex[:8]}")
        assembled_at = datetime.now(UTC)
        await incident_repo.write_enrichment_context(
            incident_id=incident_id, event_id=uuid4(),
            context=_empty_ctx(incident_id, assembled_at),
            assembled_at=assembled_at, outbox_event=None,
        )

        # Build the consumer with a fake LLM client (no live Anthropic call).
        tool_input = json.loads(_FIXTURE.read_text())
        consumer_kafka = AIOKafkaConsumer(
            topic, bootstrap_servers=kafka_brokers,
            group_id=f"diag-test-{uuid4()}",
            enable_auto_commit=False, auto_offset_reset="earliest",
        )
        await consumer_kafka.start()
        deps = ConsumerDeps(
            llm=_FakeAnthropicClient(tool_input),                # type: ignore[arg-type]
            prompt=PromptBundle(version="v1", system_text="sys", sha256_hex="0" * 64),
            max_input_tokens=12_000,
            incident_repo=incident_repo,
            diagnosis_repo=diagnosis_repo,
        )
        diagnoser = DiagnosisConsumer(
            consumer=consumer_kafka, deps=deps, agent_fn=diagnose,
            topic=topic, downstream_topic=topic,
        )

        # Publish a single incident.enriched event.
        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        try:
            event_id = uuid4()
            await producer.send_and_wait(
                topic,
                key=str(incident_id).encode(),
                value=json.dumps({
                    "event_id":    str(event_id),
                    "event":       "incident.enriched",
                    "incident_id": str(incident_id),
                    "fingerprint": "fp-e2e",
                    "source":      "generic",
                    "ts":          datetime.now(UTC).isoformat(),
                }).encode(),
            )
        finally:
            await producer.stop()

        runner = asyncio.create_task(diagnoser.run())
        try:
            # Poll until the diagnosis row appears (max ~12s).
            diag_id = None
            for _ in range(120):
                async with sf() as s:
                    row = (await s.execute(
                        select(DiagnosisModel).where(DiagnosisModel.incident_id == incident_id)
                    )).scalars().first()
                if row is not None:
                    diag_id = row.id
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("diagnosis row was never written")

            # Outbox row for incident.diagnosed
            async with sf() as s:
                rows = (await s.execute(
                    select(OutboxEventModel).where(OutboxEventModel.topic == topic)
                )).scalars().all()
                diagnosed = [r for r in rows if r.payload.get("event") == "incident.diagnosed"]
                assert len(diagnosed) == 1
                assert diagnosed[0].payload["diagnosis_id"] == str(diag_id)
                assert diagnosed[0].payload["incident_id"] == str(incident_id)
        finally:
            diagnoser.stop()
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
            await consumer_kafka.stop()
    finally:
        await engine.dispose()
```

The fixture in `fixtures/anthropic_response_ok.json` must use evidence IDs that match the seeded `IncidentContext`. Since the test seeds an *empty* context (no deploys, no similar incidents), the recorded tool input should reference no citations — but `Diagnosis.evidence` has `min_length=1`. Two options:
(a) Seed a non-empty context (add one deploy via `PostgresDeployRepository.record`), and have the fixture cite it.
(b) Use the agent's hallucination path: cite a non-existent ID; the agent will flag it and drop it; the row still persists with `hallucinated_evidence=true, evidence=[]`.

Pick (a) — seed a deploy via `PostgresDeployRepository.record(...)` for the same service before calling `write_enrichment_context`, and re-fetch via the fetcher (or shortcut: include the deploy directly in the constructed `IncidentContext`). The fixture references that deploy's id. This gives a `hallucinated_evidence=false` assertion in the test, which is the healthy-path signal we want covered.

- [ ] **Step 3: Implement using existing fixtures.**

Refer to enrichment integration tests for patterns: real-Postgres/real-Kafka fixtures, seed helpers, the polling pattern (`asyncio.wait_for(...)` on a "row exists" predicate).

- [ ] **Step 4: Run.**

```bash
make test-integration -- -k "end_to_end"
```

Expected: green.

- [ ] **Step 5: Stage.**

```bash
git add tests/integration/diagnosis/test_end_to_end.py tests/integration/diagnosis/fixtures/anthropic_response_ok.json
```

---

## Task 20: Smoke — `docker compose up` and curl flow returns a diagnosis

**Files:**
- Modify: `tests/integration/test_smoke.py` (extend, or add adjacent test)

- [ ] **Step 1: Inspect the existing smoke.**

```bash
sed -n '1,80p' tests/integration/test_smoke.py
```

Identify how it boots the stack and POSTs a sample webhook.

- [ ] **Step 2: Extend the smoke to assert a diagnosis row appears.**

After the existing webhook POST + incident-row assertion, add:

```python
# Wait up to 60s for a diagnosis row to appear for the seeded incident.
# The diagnosis consumer pulls real Anthropic by default — for smoke we accept that,
# or set DIAGNOSIS_CONSUMER_ENABLED=false in the smoke env and assert the consumer is wired
# (preferred: keep smoke fully offline; rely on Task 19 for the live-fixture pipeline test).
```

Since we don't want a live LLM call in `make test-integration`, the smoke either (a) keeps `DIAGNOSIS_CONSUMER_ENABLED=false` and asserts no diagnosis row appears, leaving the live pipeline to a dedicated nightly job, or (b) inject the `FakeAnthropicClient` via a settings-driven fixture path.

Pick (a) for V1 simplicity: confirm `DIAGNOSIS_CONSUMER_ENABLED=false` in the smoke env, assert the API still boots, and rely on Task 19's end-to-end test (which already runs in CI's integration tier with the fake client) for actual coverage.

- [ ] **Step 3: Run.**

```bash
make test-integration
```

Expected: green.

- [ ] **Step 4: Stage.**

```bash
git add tests/integration/test_smoke.py
```

---

## Task 21: Lint, typecheck, full test suite

- [ ] **Step 1: Format check.**

```bash
make fmt && make lint
```

Expected: green. Fix anything that ruff flags.

- [ ] **Step 2: Type check.**

```bash
make typecheck
```

Expected: green. `mypy --strict` over the diagnosis module especially — no `Any` leaks on public surfaces; the `Any` placeholders inside `consumer.py` (for the injected aiokafka consumer) are intentional and should be `# type: ignore`-free because `aiokafka` types poorly.

- [ ] **Step 3: Full unit suite.**

```bash
make test
```

Expected: green.

- [ ] **Step 4: Integration suite.**

```bash
make compose-up
make test-integration
```

Expected: green.

- [ ] **Step 5: Coverage spot-check.**

```bash
pytest tests/unit/diagnosis --cov=sentinel/diagnosis --cov-report=term-missing
```

Expected: ≥ 90% line coverage on `sentinel/diagnosis/`. Identify any uncovered branches and write tests for them.

---

## Task 22: Subagent code review + stage final

This is the gating step before the user commits.

- [ ] **Step 1: Stage everything.**

```bash
git add -A
git status
```

Confirm the staged file list matches the plan (no stray edits, no missed files).

- [ ] **Step 2: Run the subagent code review.**

Invoke `superpowers:requesting-code-review` on the staged diff. The review must look at the diagnosis module as a whole (not just individual files) — boundary discipline, error handling, observability coverage, and whether each failure mode in the design's failure-modes table is actually implemented.

Provide the reviewer with:
- The design doc: `plans/2026-05-19-diagnosis-agent-design.md`
- The plan: `plans/2026-05-19-diagnosis-agent-plan.md`
- The full staged diff

- [ ] **Step 3: Address review findings.**

For each finding:
- If it's a real issue, fix it and re-stage. Document the fix briefly in the review thread.
- If it's a false positive or out-of-scope, respond with the rationale and link to the relevant design section.

Re-run `make lint typecheck test` after each batch of fixes.

- [ ] **Step 4: Final pre-commit check.**

```bash
git status
git diff --staged --stat
```

Confirm:
- All files in the plan are staged.
- No unintended files are staged.
- `make lint typecheck test test-integration` is green on the staged tree.

- [ ] **Step 5: Pause for user commit.**

Per repo convention, Claude does not commit. Hand off to the user with a one-line summary:

> "Work Area G staged. {N} files changed, {M} insertions, {K} deletions. Subagent review passed. Ready for your commit."
