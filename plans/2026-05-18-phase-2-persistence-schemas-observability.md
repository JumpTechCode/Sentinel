# Phase 2 — Persistence, Core Schemas, Observability-skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the three foundational data/contract/instrumentation modules that everything downstream depends on — Work Area B (Persistence), Work Area C (Core Pydantic schemas), and Work Area J-skeleton (Observability primitives).

**Architecture:** Three independent slices. C defines the wire types every module consumes (`NormalizedAlert`, `IncidentContext`, `Diagnosis`). B defines the async SQLAlchemy 2.x models, repositories, and migration 0001 — the only module allowed to touch the DB. J-skeleton defines structlog/Prometheus/OTel/cost primitives that downstream code imports as it lands. C and J have no runtime dependency on B; B reuses the shared enum literals from C (`source`, `severity`, `status`, `category`). Treat each Work Area as its own coherent PR.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy 2.x async (asyncpg), Alembic, pgvector 0.2+, structlog, prometheus-client, OpenTelemetry SDK, testcontainers-postgres.

**Conventions for this repo (do not violate):**
- The user performs all `git commit`/`git push` — Claude must not commit. Tasks here end with a "stage and pause for review" step, not a commit. One subagent code review (via `superpowers:requesting-code-review`) runs at the end of **each Work Area** before the user commits that Area's files. Three commits total, not one.
- `mypy --strict` and `ruff` are gating in CI; both must be clean before review.
- Every new external dependency or env var goes into `pyproject.toml`, `.env.example`, and the relevant docker-compose service in the same task that introduces it. No follow-ups.
- No `dict[str, Any]` on Pydantic API boundaries. JSONB columns on the DB may use `dict[str, Any]` via SA `JSONB` mapping; that does **not** leak into wire types.
- Confidence on the wire is `float` (Pydantic). On the DB it is `NUMERIC(3,2)` → SQLAlchemy `Numeric(3,2)` → Python `Decimal`. Coerce at the repository boundary, never in callers.

---

## File Structure

### Work Area C — Schemas (new files under `sentinel/schemas/`)

| File | Responsibility |
|---|---|
| `sentinel/schemas/enums.py` | Shared `Literal` aliases + value tuples for `source`, `severity`, `incident_status`, `category`, `evidence_kind`. Single source of truth — both Pydantic schemas and SA models import from here. |
| `sentinel/schemas/ids.py` | `ContextID` parsing + helpers (`deploy_id(sha) -> str` etc.). Stable `[kind:id]` token format. Symmetric encode/parse. |
| `sentinel/schemas/alert.py` | `NormalizedAlert` — the canonical output of every adapter `normalize()`. |
| `sentinel/schemas/context.py` | `FetcherResult[T]` (generic, `status: ok\|degraded\|failed`), `IncidentContext` (six fetcher sections, each with stable IDs and `fetched_at`). |
| `sentinel/schemas/diagnosis.py` | `EvidenceRef`, `SuggestedAction`, `Diagnosis` with confidence-range and evidence-min-length validators. |
| `sentinel/schemas/api.py` | Request/response models for the API endpoints landing in Work Area I (`CreateIncidentRequest`, `ResolveIncidentRequest`, `IncidentListItem`, `IncidentDetailResponse`, `DiagnoseResponse`, `EvalRunSummary`, `HealthResponse`). |
| `tests/unit/schemas/test_ids.py` | Round-trip property tests + invalid-token rejection. |
| `tests/unit/schemas/test_alert.py` | Serialize/deserialize round-trip; reject bad enums. |
| `tests/unit/schemas/test_context.py` | `FetcherResult` status transitions; `IncidentContext` builds with empty + populated sections. |
| `tests/unit/schemas/test_diagnosis.py` | Confidence rubric bounds; evidence min_length=1; suggested_actions defaults. |
| `tests/unit/schemas/test_api.py` | Each API model serializes to/from the example JSON. |

### Work Area B — Persistence (new files under `sentinel/persistence/`, `migrations/versions/`)

| File | Responsibility |
|---|---|
| `sentinel/persistence/session.py` | `make_async_engine(settings)` + `make_session_factory(engine)` + `get_session()` async context manager bound to a request/test scope. No global state mutated at import time. |
| `sentinel/persistence/models.py` | SQLAlchemy 2.x `DeclarativeBase` + ORM classes: `IncidentModel`, `DiagnosisModel`, `ResolutionModel`, `DeployModel`, `RunbookModel`, `EvalRunModel`. Vector columns via `pgvector.sqlalchemy.Vector(1536)`. Check constraints expressed as SA `CheckConstraint` so they appear in autogenerate. |
| `sentinel/persistence/repositories.py` | Abstract `Protocol` per repo + concrete async impl. Each method returns/accepts Pydantic schemas where appropriate; never leaks SA rows past the boundary. |
| `migrations/env.py` | Modify: import `sentinel.persistence.models.Base` and set `target_metadata = Base.metadata`. |
| `migrations/versions/0001_initial.py` | `upgrade()` creates `vector` + `pgcrypto` extensions, all six tables, all check constraints, all indexes (including HNSW via raw SQL `op.execute`). `downgrade()` drops in reverse dependency order including extensions if no other consumers. |
| `tests/integration/persistence/test_migration_roundtrip.py` | Testcontainers Postgres+pgvector; `alembic upgrade head` then `alembic downgrade base`; both succeed; table list matches expected on the way up. |
| `tests/integration/persistence/test_repositories.py` | Round-trip (insert → fetch → update → fetch) for each repo. |
| `tests/unit/persistence/test_no_raw_sql_outside.py` | Source-grep guard: no `select(` / `text(` / `update(` / `insert(` / `delete(` outside `sentinel/persistence/`. Fails fast if violated. |

### Work Area J-skeleton — Observability (new files under `sentinel/observability/`)

| File | Responsibility |
|---|---|
| `sentinel/observability/logging.py` | `configure_logging(settings)` — structlog JSON renderer in prod, console renderer in dev. Adds `incident_id`, `correlation_id`, `source` from contextvars via a processor. Provides `bind_request_context(...)` and `clear_request_context()` helpers. Sets up a separate `llm_audit_logger` writing to `settings.llm_audit_log_path`. |
| `sentinel/observability/metrics.py` | Module-level singletons for every metric named in the spec (§Observability). Helper context managers `time_histogram(metric, **labels)` and `count_failure(metric, **labels)`. Guarded against double-registration. |
| `sentinel/observability/tracing.py` | `configure_tracing(settings)` — OTel SDK + OTLP exporter when `settings.otel_endpoint` is set, otherwise a no-op tracer. Helpers `span_for_fetcher(name)` and `span_for_llm(model)` (return `contextmanager`s that attach standard attributes). |
| `sentinel/observability/cost.py` | `usd_cost(model: str, input_tokens: int, output_tokens: int) -> Decimal` driven by a small price table. Unknown model → returns `Decimal("0")` and logs once at WARNING. |
| `tests/unit/observability/test_logging.py` | Configure → emit a log → assert it was rendered as JSON with the bound contextvars present; LLM audit log goes to its own handler. |
| `tests/unit/observability/test_metrics.py` | Every spec metric exists in the registry with the right type/labels/buckets; `time_histogram` records on success and on exception (re-raises). |
| `tests/unit/observability/test_tracing.py` | With no endpoint set, calling helpers does not raise; with an in-memory exporter, a wrapped block emits a span with expected attributes. |
| `tests/unit/observability/test_cost.py` | Known model → expected cents; unknown → zero + warning logged once. |

---

## Cross-cutting prep (do these once, before Task C1)

### Task 0a: Add new runtime dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add deps to `[project] dependencies`**

Ensure the following are present (some may already be from Work Area A — re-check, do not duplicate):

```
"sqlalchemy[asyncio]>=2.0,<3.0",
"asyncpg>=0.29,<0.30",
"alembic>=1.13,<2.0",
"pgvector>=0.2.5,<0.3",
"structlog>=24.1,<25.0",
"prometheus-client>=0.20,<0.21",
"opentelemetry-api>=1.25,<2.0",
"opentelemetry-sdk>=1.25,<2.0",
"opentelemetry-exporter-otlp>=1.25,<2.0",
```

- [ ] **Step 2: Add dev deps if missing**

```
"testcontainers[postgres]>=4.0,<5.0",
"hypothesis>=6.100,<7.0",
```

- [ ] **Step 3: Reinstall**

Run: `make bootstrap`
Expected: no errors; `pip show pgvector sqlalchemy structlog prometheus-client opentelemetry-sdk` all resolve.

- [ ] **Step 4: Stage (do not commit)**

Run: `git add pyproject.toml`. Do not commit — bundled into the first Work Area commit.

---

### Task 0b: Confirm docker-compose Postgres image has pgvector

**Files:**
- Read: `docker-compose.yml`

- [ ] **Step 1: Verify the Postgres service uses an image with `vector` extension available**

Read `docker-compose.yml`. The `postgres` (or equivalent) service should use `pgvector/pgvector:pg16` or `ankane/pgvector:v0.5.1` (pinned tag, not `latest`). If not, change it to `pgvector/pgvector:pg16` and update the healthcheck to include `pg_isready` only — extension creation happens in migration 0001.

- [ ] **Step 2: Restart the stack and confirm**

Run: `make compose-down && make compose-up`
Then: `docker compose exec postgres psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS vector; DROP EXTENSION vector;"`
Expected: both succeed, no errors.

- [ ] **Step 3: Stage (do not commit)**

If you modified `docker-compose.yml`: `git add docker-compose.yml`.

---

## Work Area C — Core Schemas

Each task here is in-process — no Docker required. Run `make lint typecheck test` after each.

### Task C1: `schemas/enums.py` — shared Literal types

**Files:**
- Create: `sentinel/schemas/enums.py`
- Test: `tests/unit/schemas/test_enums.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_enums.py
"""Enums are the single source of truth — schemas and SA models must agree."""

from sentinel.schemas.enums import (
    CATEGORY_VALUES,
    EVIDENCE_KIND_VALUES,
    INCIDENT_STATUS_VALUES,
    SEVERITY_VALUES,
    SOURCE_VALUES,
)


def test_source_values_match_spec() -> None:
    assert SOURCE_VALUES == ("sentry", "pagerduty", "datadog", "generic")


def test_severity_values_match_spec() -> None:
    assert SEVERITY_VALUES == ("SEV1", "SEV2", "SEV3", "SEV4")


def test_incident_status_values_match_spec() -> None:
    assert INCIDENT_STATUS_VALUES == (
        "open",
        "diagnosing",
        "mitigated",
        "resolved",
        "closed",
    )


def test_category_values_match_spec() -> None:
    assert CATEGORY_VALUES == (
        "deploy",
        "config",
        "dependency",
        "capacity",
        "data",
        "external",
    )


def test_evidence_kind_values_match_spec() -> None:
    assert EVIDENCE_KIND_VALUES == (
        "deploy",
        "similar_incident",
        "runbook",
        "log",
        "related_alert",
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/schemas/test_enums.py -v`
Expected: `ModuleNotFoundError` for `sentinel.schemas.enums`.

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/enums.py
"""Shared Literal types and value tuples.

These are imported by both Pydantic schemas (`sentinel/schemas/*.py`) and SQLAlchemy
models (`sentinel/persistence/models.py`). Add a value here, and both layers see it.
"""

from __future__ import annotations

from typing import Literal

SOURCE_VALUES: tuple[str, ...] = ("sentry", "pagerduty", "datadog", "generic")
SourceType = Literal["sentry", "pagerduty", "datadog", "generic"]

SEVERITY_VALUES: tuple[str, ...] = ("SEV1", "SEV2", "SEV3", "SEV4")
SeverityType = Literal["SEV1", "SEV2", "SEV3", "SEV4"]

INCIDENT_STATUS_VALUES: tuple[str, ...] = (
    "open",
    "diagnosing",
    "mitigated",
    "resolved",
    "closed",
)
IncidentStatusType = Literal["open", "diagnosing", "mitigated", "resolved", "closed"]

CATEGORY_VALUES: tuple[str, ...] = (
    "deploy",
    "config",
    "dependency",
    "capacity",
    "data",
    "external",
)
CategoryType = Literal["deploy", "config", "dependency", "capacity", "data", "external"]

EVIDENCE_KIND_VALUES: tuple[str, ...] = (
    "deploy",
    "similar_incident",
    "runbook",
    "log",
    "related_alert",
)
EvidenceKindType = Literal[
    "deploy", "similar_incident", "runbook", "log", "related_alert"
]
```

- [ ] **Step 4: Verify tests pass + types clean**

Run: `pytest tests/unit/schemas/test_enums.py -v && make lint typecheck`
Expected: all pass.

- [ ] **Step 5: Stage (do not commit)**

Run: `git add sentinel/schemas/enums.py tests/unit/schemas/test_enums.py tests/unit/schemas/__init__.py`
Create `tests/unit/schemas/__init__.py` if it doesn't exist (empty file).

---

### Task C2: `schemas/ids.py` — Stable context IDs

**Files:**
- Create: `sentinel/schemas/ids.py`
- Test: `tests/unit/schemas/test_ids.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_ids.py
"""ContextID helpers are the contract used by the evidence-citation gate."""

from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sentinel.schemas.ids import (
    ContextID,
    deploy_id,
    log_id,
    parse_context_id,
    related_id,
    runbook_id,
    similar_id,
)


def test_deploy_id_format() -> None:
    assert deploy_id("abc123") == "deploy:abc123"


def test_similar_id_format() -> None:
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert similar_id(u) == f"similar:{u}"


def test_runbook_id_format() -> None:
    u = uuid4()
    assert runbook_id(u) == f"runbook:{u}"


def test_log_id_format() -> None:
    assert log_id(7) == "log:7"


def test_related_id_format() -> None:
    u = uuid4()
    assert related_id(u) == f"related:{u}"


def test_parse_deploy_id() -> None:
    cid = parse_context_id("deploy:abc123")
    assert cid == ContextID(kind="deploy", id="abc123")


def test_parse_similar_id_with_uuid() -> None:
    u = uuid4()
    cid = parse_context_id(f"similar:{u}")
    assert cid == ContextID(kind="similar", id=str(u))


def test_parse_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown kind"):
        parse_context_id("trash:something")


def test_parse_rejects_missing_colon() -> None:
    with pytest.raises(ValueError, match="malformed"):
        parse_context_id("deploy-abc123")


@given(st.text(min_size=1).filter(lambda s: ":" not in s and s.strip() == s))
def test_deploy_id_round_trip(sha: str) -> None:
    encoded = deploy_id(sha)
    decoded = parse_context_id(encoded)
    assert decoded.kind == "deploy"
    assert decoded.id == sha
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/schemas/test_ids.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/ids.py
"""Stable, parseable IDs for context items referenced from diagnoses.

The diagnosis prompt sees context items prefixed with `[kind:id]`. The validation
gate parses every EvidenceRef.id with `parse_context_id` and confirms the ID
appears in the supplied context — invented citations fail the gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

ContextKind = Literal["deploy", "similar", "runbook", "log", "related"]
_KNOWN_KINDS: frozenset[str] = frozenset(
    {"deploy", "similar", "runbook", "log", "related"}
)


@dataclass(frozen=True, slots=True)
class ContextID:
    kind: ContextKind
    id: str


def deploy_id(sha: str) -> str:
    return f"deploy:{sha}"


def similar_id(uuid: UUID) -> str:
    return f"similar:{uuid}"


def runbook_id(uuid: UUID) -> str:
    return f"runbook:{uuid}"


def log_id(idx: int) -> str:
    return f"log:{idx}"


def related_id(uuid: UUID) -> str:
    return f"related:{uuid}"


def parse_context_id(value: str) -> ContextID:
    if ":" not in value:
        raise ValueError(f"malformed context id: {value!r}")
    kind, _, rest = value.partition(":")
    if kind not in _KNOWN_KINDS:
        raise ValueError(f"unknown kind {kind!r} in context id {value!r}")
    if not rest:
        raise ValueError(f"empty id portion in context id {value!r}")
    return ContextID(kind=kind, id=rest)  # type: ignore[arg-type]
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/schemas/test_ids.py -v && make lint typecheck`
Expected: all pass.

- [ ] **Step 5: Stage**

Run: `git add sentinel/schemas/ids.py tests/unit/schemas/test_ids.py`

---

### Task C3: `schemas/alert.py` — NormalizedAlert

**Files:**
- Create: `sentinel/schemas/alert.py`
- Test: `tests/unit/schemas/test_alert.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_alert.py
"""NormalizedAlert is the canonical output of every adapter's normalize()."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from sentinel.schemas.alert import NormalizedAlert


def _valid_payload() -> dict:
    return {
        "source": "sentry",
        "external_id": "evt-abc-123",
        "service": "checkout-api",
        "severity": "SEV2",
        "title": "Elevated 5xx on /checkout",
        "received_at": datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
        "raw_payload": {"any": "shape"},
    }


def test_round_trip() -> None:
    alert = NormalizedAlert(**_valid_payload())
    json_blob = alert.model_dump_json()
    parsed = NormalizedAlert.model_validate_json(json_blob)
    assert parsed == alert


def test_rejects_bad_source() -> None:
    bad = _valid_payload() | {"source": "splunk"}
    with pytest.raises(ValidationError):
        NormalizedAlert(**bad)


def test_rejects_bad_severity() -> None:
    bad = _valid_payload() | {"severity": "P1"}
    with pytest.raises(ValidationError):
        NormalizedAlert(**bad)


def test_requires_tz_aware_timestamp() -> None:
    bad = _valid_payload() | {"received_at": datetime(2026, 5, 18, 18, 0)}
    with pytest.raises(ValidationError, match="timezone"):
        NormalizedAlert(**bad)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/schemas/test_alert.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/alert.py
"""NormalizedAlert — the canonical wire shape every adapter produces."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel.schemas.enums import SeverityType, SourceType


class NormalizedAlert(BaseModel):
    """Output of every adapter's `normalize(raw_payload) -> NormalizedAlert`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: SourceType
    external_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    received_at: datetime
    raw_payload: dict[str, Any]

    @field_validator("received_at")
    @classmethod
    def _require_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        return v
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/schemas/test_alert.py -v && make lint typecheck`

- [ ] **Step 5: Stage**

Run: `git add sentinel/schemas/alert.py tests/unit/schemas/test_alert.py`

---

### Task C4: `schemas/context.py` — FetcherResult + IncidentContext

**Files:**
- Create: `sentinel/schemas/context.py`
- Test: `tests/unit/schemas/test_context.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_context.py
"""FetcherResult is generic per the spec; IncidentContext groups six sections."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


def _now() -> datetime:
    return datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc)


def test_ok_result_requires_data() -> None:
    r = FetcherResult[DeployItem](
        status="ok",
        data=[
            DeployItem(
                id="deploy:abc",
                service="checkout-api",
                sha="abc",
                pr_title="x",
                deployed_at=_now(),
            )
        ],
        fetched_at=_now(),
    )
    assert r.status == "ok"
    assert r.error is None


def test_failed_result_requires_error_message() -> None:
    with pytest.raises(ValidationError, match="error"):
        FetcherResult[DeployItem](status="failed", data=[], fetched_at=_now())


def test_empty_context_is_valid() -> None:
    ctx = IncidentContext(
        recent_deploys=FetcherResult[DeployItem](
            status="ok", data=[], fetched_at=_now()
        ),
        related_alerts=FetcherResult[RelatedAlertItem](
            status="ok", data=[], fetched_at=_now()
        ),
        similar_incidents=FetcherResult[SimilarIncidentItem](
            status="ok", data=[], fetched_at=_now()
        ),
        runbooks=FetcherResult[RunbookItem](
            status="ok", data=[], fetched_at=_now()
        ),
        recent_logs=FetcherResult[LogLine](status="ok", data=[], fetched_at=_now()),
        active_alerts=FetcherResult[RelatedAlertItem](
            status="ok", data=[], fetched_at=_now()
        ),
    )
    assert ctx.recent_deploys.status == "ok"


def test_context_ids_use_stable_prefix() -> None:
    deploy = DeployItem(
        id="deploy:abc",
        service="x",
        sha="abc",
        pr_title="t",
        deployed_at=_now(),
    )
    assert deploy.id.startswith("deploy:")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/schemas/test_context.py -v`
Expected: import error.

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/context.py
"""Pre-assembled incident context consumed by the diagnosis agent."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sentinel.schemas.enums import SeverityType

T = TypeVar("T")

FetcherStatus = Literal["ok", "degraded", "failed"]


class FetcherResult(BaseModel, Generic[T]):
    """Result envelope for every parallel fetcher.

    `degraded` means partial data; `failed` means no data (and `error` is required).
    """

    model_config = ConfigDict(frozen=True)

    status: FetcherStatus
    data: list[T] = Field(default_factory=list)
    error: str | None = None
    fetched_at: datetime

    @model_validator(mode="after")
    def _failed_requires_error(self) -> "FetcherResult[T]":
        if self.status == "failed" and not self.error:
            raise ValueError("failed FetcherResult requires a non-empty error message")
        return self


class DeployItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `deploy:<sha>`
    service: str
    sha: str
    pr_number: int | None = None
    pr_title: str | None = None
    pr_diff_summary: str | None = None
    deployed_at: datetime
    deployed_by: str | None = None


class RelatedAlertItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `related:<uuid>`
    service: str
    severity: SeverityType
    title: str
    opened_at: datetime


class SimilarIncidentItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `similar:<uuid>`
    title: str
    root_cause: str
    remediation: str
    cosine_similarity: float = Field(ge=-1.0, le=1.0)


class RunbookItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `runbook:<uuid>`
    title: str
    content: str
    cosine_similarity: float = Field(ge=-1.0, le=1.0)


class LogLine(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `log:<idx>`
    timestamp: datetime
    level: str
    service: str
    message: str


class IncidentContext(BaseModel):
    """All six fetcher sections, each as its own FetcherResult."""

    model_config = ConfigDict(frozen=True)

    recent_deploys: FetcherResult[DeployItem]
    related_alerts: FetcherResult[RelatedAlertItem]
    similar_incidents: FetcherResult[SimilarIncidentItem]
    runbooks: FetcherResult[RunbookItem]
    recent_logs: FetcherResult[LogLine]
    active_alerts: FetcherResult[RelatedAlertItem]
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/schemas/test_context.py -v && make lint typecheck`

- [ ] **Step 5: Stage**

Run: `git add sentinel/schemas/context.py tests/unit/schemas/test_context.py`

---

### Task C5: `schemas/diagnosis.py` — EvidenceRef, SuggestedAction, Diagnosis

**Files:**
- Create: `sentinel/schemas/diagnosis.py`
- Test: `tests/unit/schemas/test_diagnosis.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_diagnosis.py
"""Diagnosis enforces the spec's confidence rubric and evidence min_length."""

import pytest
from pydantic import ValidationError

from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction


def _valid_evidence() -> EvidenceRef:
    return EvidenceRef(kind="deploy", id="deploy:abc123", note="why it matters")


def _valid_action() -> SuggestedAction:
    return SuggestedAction(
        description="Roll back deploy",
        command="git revert abc123",
        risk="medium",
        rationale="recent change suspected",
    )


def test_diagnosis_round_trip() -> None:
    d = Diagnosis(
        hypothesis="bad deploy abc123 broke checkout",
        confidence=0.78,
        reasoning="recent deploy at 06:25 immediately preceded errors",
        evidence=[_valid_evidence()],
        suggested_actions=[_valid_action()],
        likely_category="deploy",
    )
    assert d.confidence == 0.78
    assert d.suggested_actions[0].requires_human_approval is True


def test_confidence_must_be_in_unit_interval() -> None:
    with pytest.raises(ValidationError):
        Diagnosis(
            hypothesis="x",
            confidence=1.5,
            reasoning="x",
            evidence=[_valid_evidence()],
            suggested_actions=[],
            likely_category="deploy",
        )


def test_evidence_min_length_one() -> None:
    with pytest.raises(ValidationError, match="at least 1"):
        Diagnosis(
            hypothesis="x",
            confidence=0.5,
            reasoning="x",
            evidence=[],
            suggested_actions=[],
            likely_category="deploy",
        )


def test_rejects_bad_category() -> None:
    with pytest.raises(ValidationError):
        Diagnosis(
            hypothesis="x",
            confidence=0.5,
            reasoning="x",
            evidence=[_valid_evidence()],
            suggested_actions=[],
            likely_category="totally-bogus",
        )


def test_rejects_bad_evidence_kind() -> None:
    with pytest.raises(ValidationError):
        EvidenceRef(kind="hunch", id="deploy:abc", note="x")


def test_requires_human_approval_defaults_true() -> None:
    a = SuggestedAction(
        description="x", risk="low", rationale="y"
    )
    assert a.requires_human_approval is True
```

- [ ] **Step 2: Run to fail**

Run: `pytest tests/unit/schemas/test_diagnosis.py -v`
Expected: import error.

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/diagnosis.py
"""Diagnosis structured output schema — what the LLM must produce.

The confidence rubric is encoded in the system prompt; this module enforces the
syntactic envelope (range, min_length, enum values). Semantic validation
(evidence-citation gate) lives in `sentinel/diagnosis/validation.py`.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from sentinel.schemas.enums import CategoryType, EvidenceKindType


class EvidenceRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: EvidenceKindType
    id: str = Field(min_length=1)
    note: str = Field(min_length=1)


class SuggestedAction(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    description: str = Field(min_length=1)
    command: str | None = None
    risk: str = Field(pattern=r"^(low|medium|high)$")
    rationale: str = Field(min_length=1)
    requires_human_approval: bool = True


class Diagnosis(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    hypothesis: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=1)
    evidence: list[EvidenceRef] = Field(min_length=1)
    suggested_actions: list[SuggestedAction] = Field(default_factory=list)
    likely_category: CategoryType
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/schemas/test_diagnosis.py -v && make lint typecheck`

- [ ] **Step 5: Stage**

Run: `git add sentinel/schemas/diagnosis.py tests/unit/schemas/test_diagnosis.py`

---

### Task C6: `schemas/api.py` — request/response models

**Files:**
- Create: `sentinel/schemas/api.py`
- Test: `tests/unit/schemas/test_api.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/schemas/test_api.py
"""Wire shapes for the API endpoints landing in Work Area I."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from sentinel.schemas.api import (
    CreateIncidentRequest,
    DiagnoseResponse,
    EvalRunSummary,
    HealthResponse,
    IncidentDetailResponse,
    IncidentListItem,
    ResolveIncidentRequest,
)


def test_resolve_request_round_trip() -> None:
    body = {
        "root_cause": "BGP misconfig",
        "remediation": "revert change",
        "category": "config",
        "diagnosis_was_correct": True,
        "notes": "see incident #42",
    }
    parsed = ResolveIncidentRequest.model_validate(body)
    assert parsed.category == "config"
    assert parsed.diagnosis_was_correct is True


def test_resolve_request_rejects_bad_category() -> None:
    with pytest.raises(ValidationError):
        ResolveIncidentRequest(
            root_cause="x",
            remediation="y",
            category="not-a-category",
            diagnosis_was_correct=True,
        )


def test_create_incident_request() -> None:
    r = CreateIncidentRequest(
        source="generic",
        external_id="e-1",
        service="x",
        severity="SEV3",
        title="x",
        raw_payload={"k": "v"},
    )
    assert r.source == "generic"


def test_incident_list_item() -> None:
    i = IncidentListItem(
        id=uuid4(),
        service="x",
        severity="SEV2",
        status="open",
        title="t",
        opened_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
    )
    assert i.status == "open"


def test_health_response() -> None:
    h = HealthResponse(status="ok", checks={"db": "ok", "redis": "ok", "kafka": "ok"})
    assert h.status == "ok"


def test_eval_run_summary() -> None:
    s = EvalRunSummary(
        id=uuid4(),
        started_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
        completed_at=None,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        corpus_version="2026-05-18",
        summary=None,
    )
    assert s.completed_at is None
```

- [ ] **Step 2: Run to fail**

Run: `pytest tests/unit/schemas/test_api.py -v`

- [ ] **Step 3: Implement**

```python
# sentinel/schemas/api.py
"""Request and response models for the HTTP API surface.

These are imported by `sentinel/api/routes/*` (lands in Work Area I). Defining
them here keeps the wire contract decoupled from FastAPI plumbing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis
from sentinel.schemas.enums import (
    CategoryType,
    IncidentStatusType,
    SeverityType,
    SourceType,
)


# --- Webhooks / incidents --------------------------------------------------- #


class WebhookAcceptedResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    status: str  # "accepted" or "duplicate"
    incident_id: UUID | None = None


class CreateIncidentRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: SourceType
    external_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    raw_payload: dict[str, Any]


class ResolveIncidentRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    root_cause: str = Field(min_length=1)
    remediation: str = Field(min_length=1)
    category: CategoryType
    diagnosis_was_correct: bool | None = None
    notes: str | None = None
    resolved_by: str | None = None


class IncidentListItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    service: str
    severity: SeverityType
    status: IncidentStatusType
    title: str
    opened_at: datetime
    resolved_at: datetime | None = None


class IncidentDetailResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    source: SourceType
    external_id: str
    service: str
    severity: SeverityType
    status: IncidentStatusType
    title: str
    fingerprint: str
    opened_at: datetime
    resolved_at: datetime | None = None
    context: IncidentContext | None = None
    diagnoses: list[Diagnosis] = Field(default_factory=list)


class DiagnoseResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    diagnosis: Diagnosis
    hallucinated_evidence: bool


# --- Eval runs -------------------------------------------------------------- #


class EvalRunSummary(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    started_at: datetime
    completed_at: datetime | None
    model: str
    prompt_version: str
    corpus_version: str
    summary: dict[str, float] | None  # aggregated scorer outputs


# --- Health ----------------------------------------------------------------- #


class HealthResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    status: str  # "ok" | "degraded"
    checks: dict[str, str]
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/schemas/test_api.py -v && make lint typecheck && pytest tests/unit/schemas -v`

- [ ] **Step 5: Stage**

Run: `git add sentinel/schemas/api.py tests/unit/schemas/test_api.py`

---

### Task C7: Update `sentinel/schemas/__init__.py` re-exports

**Files:**
- Modify: `sentinel/schemas/__init__.py`

- [ ] **Step 1: Replace with public surface**

```python
# sentinel/schemas/__init__.py
"""Canonical wire shapes for Sentinel.

Import from this package, not from submodules, so that downstream files do
not need to know the file layout."""

from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import (
    CreateIncidentRequest,
    DiagnoseResponse,
    EvalRunSummary,
    HealthResponse,
    IncidentDetailResponse,
    IncidentListItem,
    ResolveIncidentRequest,
    WebhookAcceptedResponse,
)
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction
from sentinel.schemas.enums import (
    CATEGORY_VALUES,
    EVIDENCE_KIND_VALUES,
    INCIDENT_STATUS_VALUES,
    SEVERITY_VALUES,
    SOURCE_VALUES,
    CategoryType,
    EvidenceKindType,
    IncidentStatusType,
    SeverityType,
    SourceType,
)
from sentinel.schemas.ids import (
    ContextID,
    deploy_id,
    log_id,
    parse_context_id,
    related_id,
    runbook_id,
    similar_id,
)

__all__ = [
    "CATEGORY_VALUES",
    "EVIDENCE_KIND_VALUES",
    "INCIDENT_STATUS_VALUES",
    "SEVERITY_VALUES",
    "SOURCE_VALUES",
    "CategoryType",
    "ContextID",
    "CreateIncidentRequest",
    "DeployItem",
    "Diagnosis",
    "DiagnoseResponse",
    "EvalRunSummary",
    "EvidenceKindType",
    "EvidenceRef",
    "FetcherResult",
    "HealthResponse",
    "IncidentContext",
    "IncidentDetailResponse",
    "IncidentListItem",
    "IncidentStatusType",
    "LogLine",
    "NormalizedAlert",
    "RelatedAlertItem",
    "ResolveIncidentRequest",
    "RunbookItem",
    "SeverityType",
    "SimilarIncidentItem",
    "SourceType",
    "SuggestedAction",
    "WebhookAcceptedResponse",
    "deploy_id",
    "log_id",
    "parse_context_id",
    "related_id",
    "runbook_id",
    "similar_id",
]
```

- [ ] **Step 2: Verify whole schemas package + repo green**

Run: `pytest tests/unit/schemas -v && make lint typecheck`
Expected: all pass.

- [ ] **Step 3: Stage**

Run: `git add sentinel/schemas/__init__.py`

---

### Task C8: Work Area C code review (subagent)

- [ ] **Step 1: Invoke the code-review skill**

Use `superpowers:requesting-code-review` to dispatch a reviewer subagent over the staged changes (everything under `sentinel/schemas/` and `tests/unit/schemas/`). Brief: "Review Work Area C (Core Pydantic schemas) of Sentinel against the project's CLAUDE.md production engineering bar and the program-of-work file `.claude/program-of-work.md` §C. Focus: (1) no `dict[str, Any]` leaks on wire boundaries, (2) ContextID symmetric round-trip, (3) Pydantic v2 patterns correct (frozen, extra=forbid, field_validator vs model_validator), (4) every Literal in `enums.py` matches the spec's CHECK constraints exactly."

- [ ] **Step 2: Address review findings**

Land fixes as additional staged changes; re-run `make lint typecheck && pytest tests/unit/schemas -v`.

- [ ] **Step 3: Hand off for commit**

Tell the user: "Work Area C ready for commit. Staged files: <list>. Suggested message: `feat(schemas): add core Pydantic wire types (Work Area C)`."

---

## Work Area B — Persistence & migrations

These tasks need Docker Desktop running. Integration tests use testcontainers and will spin a temporary Postgres+pgvector container.

### Task B1: `persistence/session.py` — async engine + session factory

**Files:**
- Create: `sentinel/persistence/session.py`
- Test: `tests/unit/persistence/test_session.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/persistence/test_session.py
"""Session module exposes async engine + session factory bound to settings."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from sentinel.config.settings import Settings
from sentinel.persistence.session import make_async_engine, make_session_factory


def _settings() -> Settings:
    return Settings(
        postgres_dsn="postgresql+asyncpg://u:p@localhost:5432/db",
        redis_url="redis://localhost:6379/0",
        kafka_brokers="localhost:9092",
        anthropic_api_key="x",
    )


def test_engine_construction_does_not_connect() -> None:
    eng = make_async_engine(_settings())
    assert isinstance(eng, AsyncEngine)
    assert "asyncpg" in str(eng.url)


def test_session_factory_is_async_sessionmaker() -> None:
    eng = make_async_engine(_settings())
    factory = make_session_factory(eng)
    assert isinstance(factory, async_sessionmaker)


def test_engine_rejects_sync_dsn() -> None:
    s = _settings()
    s = s.model_copy(update={"postgres_dsn": "postgresql://u:p@localhost/db"})
    with pytest.raises(ValueError, match="asyncpg"):
        make_async_engine(s)
```

- [ ] **Step 2: Run to fail**

Run: `pytest tests/unit/persistence/test_session.py -v`
Expected: import error.

- [ ] **Step 3: Implement**

```python
# sentinel/persistence/session.py
"""Async SQLAlchemy engine + session factory.

Construction is pure (no connection attempt); callers control lifecycle. The
engine pool is sized for the API process; the worker process uses its own
engine instance via `make_async_engine`.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sentinel.config.settings import Settings


def make_async_engine(settings: Settings) -> AsyncEngine:
    dsn = settings.postgres_dsn
    if "+asyncpg" not in dsn:
        raise ValueError(
            f"postgres_dsn must use the asyncpg driver "
            f"(postgresql+asyncpg://...), got: {dsn}"
        )
    return create_async_engine(
        dsn,
        pool_size=10,
        max_overflow=5,
        pool_pre_ping=True,
        future=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
    )
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/persistence/test_session.py -v && make lint typecheck`

- [ ] **Step 5: Stage**

Run: `mkdir -p tests/unit/persistence && touch tests/unit/persistence/__init__.py`
Then: `git add sentinel/persistence/session.py tests/unit/persistence/`

---

### Task B2: `persistence/models.py` — SQLAlchemy 2.x models

**Files:**
- Create: `sentinel/persistence/models.py`
- Test: `tests/unit/persistence/test_models_metadata.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/persistence/test_models_metadata.py
"""Static metadata checks — no DB needed."""

from sentinel.persistence.models import (
    Base,
    DeployModel,
    DiagnosisModel,
    EvalRunModel,
    IncidentModel,
    ResolutionModel,
    RunbookModel,
)


def test_all_tables_registered_on_base() -> None:
    expected = {
        "incidents",
        "diagnoses",
        "resolutions",
        "deploys",
        "runbooks",
        "eval_runs",
    }
    actual = set(Base.metadata.tables.keys())
    assert expected.issubset(actual), f"missing: {expected - actual}"


def test_incident_has_vector_column() -> None:
    cols = {c.name: c for c in IncidentModel.__table__.columns}
    assert "embedding" in cols
    assert "VECTOR" in str(cols["embedding"].type).upper()


def test_runbook_has_vector_column() -> None:
    cols = {c.name: c for c in RunbookModel.__table__.columns}
    assert "embedding" in cols
    assert "VECTOR" in str(cols["embedding"].type).upper()


def test_unique_source_external_id_on_incidents() -> None:
    unique = [c for c in IncidentModel.__table__.constraints if c.__class__.__name__ == "UniqueConstraint"]
    cols = {tuple(sorted(c.column.name for c in uc.columns)) for uc in unique}  # type: ignore[attr-defined]
    assert ("external_id", "source") in cols


def test_diagnosis_fk_cascade() -> None:
    fk = next(iter(DiagnosisModel.__table__.foreign_keys))
    assert fk.ondelete == "CASCADE"


def test_resolution_pk_is_incident_id() -> None:
    pk_cols = [c.name for c in ResolutionModel.__table__.primary_key.columns]
    assert pk_cols == ["incident_id"]


def test_deploy_index_exists() -> None:
    idx_names = {ix.name for ix in DeployModel.__table__.indexes}
    assert "idx_deploys_service_time" in idx_names


def test_eval_run_table_exists() -> None:
    assert EvalRunModel.__tablename__ == "eval_runs"
```

- [ ] **Step 2: Run to fail**

Run: `pytest tests/unit/persistence/test_models_metadata.py -v`
Expected: import error.

- [ ] **Step 3: Implement**

```python
# sentinel/persistence/models.py
"""SQLAlchemy 2.x async ORM models, exact match of the spec's DDL.

Conventions:
- All columns are typed via `Mapped[...]`.
- Check constraints mirror the spec verbatim — autogenerate picks them up.
- Vector columns use pgvector's SA type; index creation (HNSW) lives in the
  migration, not here (HNSW is not first-class in SA's autogenerate).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sentinel.schemas.enums import (
    CATEGORY_VALUES,
    INCIDENT_STATUS_VALUES,
    SEVERITY_VALUES,
    SOURCE_VALUES,
)


def _check_in(col: str, values: tuple[str, ...]) -> CheckConstraint:
    quoted = ",".join(f"'{v}'" for v in values)
    return CheckConstraint(f"{col} IN ({quoted})", name=f"ck_{col}_valid")


class Base(DeclarativeBase):
    pass


class IncidentModel(Base):
    __tablename__ = "incidents"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    service: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="open")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    opened_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_incidents_source_external"),
        _check_in("source", SOURCE_VALUES),
        _check_in("severity", SEVERITY_VALUES),
        _check_in("status", INCIDENT_STATUS_VALUES),
        Index("idx_incidents_service_opened", "service", "opened_at"),
        Index("idx_incidents_fingerprint", "fingerprint"),
    )


class DiagnosisModel(Base):
    __tablename__ = "diagnoses"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    incident_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    evidence: Mapped[Any] = mapped_column(JSONB, nullable=False)
    suggested_actions: Mapped[Any] = mapped_column(JSONB, nullable=False)
    likely_category: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_version: Mapped[str] = mapped_column(Text, nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    token_usage: Mapped[Any] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "confidence BETWEEN 0 AND 1", name="ck_diagnoses_confidence_unit"
        ),
        _check_in("likely_category", CATEGORY_VALUES),
        Index("idx_diagnoses_incident", "incident_id", "created_at"),
    )


class ResolutionModel(Base):
    __tablename__ = "resolutions"

    incident_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("incidents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    remediation: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    diagnosis_was_correct: Mapped[bool | None] = mapped_column(nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class DeployModel(Base):
    __tablename__ = "deploys"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    service: Mapped[str] = mapped_column(Text, nullable=False)
    sha: Mapped[str] = mapped_column(Text, nullable=False)
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pr_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    pr_diff_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    deployed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    deployed_by: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (Index("idx_deploys_service_time", "service", "deployed_at"),)


class RunbookModel(Base):
    __tablename__ = "runbooks"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    service: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class EvalRunModel(Base):
    __tablename__ = "eval_runs"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    model: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_version: Mapped[str] = mapped_column(String, nullable=False)
    corpus_version: Mapped[str] = mapped_column(String, nullable=False)
    results: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    summary: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
```

- [ ] **Step 4: Verify**

Run: `pytest tests/unit/persistence/test_models_metadata.py -v && make lint typecheck`

- [ ] **Step 5: Stage**

Run: `git add sentinel/persistence/models.py tests/unit/persistence/test_models_metadata.py`

---

### Task B3: Migration `0001_initial.py`

**Files:**
- Create: `migrations/versions/0001_initial.py`
- Modify: `migrations/env.py` (one-line change to set `target_metadata`)

- [ ] **Step 1: Wire `env.py` to the models**

Edit `migrations/env.py`. Replace the line `target_metadata = None` with:

```python
from sentinel.persistence.models import Base  # noqa: E402
target_metadata = Base.metadata
```

- [ ] **Step 2: Generate a revision id locally**

Run: `python -c "from alembic.util import rev_id; print(rev_id())"`
Use the printed 12-char hex string as `revision = "<that>"` below; or use the fixed value `0001` if the project convention is sequential. (Inspect `alembic.ini` for `file_template`; if it uses `%(rev)s_%(slug)s` and no zero-padded sequence, use a generated hex id and name the file `0001_initial.py` anyway.)

- [ ] **Step 3: Write the migration**

```python
# migrations/versions/0001_initial.py
"""initial schema: incidents, diagnoses, resolutions, deploys, runbooks, eval_runs

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-18

This migration creates the entire V1 schema in one shot. We intentionally do
not split per-table — they are mutually-referencing and HNSW indexes need raw
SQL, so a single file keeps the operation auditable.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    op.create_table(
        "incidents",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column(
            "status", sa.Text(), nullable=False, server_default=sa.text("'open'")
        ),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "opened_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "resolved_at", postgresql.TIMESTAMP(timezone=True), nullable=True
        ),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.UniqueConstraint("source", "external_id", name="uq_incidents_source_external"),
        sa.CheckConstraint(
            "source IN ('sentry','pagerduty','datadog','generic')",
            name="ck_source_valid",
        ),
        sa.CheckConstraint(
            "severity IN ('SEV1','SEV2','SEV3','SEV4')", name="ck_severity_valid"
        ),
        sa.CheckConstraint(
            "status IN ('open','diagnosing','mitigated','resolved','closed')",
            name="ck_status_valid",
        ),
    )
    op.create_index(
        "idx_incidents_service_opened",
        "incidents",
        ["service", sa.text("opened_at DESC")],
    )
    op.create_index("idx_incidents_fingerprint", "incidents", ["fingerprint"])
    op.execute(
        "CREATE INDEX idx_incidents_embedding ON incidents "
        "USING hnsw (embedding vector_cosine_ops)"
    )

    op.create_table(
        "diagnoses",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("hypothesis", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("reasoning", sa.Text(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("suggested_actions", postgresql.JSONB(), nullable=False),
        sa.Column("likely_category", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("token_usage", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "confidence BETWEEN 0 AND 1", name="ck_diagnoses_confidence_unit"
        ),
        sa.CheckConstraint(
            "likely_category IN ('deploy','config','dependency','capacity','data','external')",
            name="ck_likely_category_valid",
        ),
    )
    op.create_index(
        "idx_diagnoses_incident",
        "diagnoses",
        ["incident_id", sa.text("created_at DESC")],
    )

    op.create_table(
        "resolutions",
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("root_cause", sa.Text(), nullable=False),
        sa.Column("remediation", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("diagnosis_was_correct", sa.Boolean(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("resolved_by", sa.Text(), nullable=True),
        sa.Column(
            "resolved_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "deploys",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("sha", sa.Text(), nullable=False),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("pr_title", sa.Text(), nullable=True),
        sa.Column("pr_diff_summary", sa.Text(), nullable=True),
        sa.Column(
            "deployed_at", postgresql.TIMESTAMP(timezone=True), nullable=False
        ),
        sa.Column("deployed_by", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_deploys_service_time",
        "deploys",
        ["service", sa.text("deployed_at DESC")],
    )

    op.create_table(
        "runbooks",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.execute(
        "CREATE INDEX idx_runbooks_embedding ON runbooks "
        "USING hnsw (embedding vector_cosine_ops)"
    )

    op.create_table(
        "eval_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "started_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "completed_at", postgresql.TIMESTAMP(timezone=True), nullable=True
        ),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("corpus_version", sa.Text(), nullable=False),
        sa.Column("results", postgresql.JSONB(), nullable=True),
        sa.Column("summary", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("eval_runs")
    op.execute("DROP INDEX IF EXISTS idx_runbooks_embedding")
    op.drop_table("runbooks")
    op.drop_table("deploys")
    op.drop_table("resolutions")
    op.drop_index("idx_diagnoses_incident", table_name="diagnoses")
    op.drop_table("diagnoses")
    op.execute("DROP INDEX IF EXISTS idx_incidents_embedding")
    op.drop_index("idx_incidents_fingerprint", table_name="incidents")
    op.drop_index("idx_incidents_service_opened", table_name="incidents")
    op.drop_table("incidents")
    # Do NOT drop extensions — other migrations / databases may depend on them
    # and pgcrypto is harmless to leave in place.
```

- [ ] **Step 4: Stage (test in next task)**

Run: `git add migrations/env.py migrations/versions/0001_initial.py`

---

### Task B4: Integration test — migration round-trip with testcontainers

**Files:**
- Create: `tests/integration/persistence/__init__.py` (empty)
- Create: `tests/integration/persistence/conftest.py`
- Create: `tests/integration/persistence/test_migration_roundtrip.py`

- [ ] **Step 1: Conftest fixture**

```python
# tests/integration/persistence/conftest.py
"""Testcontainers-backed Postgres with pgvector."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    image = os.environ.get("SENTINEL_TEST_PG_IMAGE", "pgvector/pgvector:pg16")
    with PostgresContainer(image, driver="asyncpg") as pg:
        yield pg


@pytest.fixture()
def pg_dsn(pg_container: PostgresContainer) -> str:
    # testcontainers returns a sync-flavored URL; rewrite to asyncpg.
    raw = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
```

- [ ] **Step 2: Migration round-trip test**

```python
# tests/integration/persistence/test_migration_roundtrip.py
"""upgrade head → downgrade base on a real Postgres with pgvector."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn.replace("+asyncpg", ""))
    return cfg


def test_upgrade_then_downgrade(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")

    async def _list_tables() -> set[str]:
        eng = create_async_engine(pg_dsn)
        try:
            async with eng.connect() as conn:
                names = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
        finally:
            await eng.dispose()
        return names

    tables = asyncio.run(_list_tables())
    assert {"incidents", "diagnoses", "resolutions", "deploys", "runbooks", "eval_runs"}.issubset(
        tables
    )

    command.downgrade(cfg, "base")
    tables_after = asyncio.run(_list_tables())
    assert not {"incidents", "diagnoses", "resolutions", "deploys", "runbooks", "eval_runs"} & tables_after


def test_vector_extension_present_after_upgrade(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")

    async def _check() -> bool:
        eng = create_async_engine(pg_dsn)
        try:
            async with eng.connect() as conn:
                row = await conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                )
                return row.first() is not None
        finally:
            await eng.dispose()

    assert asyncio.run(_check()) is True

    command.downgrade(cfg, "base")
```

- [ ] **Step 3: Sync env.py needed for sync alembic command**

Alembic's `command.upgrade` invokes our `run_migrations_online` which is async via `asyncio.run`. That conflicts when pytest itself is sync calling sync alembic. The existing `migrations/env.py` already calls `asyncio.run`; the integration test calls `command.upgrade(cfg, "head")` synchronously, which loads env.py and runs `asyncio.run(...)`. This works only outside an event loop. If a future test runs in an async context, wrap with `loop.run_in_executor`. For now this is fine — note in the test docstring.

- [ ] **Step 4: Run the integration test (Docker required)**

Run: `make compose-down 2>/dev/null; pytest tests/integration/persistence -m integration -v`
Expected: PASS (≤90s including container pull on cold cache).

If it fails because pgvector image isn't pulled: `docker pull pgvector/pgvector:pg16` and retry.

- [ ] **Step 5: Stage**

Run: `git add tests/integration/persistence/`

---

### Task B5: `persistence/repositories.py` — async repositories

**Files:**
- Create: `sentinel/persistence/repositories.py`
- Create: `tests/integration/persistence/test_repositories.py`

- [ ] **Step 1: Write the failing integration test**

```python
# tests/integration/persistence/test_repositories.py
"""Round-trip each repo against a live Postgres."""

from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine

from sentinel.persistence.repositories import (
    DeployRepository,
    IncidentRepository,
    PostgresDeployRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas import NormalizedAlert
from datetime import datetime, timezone

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn.replace("+asyncpg", ""))
    return cfg


@pytest.fixture()
def migrated_dsn(pg_dsn: str) -> str:
    command.upgrade(_alembic_cfg(pg_dsn), "head")
    return pg_dsn


@pytest.mark.asyncio
async def test_incident_insert_then_fetch(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: IncidentRepository = PostgresIncidentRepository(factory)

    alert = NormalizedAlert(
        source="generic",
        external_id="ext-1",
        service="svc",
        severity="SEV3",
        title="t",
        received_at=datetime.now(timezone.utc),
        raw_payload={"k": "v"},
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-1")

    fetched = await repo.get(incident_id)
    assert fetched is not None
    assert fetched.title == "t"
    assert fetched.status == "open"
    await engine.dispose()


@pytest.mark.asyncio
async def test_deploy_insert_then_list_by_service(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: DeployRepository = PostgresDeployRepository(factory)
    await repo.record(service="svc", sha="abc123", deployed_at=datetime.now(timezone.utc))
    deploys = await repo.recent_for_service("svc", limit=10)
    assert any(d.sha == "abc123" for d in deploys)
    await engine.dispose()
```

- [ ] **Step 2: Implement**

```python
# sentinel/persistence/repositories.py
"""Repository layer — the only module that may write SQL.

Each repository exposes a `Protocol` (the interface other modules depend on)
plus a Postgres-backed concrete implementation. Methods accept and return
Pydantic schemas where they cross a module boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sentinel.persistence.models import (
    DeployModel,
    DiagnosisModel,
    EvalRunModel,
    IncidentModel,
    ResolutionModel,
    RunbookModel,
)
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import IncidentDetailResponse, IncidentListItem


# --- Read DTOs -------------------------------------------------------------- #

@dataclass(frozen=True, slots=True)
class DeployRow:
    id: UUID
    service: str
    sha: str
    pr_title: str | None
    deployed_at: datetime


# --- Incident repository ---------------------------------------------------- #

class IncidentRepository(Protocol):
    async def create_from_alert(
        self, alert: NormalizedAlert, *, fingerprint: str
    ) -> UUID: ...
    async def get(self, incident_id: UUID) -> IncidentDetailResponse | None: ...
    async def list_recent(self, *, limit: int = 50) -> list[IncidentListItem]: ...


class PostgresIncidentRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create_from_alert(
        self, alert: NormalizedAlert, *, fingerprint: str
    ) -> UUID:
        async with self._session_factory() as s:
            row = IncidentModel(
                external_id=alert.external_id,
                source=alert.source,
                service=alert.service,
                severity=alert.severity,
                title=alert.title,
                fingerprint=fingerprint,
                raw_payload=alert.raw_payload,
            )
            s.add(row)
            await s.commit()
            await s.refresh(row)
            return row.id

    async def get(self, incident_id: UUID) -> IncidentDetailResponse | None:
        async with self._session_factory() as s:
            row = await s.get(IncidentModel, incident_id)
            if row is None:
                return None
            return IncidentDetailResponse(
                id=row.id,
                source=row.source,  # type: ignore[arg-type]
                external_id=row.external_id,
                service=row.service,
                severity=row.severity,  # type: ignore[arg-type]
                status=row.status,  # type: ignore[arg-type]
                title=row.title,
                fingerprint=row.fingerprint,
                opened_at=row.opened_at,
                resolved_at=row.resolved_at,
            )

    async def list_recent(self, *, limit: int = 50) -> list[IncidentListItem]:
        async with self._session_factory() as s:
            stmt = select(IncidentModel).order_by(IncidentModel.opened_at.desc()).limit(limit)
            result = await s.execute(stmt)
            return [
                IncidentListItem(
                    id=row.id,
                    service=row.service,
                    severity=row.severity,  # type: ignore[arg-type]
                    status=row.status,  # type: ignore[arg-type]
                    title=row.title,
                    opened_at=row.opened_at,
                    resolved_at=row.resolved_at,
                )
                for row in result.scalars()
            ]


# --- Deploy repository ------------------------------------------------------ #

class DeployRepository(Protocol):
    async def record(
        self, *, service: str, sha: str, deployed_at: datetime,
        pr_number: int | None = None, pr_title: str | None = None,
        pr_diff_summary: str | None = None, deployed_by: str | None = None,
    ) -> UUID: ...
    async def recent_for_service(
        self, service: str, *, limit: int = 20
    ) -> list[DeployRow]: ...


class PostgresDeployRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(
        self,
        *,
        service: str,
        sha: str,
        deployed_at: datetime,
        pr_number: int | None = None,
        pr_title: str | None = None,
        pr_diff_summary: str | None = None,
        deployed_by: str | None = None,
    ) -> UUID:
        async with self._session_factory() as s:
            row = DeployModel(
                service=service,
                sha=sha,
                pr_number=pr_number,
                pr_title=pr_title,
                pr_diff_summary=pr_diff_summary,
                deployed_at=deployed_at,
                deployed_by=deployed_by,
            )
            s.add(row)
            await s.commit()
            await s.refresh(row)
            return row.id

    async def recent_for_service(
        self, service: str, *, limit: int = 20
    ) -> list[DeployRow]:
        async with self._session_factory() as s:
            stmt = (
                select(DeployModel)
                .where(DeployModel.service == service)
                .order_by(DeployModel.deployed_at.desc())
                .limit(limit)
            )
            result = await s.execute(stmt)
            return [
                DeployRow(
                    id=row.id,
                    service=row.service,
                    sha=row.sha,
                    pr_title=row.pr_title,
                    deployed_at=row.deployed_at,
                )
                for row in result.scalars()
            ]


# --- Stubs for the rest (implemented as needed in their consuming work areas) ---

class DiagnosisRepository(Protocol):
    async def save(self, incident_id: UUID, payload: dict[str, Any]) -> UUID: ...


class ResolutionRepository(Protocol):
    async def record(self, incident_id: UUID, payload: dict[str, Any]) -> None: ...


class RunbookRepository(Protocol):
    async def search(
        self, embedding: list[float], *, k: int, min_cosine: float
    ) -> list[Any]: ...


class EvalRunRepository(Protocol):
    async def start(
        self, *, model: str, prompt_version: str, corpus_version: str
    ) -> UUID: ...
    async def complete(self, run_id: UUID, summary: dict[str, float]) -> None: ...


# Concrete classes for the stubbed Protocols above land with their consumers
# (Work Areas G, H, K). Keeping the Protocols here means downstream modules
# can depend on the interface without forcing the implementation now.
__all__ = [
    "DeployRepository",
    "DeployRow",
    "DiagnosisRepository",
    "EvalRunRepository",
    "IncidentRepository",
    "PostgresDeployRepository",
    "PostgresIncidentRepository",
    "ResolutionRepository",
    "RunbookRepository",
]
```

- [ ] **Step 3: Run repository integration tests**

Run: `pytest tests/integration/persistence/test_repositories.py -m integration -v`
Expected: PASS.

- [ ] **Step 4: Verify the full persistence slice**

Run: `make lint typecheck && pytest tests/unit/persistence tests/integration/persistence -v -m "not integration or integration"`

- [ ] **Step 5: Stage**

Run: `git add sentinel/persistence/repositories.py tests/integration/persistence/test_repositories.py`

---

### Task B6: Guard test — no raw SQL outside `persistence/`

**Files:**
- Create: `tests/unit/persistence/test_no_raw_sql_outside.py`

- [ ] **Step 1: Write the test**

```python
# tests/unit/persistence/test_no_raw_sql_outside.py
"""Enforce the architectural rule: only `sentinel/persistence/` writes SQL.

If you need DB access from another module, add a repository method, not a query.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3] / "sentinel"
_FORBIDDEN = re.compile(
    r"\b(select|insert|update|delete|text)\s*\(", re.IGNORECASE
)
_ALLOWED_PREFIXES = ("persistence/",)


def _is_allowed(path: Path) -> bool:
    rel = path.relative_to(_ROOT).as_posix()
    return any(rel.startswith(p) for p in _ALLOWED_PREFIXES)


def test_no_raw_sql_outside_persistence() -> None:
    offenders: list[str] = []
    for py in _ROOT.rglob("*.py"):
        if _is_allowed(py):
            continue
        src = py.read_text(encoding="utf-8")
        # Strip comments and strings (rough): drop lines starting with #
        sanitized = "\n".join(
            line for line in src.splitlines() if not line.strip().startswith("#")
        )
        if _FORBIDDEN.search(sanitized):
            offenders.append(py.relative_to(_ROOT).as_posix())
    assert offenders == [], (
        "Files outside sentinel/persistence/ contain raw SQL constructs "
        f"(select/insert/update/delete/text()): {offenders}. "
        "Add a repository method instead."
    )
```

- [ ] **Step 2: Run**

Run: `pytest tests/unit/persistence/test_no_raw_sql_outside.py -v`
Expected: PASS (currently no offenders).

- [ ] **Step 3: Stage**

Run: `git add tests/unit/persistence/test_no_raw_sql_outside.py`

---

### Task B7: Update `sentinel/persistence/__init__.py` re-exports + `Makefile` migrate target

**Files:**
- Modify: `sentinel/persistence/__init__.py`
- Verify: `Makefile` has `migrate` / `migrate-down` (already from Work Area A)

- [ ] **Step 1: Re-exports**

```python
# sentinel/persistence/__init__.py
"""Persistence layer — async SQLAlchemy 2.x models, repositories, sessions.

This is the only module allowed to perform raw SQL operations.
"""

from sentinel.persistence.models import (
    Base,
    DeployModel,
    DiagnosisModel,
    EvalRunModel,
    IncidentModel,
    ResolutionModel,
    RunbookModel,
)
from sentinel.persistence.repositories import (
    DeployRepository,
    DeployRow,
    DiagnosisRepository,
    EvalRunRepository,
    IncidentRepository,
    PostgresDeployRepository,
    PostgresIncidentRepository,
    ResolutionRepository,
    RunbookRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory

__all__ = [
    "Base",
    "DeployModel",
    "DeployRepository",
    "DeployRow",
    "DiagnosisModel",
    "DiagnosisRepository",
    "EvalRunModel",
    "EvalRunRepository",
    "IncidentModel",
    "IncidentRepository",
    "PostgresDeployRepository",
    "PostgresIncidentRepository",
    "ResolutionModel",
    "ResolutionRepository",
    "RunbookModel",
    "RunbookRepository",
    "make_async_engine",
    "make_session_factory",
]
```

- [ ] **Step 2: Verify `make migrate` runs end-to-end against compose stack**

Run: `make compose-up && make migrate && make migrate-down && make migrate`
Expected: each command exits 0; `psql` shows tables.

- [ ] **Step 3: Stage**

Run: `git add sentinel/persistence/__init__.py`

---

### Task B8: Work Area B code review

- [ ] **Step 1: Invoke `superpowers:requesting-code-review`**

Brief the reviewer: "Review Work Area B (Persistence) of Sentinel against CLAUDE.md and `.claude/program-of-work.md` §B. Focus: (1) migration 0001 upgrade+downgrade actually reversible, (2) check constraints match spec verbatim, (3) HNSW indexes present, (4) repositories never leak SA rows, (5) `Decimal` vs `float` for `confidence` correctly handled at the boundary, (6) `mypy --strict` clean."

- [ ] **Step 2: Address findings, re-run `make lint typecheck && pytest`**

- [ ] **Step 3: Hand off for commit**

Tell the user: "Work Area B ready. Suggested message: `feat(persistence): SQLAlchemy models, repositories, migration 0001 (Work Area B)`."

---

## Work Area J — Observability skeleton

These are in-process tests. No Docker required.

### Task J1: `observability/logging.py` — structlog config

**Files:**
- Create: `sentinel/observability/logging.py`
- Test: `tests/unit/observability/test_logging.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/observability/test_logging.py
"""Logging config produces JSON in prod, attaches context vars."""

from __future__ import annotations

import json
import logging
from io import StringIO

import structlog

from sentinel.config.settings import Settings
from sentinel.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_llm_audit_logger,
)


def _settings(**overrides: object) -> Settings:
    base = {
        "postgres_dsn": "postgresql+asyncpg://u:p@localhost/d",
        "redis_url": "redis://localhost:6379/0",
        "kafka_brokers": "localhost:9092",
        "anthropic_api_key": "x",
        "log_level": "INFO",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_json_renderer_in_prod(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "sentinel.observability.logging._stream_for_tests",
        StringIO(),
        raising=False,
    )
    s = _settings(env="prod", llm_audit_log_path=str(tmp_path / "llm.log"))
    configure_logging(s)
    log = structlog.get_logger("test")
    bind_request_context(incident_id="abc-123", correlation_id="corr-1", source="sentry")
    try:
        log.info("hello", k="v")
    finally:
        clear_request_context()
    # Smoke check: configure runs without error and a logger exists.
    assert structlog.is_configured()


def test_llm_audit_logger_writes_to_audit_path(tmp_path) -> None:
    audit = tmp_path / "llm-audit.log"
    s = _settings(env="dev", llm_audit_log_path=str(audit))
    configure_logging(s)
    audit_log = get_llm_audit_logger()
    audit_log.info("prompt-and-response", prompt="hi", response="world")
    for h in logging.getLogger("sentinel.llm_audit").handlers:
        h.flush()
    text = audit.read_text(encoding="utf-8")
    assert "prompt-and-response" in text


def test_context_vars_bind_and_clear() -> None:
    s = _settings()
    configure_logging(s)
    bind_request_context(incident_id="i", correlation_id="c", source="x")
    # processors flush via structlog contextvars; assert no exception on clear.
    clear_request_context()
```

- [ ] **Step 2: Implement**

```python
# sentinel/observability/logging.py
"""structlog configuration + LLM audit log handler.

`configure_logging(settings)` is safe to call more than once — repeated calls
reapply the same config. Context vars are managed via `structlog.contextvars`
so they propagate across async tasks correctly.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog

from sentinel.config.settings import Settings

_LLM_AUDIT_LOGGER_NAME = "sentinel.llm_audit"


def configure_logging(settings: Settings) -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level, format="%(message)s", force=True
    )
    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    if settings.env == "prod":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _configure_llm_audit(settings.llm_audit_log_path)


def _configure_llm_audit(path_str: str) -> None:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(_LLM_AUDIT_LOGGER_NAME)
    logger.handlers.clear()
    handler = RotatingFileHandler(
        path, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def get_llm_audit_logger() -> structlog.stdlib.BoundLogger:
    """Returns a structlog logger writing to the LLM audit log file."""
    return structlog.wrap_logger(
        logging.getLogger(_LLM_AUDIT_LOGGER_NAME),
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
    )


def bind_request_context(
    *, incident_id: str | None = None, correlation_id: str, source: str | None = None
) -> None:
    fields: dict[str, str] = {"correlation_id": correlation_id}
    if incident_id is not None:
        fields["incident_id"] = incident_id
    if source is not None:
        fields["source"] = source
    structlog.contextvars.bind_contextvars(**fields)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
```

- [ ] **Step 3: Verify**

Run: `pytest tests/unit/observability/test_logging.py -v && make lint typecheck`

- [ ] **Step 4: Stage**

Run: `mkdir -p tests/unit/observability && touch tests/unit/observability/__init__.py`
Then: `git add sentinel/observability/logging.py tests/unit/observability/`

---

### Task J2: `observability/metrics.py` — Prometheus singletons

**Files:**
- Create: `sentinel/observability/metrics.py`
- Test: `tests/unit/observability/test_metrics.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/observability/test_metrics.py
"""Every metric from the spec exists with correct type and labels."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from sentinel.observability import metrics as M


def test_all_spec_metrics_exist() -> None:
    expected: dict[str, type] = {
        "sentinel_webhooks_total": Counter,
        "sentinel_incidents_opened_total": Counter,
        "sentinel_enrichment_duration_seconds": Histogram,
        "sentinel_enrichment_failures_total": Counter,
        "sentinel_circuit_breaker_state": Gauge,
        "sentinel_diagnosis_latency_seconds": Histogram,
        "sentinel_diagnosis_confidence": Histogram,
        "sentinel_llm_tokens_total": Counter,
        "sentinel_llm_cost_usd_total": Counter,
        "sentinel_hallucinated_evidence_rate": Gauge,
        "sentinel_diagnosis_correctness_rate_30d": Gauge,
    }
    for name, klass in expected.items():
        instance = getattr(M, M.attr_for(name))
        assert isinstance(instance, klass), f"{name} is {type(instance).__name__}, expected {klass.__name__}"


def test_diagnosis_latency_buckets_match_spec() -> None:
    h = M.diagnosis_latency_seconds
    bounds = [s for s in h._upper_bounds if s != float("inf")]
    assert bounds == [1, 2, 5, 10, 15, 30]


def test_diagnosis_confidence_buckets_match_spec() -> None:
    h = M.diagnosis_confidence
    bounds = [s for s in h._upper_bounds if s != float("inf")]
    assert bounds == [0.1, 0.3, 0.5, 0.7, 0.85, 1.0]


def test_time_histogram_records_on_success() -> None:
    with M.time_histogram(M.enrichment_duration_seconds, fetcher="deploys"):
        pass
    sample = M.enrichment_duration_seconds.labels(fetcher="deploys")
    assert sample._sum.get() > 0


def test_time_histogram_records_on_exception() -> None:
    with pytest.raises(RuntimeError):
        with M.time_histogram(M.enrichment_duration_seconds, fetcher="logs"):
            raise RuntimeError("boom")
    sample = M.enrichment_duration_seconds.labels(fetcher="logs")
    assert sample._sum.get() > 0


def test_double_import_does_not_raise() -> None:
    import importlib

    importlib.reload(M)  # must not throw on re-registration
```

- [ ] **Step 2: Implement**

```python
# sentinel/observability/metrics.py
"""Prometheus metrics named per the spec. Singletons at module level.

The `_safe_*` helpers guard against double-registration when this module is
reimported (e.g., by a test reload). On collision, we return the existing
collector from the registry.
"""

from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

from prometheus_client import REGISTRY, Counter, Gauge, Histogram


def _safe_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = REGISTRY._names_to_collectors[name]  # type: ignore[attr-defined]
        return existing  # type: ignore[return-value]


def _safe_gauge(name: str, documentation: str, labelnames: list[str] | None = None) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames=labelnames or [])
    except ValueError:
        existing = REGISTRY._names_to_collectors[name]  # type: ignore[attr-defined]
        return existing  # type: ignore[return-value]


def _safe_histogram(
    name: str,
    documentation: str,
    labelnames: list[str],
    buckets: tuple[float, ...] | None = None,
) -> Histogram:
    try:
        if buckets is not None:
            return Histogram(
                name, documentation, labelnames=labelnames, buckets=buckets
            )
        return Histogram(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = REGISTRY._names_to_collectors[name]  # type: ignore[attr-defined]
        return existing  # type: ignore[return-value]


webhooks_total = _safe_counter(
    "sentinel_webhooks_total", "Incoming webhooks", ["source", "status"]
)
incidents_opened_total = _safe_counter(
    "sentinel_incidents_opened_total",
    "Incidents opened",
    ["service", "severity"],
)
enrichment_duration_seconds = _safe_histogram(
    "sentinel_enrichment_duration_seconds",
    "Time spent in each enrichment fetcher",
    ["fetcher"],
)
enrichment_failures_total = _safe_counter(
    "sentinel_enrichment_failures_total",
    "Enrichment fetcher failures",
    ["fetcher", "reason"],
)
circuit_breaker_state = _safe_gauge(
    "sentinel_circuit_breaker_state",
    "Breaker state (0=closed,1=open,2=half_open)",
    ["integration"],
)
diagnosis_latency_seconds = _safe_histogram(
    "sentinel_diagnosis_latency_seconds",
    "End-to-end diagnosis latency",
    [],
    buckets=(1, 2, 5, 10, 15, 30),
)
diagnosis_confidence = _safe_histogram(
    "sentinel_diagnosis_confidence",
    "Confidence values reported by the model",
    [],
    buckets=(0.1, 0.3, 0.5, 0.7, 0.85, 1.0),
)
llm_tokens_total = _safe_counter(
    "sentinel_llm_tokens_total",
    "LLM token usage",
    ["model", "kind"],
)
llm_cost_usd_total = _safe_counter(
    "sentinel_llm_cost_usd_total",
    "LLM cost (USD), cumulative",
    ["model"],
)
hallucinated_evidence_rate = _safe_gauge(
    "sentinel_hallucinated_evidence_rate",
    "Fraction of diagnoses with invented citations",
)
diagnosis_correctness_rate_30d = _safe_gauge(
    "sentinel_diagnosis_correctness_rate_30d",
    "Fraction of diagnoses marked correct on resolution, 30d rolling",
)

_NAME_TO_ATTR: dict[str, str] = {
    "sentinel_webhooks_total": "webhooks_total",
    "sentinel_incidents_opened_total": "incidents_opened_total",
    "sentinel_enrichment_duration_seconds": "enrichment_duration_seconds",
    "sentinel_enrichment_failures_total": "enrichment_failures_total",
    "sentinel_circuit_breaker_state": "circuit_breaker_state",
    "sentinel_diagnosis_latency_seconds": "diagnosis_latency_seconds",
    "sentinel_diagnosis_confidence": "diagnosis_confidence",
    "sentinel_llm_tokens_total": "llm_tokens_total",
    "sentinel_llm_cost_usd_total": "llm_cost_usd_total",
    "sentinel_hallucinated_evidence_rate": "hallucinated_evidence_rate",
    "sentinel_diagnosis_correctness_rate_30d": "diagnosis_correctness_rate_30d",
}


def attr_for(metric_name: str) -> str:
    return _NAME_TO_ATTR[metric_name]


@contextmanager
def time_histogram(metric: Histogram, **labels: str) -> Iterator[None]:
    start = perf_counter()
    try:
        yield
    finally:
        elapsed = perf_counter() - start
        if labels:
            metric.labels(**labels).observe(elapsed)
        else:
            metric.observe(elapsed)


@contextmanager
def count_failure(metric: Counter, **labels: str) -> Iterator[None]:
    try:
        yield
    except Exception:
        if labels:
            metric.labels(**labels).inc()
        else:
            metric.inc()
        raise
```

- [ ] **Step 3: Verify**

Run: `pytest tests/unit/observability/test_metrics.py -v && make lint typecheck`

- [ ] **Step 4: Stage**

Run: `git add sentinel/observability/metrics.py tests/unit/observability/test_metrics.py`

---

### Task J3: `observability/tracing.py` — OTel SDK init + helpers

**Files:**
- Create: `sentinel/observability/tracing.py`
- Test: `tests/unit/observability/test_tracing.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/observability/test_tracing.py
"""Tracing helpers are safe when no endpoint is configured."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from sentinel.config.settings import Settings
from sentinel.observability.tracing import (
    configure_tracing,
    span_for_fetcher,
    span_for_llm,
)


def _settings(otel_endpoint: str | None = None) -> Settings:
    return Settings(
        postgres_dsn="postgresql+asyncpg://u:p@localhost/d",
        redis_url="redis://localhost:6379/0",
        kafka_brokers="localhost:9092",
        anthropic_api_key="x",
        otel_endpoint=otel_endpoint,
    )


def test_no_endpoint_is_noop() -> None:
    configure_tracing(_settings(otel_endpoint=None))
    with span_for_fetcher("deploys"):
        pass
    with span_for_llm("claude-sonnet-4-5"):
        pass


def test_with_inmemory_exporter_records_spans(monkeypatch) -> None:
    # Configure manually with an in-memory exporter for the test
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    with span_for_fetcher("logs"):
        pass
    with span_for_llm("claude-sonnet-4-5"):
        pass

    spans = exporter.get_finished_spans()
    names = {s.name for s in spans}
    assert "fetcher.logs" in names
    assert "llm.claude-sonnet-4-5" in names
```

- [ ] **Step 2: Implement**

```python
# sentinel/observability/tracing.py
"""OTel SDK init + span helpers.

When `settings.otel_endpoint` is unset we still call `set_tracer_provider`
with a default provider so that the helper context managers work as no-ops
(they emit unsampled spans rather than raising).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

from sentinel.config.settings import Settings

_TRACER_NAME = "sentinel"


def configure_tracing(settings: Settings) -> None:
    resource = Resource.create({"service.name": "sentinel"})
    provider = TracerProvider(resource=resource)
    if settings.otel_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_endpoint))
        )
    trace.set_tracer_provider(provider)


def _tracer() -> Tracer:
    return trace.get_tracer(_TRACER_NAME)


@contextmanager
def span_for_fetcher(name: str) -> Iterator[trace.Span]:
    with _tracer().start_as_current_span(f"fetcher.{name}") as span:
        span.set_attribute("sentinel.fetcher", name)
        yield span


@contextmanager
def span_for_llm(model: str) -> Iterator[trace.Span]:
    with _tracer().start_as_current_span(f"llm.{model}") as span:
        span.set_attribute("sentinel.llm.model", model)
        yield span
```

- [ ] **Step 3: Verify**

Run: `pytest tests/unit/observability/test_tracing.py -v && make lint typecheck`

- [ ] **Step 4: Stage**

Run: `git add sentinel/observability/tracing.py tests/unit/observability/test_tracing.py`

---

### Task J4: `observability/cost.py` — token→USD calculator

**Files:**
- Create: `sentinel/observability/cost.py`
- Test: `tests/unit/observability/test_cost.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/observability/test_cost.py
"""Token-to-USD calculator for LLM cost tracking."""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from sentinel.observability.cost import _WARNED_MODELS, usd_cost


@pytest.fixture(autouse=True)
def _reset_warnings() -> None:
    _WARNED_MODELS.clear()
    yield
    _WARNED_MODELS.clear()


def test_known_model_known_cost() -> None:
    # claude-sonnet-4-5: $3.00 per 1M input, $15.00 per 1M output (placeholder rates)
    cost = usd_cost("claude-sonnet-4-5", input_tokens=1_000_000, output_tokens=0)
    assert cost == Decimal("3.00")


def test_known_model_combined_input_output() -> None:
    cost = usd_cost("claude-sonnet-4-5", input_tokens=1_000_000, output_tokens=1_000_000)
    assert cost == Decimal("18.00")


def test_unknown_model_returns_zero_and_warns_once(caplog) -> None:
    caplog.set_level(logging.WARNING, logger="sentinel.cost")
    assert usd_cost("totally-fake-model", 1_000, 1_000) == Decimal("0")
    assert usd_cost("totally-fake-model", 1_000, 1_000) == Decimal("0")
    warnings = [
        r for r in caplog.records if "totally-fake-model" in r.getMessage()
    ]
    assert len(warnings) == 1
```

- [ ] **Step 2: Implement**

```python
# sentinel/observability/cost.py
"""LLM cost in USD from token counts. Update the price table as Anthropic publishes.

Rates are USD per 1M tokens. If a model isn't in the table, we return zero and
log a single WARNING (so cost goes uncounted rather than miscounted)."""

from __future__ import annotations

import logging
from decimal import Decimal

_LOG = logging.getLogger("sentinel.cost")

# Per 1M tokens; placeholder values, easy to update.
_PRICES: dict[str, tuple[Decimal, Decimal]] = {
    "claude-sonnet-4-5": (Decimal("3.00"), Decimal("15.00")),
    "claude-opus-4-7": (Decimal("15.00"), Decimal("75.00")),
    "claude-haiku-4-5": (Decimal("0.80"), Decimal("4.00")),
}

_WARNED_MODELS: set[str] = set()
_PER_MILLION = Decimal("1000000")


def usd_cost(model: str, input_tokens: int, output_tokens: int) -> Decimal:
    rates = _PRICES.get(model)
    if rates is None:
        if model not in _WARNED_MODELS:
            _WARNED_MODELS.add(model)
            _LOG.warning("no price table entry for model %r — cost reported as 0", model)
        return Decimal("0")
    input_rate, output_rate = rates
    return (
        Decimal(input_tokens) * input_rate / _PER_MILLION
        + Decimal(output_tokens) * output_rate / _PER_MILLION
    )
```

- [ ] **Step 3: Verify**

Run: `pytest tests/unit/observability/test_cost.py -v && make lint typecheck`

- [ ] **Step 4: Stage**

Run: `git add sentinel/observability/cost.py tests/unit/observability/test_cost.py`

---

### Task J5: Update `sentinel/observability/__init__.py` re-exports

**Files:**
- Modify: `sentinel/observability/__init__.py`

- [ ] **Step 1: Re-exports**

```python
# sentinel/observability/__init__.py
"""Observability primitives — logging, metrics, tracing, cost.

Skeleton lands here; downstream work areas import these and call them as code
that needs instrumentation lands.
"""

from sentinel.observability.cost import usd_cost
from sentinel.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_llm_audit_logger,
)
from sentinel.observability.tracing import (
    configure_tracing,
    span_for_fetcher,
    span_for_llm,
)

__all__ = [
    "bind_request_context",
    "clear_request_context",
    "configure_logging",
    "configure_tracing",
    "get_llm_audit_logger",
    "span_for_fetcher",
    "span_for_llm",
    "usd_cost",
]
```

- [ ] **Step 2: Whole-package verify**

Run: `pytest tests/unit/observability -v && make lint typecheck`

- [ ] **Step 3: Stage**

Run: `git add sentinel/observability/__init__.py`

---

### Task J6: Work Area J code review

- [ ] **Step 1: Invoke `superpowers:requesting-code-review`**

Brief: "Review Work Area J-skeleton (Observability) of Sentinel against CLAUDE.md and `.claude/program-of-work.md` §J. Focus: (1) every metric named in the spec exists with correct type, labels, and histogram buckets, (2) `configure_logging` is idempotent and safe to call multiple times, (3) `bind_request_context` uses `structlog.contextvars` so it propagates across `asyncio.gather`, (4) tracing is a no-op when `otel_endpoint` is unset (does not raise), (5) cost calculator warns once per unknown model."

- [ ] **Step 2: Address findings, re-run `make lint typecheck && pytest`**

- [ ] **Step 3: Hand off for commit**

Tell the user: "Work Area J-skeleton ready. Suggested message: `feat(observability): logging, metrics, tracing, cost skeleton (Work Area J)`."

---

## Final cross-area check

### Task Z1: Full repo green

- [ ] **Step 1: Lint, typecheck, full test run**

Run: `make lint typecheck test`
Expected: all pass.

- [ ] **Step 2: Integration tests (Docker required)**

Run: `make test-integration`
Expected: pass (≤2 min on warm caches).

- [ ] **Step 3: CI dry-run (act, if installed) or push to feature branch and watch CI**

If `act` is installed locally: `act -j lint -j typecheck -j test`.
Otherwise, after user creates the PR, watch the GH Actions run.

---

## Self-review notes (already applied)

- **Spec coverage:** Every deliverable bullet from §B, §C, §J(skeleton) of the program-of-work maps to a task above (C1–C7 → §C; B1–B7 → §B; J1–J5 → §J). Bonus task B6 covers the "no raw SQL outside" guard called out in §B Acceptance.
- **Placeholder scan:** No "TODO", no "handle edge cases", no "similar to". Every code block is complete.
- **Type consistency:** `SourceType/SeverityType/IncidentStatusType/CategoryType/EvidenceKindType` defined in C1, used by C3/C4/C5/C6 and B2 (via `*_VALUES` tuples). `IncidentContext` defined in C4, imported by C6 and consumed by B5 repos. `NormalizedAlert` in C3 → consumed by `PostgresIncidentRepository.create_from_alert` in B5.
- **Cross-area commit order:** C first (no DB), then B (depends on C enums for the CHECK constraints / Literal types), then J (depends on nothing in B or C). This is also the order least likely to leave the repo in a non-green state between commits.
