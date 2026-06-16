# API Surface PR 2b — Evals Read Surface — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the eval-run history over HTTP — `GET /evals/runs`, `GET /evals/runs/{id}`, `GET /evals/baseline` — so the eval dashboard (Work Area L) can read run metrics, trends, and the current baseline.

**Architecture:** Three read-only routes over the already-built `EvalRunRepository`, following the established `request.app.state.<repo>` DI pattern. `EvalRunRepository.list_recent/get_run/get_latest_ok_run` already exist; this PR wires `eval_run_repo` into `app.state`, restores the `started_at` field the read-mapper currently drops, and adds the wire response models. `POST /evals/runs` is intentionally **deferred** (ADR 0014) — it would create eval incidents in the live DB, relies on the `make evals-reset` precondition the API can't guarantee, only runs in `eval_mode`, and duplicates the `make evals` CLI.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy async, pytest, ruff, mypy --strict.

**Spec references:** `sentinel-claude-code-prompt.md` §"API contracts" lines 330–333. Design: `plans/2026-06-15-api-surface-design.md`.

**Out of scope:** `POST /evals/runs` (deferred → ADR 0014); per-case drill-down (`EvalCaseResultRecord` has no read method yet); SSE/WS (PR 3).

---

## File structure

| File | Action | Responsibility |
|---|---|---|
| `sentinel/persistence/repositories.py` | modify | add `started_at` to `EvalRunRecord` + `_eval_run_record_from_model` |
| `sentinel/schemas/api.py` | modify | add `EvalRunDetail`; keep existing `EvalRunSummary` |
| `sentinel/api/routes/evals.py` | create | the three GET routes + record→wire mappers |
| `sentinel/api/app.py` | modify | construct + stash `eval_run_repo`; mount `evals_router` |
| `docs/adr/0014-defer-eval-run-trigger-endpoint.md` | create | ADR: why `POST /evals/runs` is deferred |
| `tests/unit/api/test_evals_route.py` | create | route unit tests (fake repo) |
| `tests/unit/schemas/test_api.py` | modify | `EvalRunDetail` model test |
| `tests/integration/persistence/test_eval_run_repository_e2e.py` | modify | assert `started_at` round-trips |
| `tests/integration/api/test_evals_api.py` | create | end-to-end against real DB |

---

## Task 1: Restore `started_at` on `EvalRunRecord`

**Files:**
- Modify: `sentinel/persistence/repositories.py` — `EvalRunRecord` (~line 133) + `_eval_run_record_from_model` (~line 1446)
- Test: `tests/integration/persistence/test_eval_run_repository_e2e.py`

> The `eval_runs` table + `EvalRunModel` have `started_at` (server_default now()), but the `EvalRunRecord` dataclass and its mapper drop it. The wire models need it (run timeline). Restore it.

- [ ] **Step 1: Write the failing assertion**

In `tests/integration/persistence/test_eval_run_repository_e2e.py`, find the test that calls `start_run(...)` then `get_run(...)` (there is an existing round-trip test). Add to that test's assertions on the fetched record (or add a focused test mirroring its fixture setup):

```python
    # started_at is populated by the DB server_default and surfaced on the record.
    assert fetched.started_at is not None
    from datetime import datetime

    assert isinstance(fetched.started_at, datetime)
```

(If the existing round-trip test's fetched record variable is named differently, match it. If no round-trip test exists, add one that calls `start_run(...)` → `get_run(run_id)` using the file's existing `eval_run_repo`/session fixtures.)

- [ ] **Step 2: Run to verify it fails**

Run: `make compose-up && pytest tests/integration/persistence/test_eval_run_repository_e2e.py -v -m integration --no-cov`
Expected: FAIL — `AttributeError: 'EvalRunRecord' object has no attribute 'started_at'`.

- [ ] **Step 3: Add the field**

In `sentinel/persistence/repositories.py`, in the `EvalRunRecord` dataclass, add `started_at: datetime` immediately before the trailing `completed_at: datetime | None = None` (a non-default field must precede the defaulted one):

```python
    extra: dict[str, Any]
    started_at: datetime
    completed_at: datetime | None = None
```

- [ ] **Step 4: Populate it in the mapper**

In `_eval_run_record_from_model` (the function that builds `EvalRunRecord` from an `EvalRunModel` row, ~line 1446), add `started_at=row.started_at` to the constructor call (alongside the other `row.<field>` assignments):

```python
        started_at=row.started_at,
        completed_at=row.completed_at,
```

- [ ] **Step 5: Run to verify it passes**

Run: `pytest tests/integration/persistence/test_eval_run_repository_e2e.py -v -m integration --no-cov`
Expected: PASS.

- [ ] **Step 6: Confirm no other caller breaks**

`started_at` is additive (no removed/renamed fields), so existing constructors of `EvalRunRecord` are unaffected only if they used keyword args. Check: `grep -rn "EvalRunRecord(" sentinel/ tests/` — if any positional construction exists, fix it to keyword. Run the full unit suite: `pytest tests/unit -q` (must stay green).

- [ ] **Step 7: Lint + typecheck + commit**

Run: `ruff format sentinel/persistence/repositories.py && ruff check sentinel/persistence/repositories.py && mypy sentinel/persistence/repositories.py`

```bash
git add sentinel/persistence/repositories.py tests/integration/persistence/test_eval_run_repository_e2e.py
git commit -m "$(cat <<'EOF'
feat(persistence): surface started_at on EvalRunRecord (DB had it; mapper dropped it)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `EvalRunDetail` response model

**Files:**
- Modify: `sentinel/schemas/api.py` (after `EvalRunSummary`, ~line 146)
- Test: `tests/unit/schemas/test_api.py`

> `EvalRunSummary` (existing) is the list-item shape. The detail/baseline endpoints return the full run: status, trigger, metrics, stability, regression verdict. `regression_detail` stays `dict[str, Any] | None` — an intentional opaque field (like `raw_payload`), since its shape varies by regression check.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/schemas/test_api.py`:

```python
def test_eval_run_detail_round_trips() -> None:
    from datetime import UTC, datetime

    from sentinel.schemas.api import EvalRunDetail

    d = EvalRunDetail(
        id=uuid4(),
        status="ok",
        trigger="baseline",
        git_sha="abc123",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=1,
        started_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 6, 15, 12, 2, tzinfo=UTC),
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_passed=True,
        regression_baseline_sha="def456",
        regression_detail={"category_match": {"delta": 0.0}},
    )
    dumped = d.model_dump(mode="json")
    assert dumped["status"] == "ok"
    assert dumped["metrics"]["category_match"] == 0.9
    assert dumped["regression_passed"] is True
```

Add `EvalRunDetail` to the file's top-level `from sentinel.schemas.api import (...)` block.

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/schemas/test_api.py -k eval_run_detail -v --no-cov`
Expected: FAIL — `ImportError: cannot import name 'EvalRunDetail'`.

- [ ] **Step 3: Add the model**

In `sentinel/schemas/api.py`, after `EvalRunSummary`, add (`Any`, `Literal`, `datetime`, `UUID`, `BaseModel`, `ConfigDict` are already imported):

```python
class EvalRunDetail(BaseModel):
    """Full eval-run record for the detail + baseline endpoints.

    `regression_detail` is an intentionally opaque object (its keys vary by
    regression check), the same documented exception as `raw_payload`.
    """

    model_config = ConfigDict(frozen=True)
    id: UUID
    status: Literal["running", "ok", "failed", "partial"]
    trigger: Literal["local", "ci-smoke", "ci-nightly", "baseline", "manual"]
    git_sha: str
    model: str
    prompt_version: str
    embedding_model_id: str
    corpus_version: str
    corpus_size: int
    shots_per_case: int
    started_at: datetime
    completed_at: datetime | None
    metrics: dict[str, float]
    metrics_stability: dict[str, float]
    regression_passed: bool | None
    regression_baseline_sha: str | None
    regression_detail: dict[str, Any] | None
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/schemas/test_api.py -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck + commit**

Run: `ruff format sentinel/schemas/api.py tests/unit/schemas/test_api.py && ruff check sentinel/schemas/api.py tests/unit/schemas/test_api.py && mypy sentinel/schemas/api.py`

```bash
git add sentinel/schemas/api.py tests/unit/schemas/test_api.py
git commit -m "$(cat <<'EOF'
feat(api): EvalRunDetail response model for eval run detail + baseline

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `GET /evals/runs` route + record→wire mappers

**Files:**
- Create: `sentinel/api/routes/evals.py`
- Test: `tests/unit/api/test_evals_route.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/api/test_evals_route.py`:

```python
"""Eval read routes against a fake EvalRunRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.evals import router as evals_router
from sentinel.persistence.repositories import EvalRunRecord


def _record(**overrides) -> EvalRunRecord:
    base = dict(
        id=uuid4(),
        status="ok",
        trigger="baseline",
        git_sha="abc123",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=1,
        fetcher_fixture_hash="fh",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha="def456",
        regression_passed=True,
        regression_detail={"category_match": {"delta": 0.0}},
        extra={},
        started_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        completed_at=datetime(2026, 6, 15, 12, 2, tzinfo=UTC),
    )
    base.update(overrides)
    return EvalRunRecord(**base)


def _app(repo: object) -> FastAPI:
    app = FastAPI()
    app.state.eval_run_repo = repo
    app.include_router(evals_router)
    return app


def test_list_runs_returns_summaries() -> None:
    rec = _record()
    repo = type("R", (), {})()
    repo.list_recent = AsyncMock(return_value=[rec])
    client = TestClient(_app(repo))
    resp = client.get("/evals/runs?limit=10")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data) == 1
    assert data[0]["id"] == str(rec.id)
    assert data[0]["summary"]["category_match"] == 0.9
    assert data[0]["started_at"] is not None
    repo.list_recent.assert_awaited_once_with(limit=10)


def test_list_runs_rejects_bad_limit() -> None:
    repo = type("R", (), {})()
    repo.list_recent = AsyncMock(return_value=[])
    client = TestClient(_app(repo))
    assert client.get("/evals/runs?limit=0").status_code == 422
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/api/test_evals_route.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'sentinel.api.routes.evals'`.

- [ ] **Step 3: Create the route module + list handler**

Create `sentinel/api/routes/evals.py`:

```python
# sentinel/api/routes/evals.py
"""Eval-run read routes — list / detail / baseline.

Read-only over `EvalRunRepository` (lifespan-stashed as
`request.app.state.eval_run_repo`). Triggering a run is intentionally not an
API operation — see docs/adr/0014-defer-eval-run-trigger-endpoint.md.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from sentinel.persistence.repositories import EvalRunRecord, EvalRunRepository
from sentinel.schemas.api import EvalRunDetail, EvalRunSummary

router = APIRouter(tags=["evals"])


def _summary(record: EvalRunRecord) -> EvalRunSummary:
    return EvalRunSummary(
        id=record.id,
        started_at=record.started_at,
        completed_at=record.completed_at,
        model=record.model,
        prompt_version=record.prompt_version,
        corpus_version=record.corpus_version,
        summary=record.metrics or None,
    )


def _detail(record: EvalRunRecord) -> EvalRunDetail:
    return EvalRunDetail(
        id=record.id,
        status=record.status,
        trigger=record.trigger,
        git_sha=record.git_sha,
        model=record.model,
        prompt_version=record.prompt_version,
        embedding_model_id=record.embedding_model_id,
        corpus_version=record.corpus_version,
        corpus_size=record.corpus_size,
        shots_per_case=record.shots_per_case,
        started_at=record.started_at,
        completed_at=record.completed_at,
        metrics=record.metrics,
        metrics_stability=record.metrics_stability,
        regression_passed=record.regression_passed,
        regression_baseline_sha=record.regression_baseline_sha,
        regression_detail=record.regression_detail,
    )


@router.get("/evals/runs", response_model=list[EvalRunSummary])
async def list_eval_runs(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[EvalRunSummary]:
    repo: EvalRunRepository = request.app.state.eval_run_repo
    records = await repo.list_recent(limit=limit)
    return [_summary(r) for r in records]
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/api/test_evals_route.py -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck + commit**

Run: `ruff format sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && ruff check sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && mypy sentinel/api/routes/evals.py`

```bash
git add sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py
git commit -m "$(cat <<'EOF'
feat(api): GET /evals/runs list with record->summary mapper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `GET /evals/runs/{run_id}` detail route

**Files:**
- Modify: `sentinel/api/routes/evals.py`
- Test: `tests/unit/api/test_evals_route.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_evals_route.py`:

```python
def test_get_run_returns_detail() -> None:
    rec = _record()
    repo = type("R", (), {})()
    repo.get_run = AsyncMock(return_value=rec)
    client = TestClient(_app(repo))
    resp = client.get(f"/evals/runs/{rec.id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == str(rec.id)
    assert data["status"] == "ok"
    assert data["metrics"]["category_match"] == 0.9
    assert data["regression_passed"] is True
    repo.get_run.assert_awaited_once_with(rec.id)


def test_get_run_404_when_missing() -> None:
    repo = type("R", (), {})()
    repo.get_run = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get(f"/evals/runs/{uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "eval_run_not_found"
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/api/test_evals_route.py -k get_run -v --no-cov`
Expected: FAIL (route not defined → 404 for wrong reason / 405).

- [ ] **Step 3: Add the detail handler**

Append to `sentinel/api/routes/evals.py`:

```python
@router.get(
    "/evals/runs/{run_id}",
    response_model=EvalRunDetail,
    responses={404: {"description": "Eval run not found"}},
)
async def get_eval_run(run_id: UUID, request: Request) -> EvalRunDetail:
    repo: EvalRunRepository = request.app.state.eval_run_repo
    record = await repo.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="eval_run_not_found")
    return _detail(record)
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/api/test_evals_route.py -v --no-cov`
Expected: PASS (all eval-route tests).

- [ ] **Step 5: Lint + typecheck + commit**

Run: `ruff format sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && ruff check sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && mypy sentinel/api/routes/evals.py`

```bash
git add sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py
git commit -m "$(cat <<'EOF'
feat(api): GET /evals/runs/{id} detail

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `GET /evals/baseline` route

**Files:**
- Modify: `sentinel/api/routes/evals.py`
- Test: `tests/unit/api/test_evals_route.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_evals_route.py`:

```python
def test_baseline_returns_detail() -> None:
    rec = _record(trigger="baseline")
    repo = type("R", (), {})()
    repo.get_latest_ok_run = AsyncMock(return_value=rec)
    client = TestClient(_app(repo))
    resp = client.get("/evals/baseline")
    assert resp.status_code == 200, resp.text
    assert resp.json()["trigger"] == "baseline"
    repo.get_latest_ok_run.assert_awaited_once_with(trigger="baseline")


def test_baseline_404_when_none() -> None:
    repo = type("R", (), {})()
    repo.get_latest_ok_run = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get("/evals/baseline")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "no_baseline"
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/api/test_evals_route.py -k baseline -v --no-cov`
Expected: FAIL (route not defined).

- [ ] **Step 3: Add the baseline handler**

Append to `sentinel/api/routes/evals.py`:

```python
@router.get(
    "/evals/baseline",
    response_model=EvalRunDetail,
    responses={404: {"description": "No baseline run recorded"}},
)
async def get_eval_baseline(request: Request) -> EvalRunDetail:
    repo: EvalRunRepository = request.app.state.eval_run_repo
    record = await repo.get_latest_ok_run(trigger="baseline")
    if record is None:
        raise HTTPException(status_code=404, detail="no_baseline")
    return _detail(record)
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/unit/api/test_evals_route.py -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck + commit**

Run: `ruff format sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && ruff check sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py && mypy sentinel/api/routes/evals.py`

```bash
git add sentinel/api/routes/evals.py tests/unit/api/test_evals_route.py
git commit -m "$(cat <<'EOF'
feat(api): GET /evals/baseline (latest ok baseline run)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Wire `eval_run_repo` + mount the router

**Files:**
- Modify: `sentinel/api/app.py` (lifespan + `build_app`)
- Test: `tests/integration/api/test_evals_api.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/api/test_evals_api.py`:

```python
"""Eval read routes end-to-end against the real app + a seeded run."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_evals_read_surface_end_to_end(client, app) -> None:
    # Seed a finished baseline run directly via the real repo on app.state.
    from sentinel.persistence.repositories import PostgresEvalRunRepository

    repo: PostgresEvalRunRepository = app.state.eval_run_repo
    run_id = await repo.start_run(
        status="running",
        trigger="baseline",
        git_sha="testsha",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=1,
        fetcher_fixture_hash="fh",
        extra={},
    )
    await repo.finalize_run(
        run_id,
        status="ok",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha="testsha",
        regression_passed=True,
        regression_detail={"ok": True},
    )

    # list
    listed = await client.get("/evals/runs")
    assert listed.status_code == 200, listed.text
    assert any(r["id"] == str(run_id) for r in listed.json())

    # detail
    detail = await client.get(f"/evals/runs/{run_id}")
    assert detail.status_code == 200
    assert detail.json()["status"] == "ok"
    assert detail.json()["metrics"]["category_match"] == 0.9

    # baseline (the run we seeded is trigger=baseline, status=ok)
    baseline = await client.get("/evals/baseline")
    assert baseline.status_code == 200
    assert baseline.json()["id"] == str(run_id)


async def test_get_unknown_eval_run_404(client) -> None:
    resp = await client.get("/evals/runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "eval_run_not_found"
```

> Reuse the `client` + `app` fixtures from `tests/integration/api/conftest.py`. The `eval_runs` table is migrated by that conftest's session-scoped migration. Add `eval_runs` / `eval_case_results` truncation to the conftest's `_reset_state` (the `TRUNCATE incidents, diagnoses, outbox_events CASCADE` line) so seeded runs don't bleed across tests — change it to `TRUNCATE incidents, diagnoses, outbox_events, eval_runs, eval_case_results CASCADE`.

- [ ] **Step 2: Run to verify it fails**

Run: `make compose-up && pytest tests/integration/api/test_evals_api.py -v -m integration --no-cov`
Expected: FAIL — `AttributeError: ... 'eval_run_repo'` (not on app.state) / routes 404 (not mounted).

- [ ] **Step 3: Construct + stash `eval_run_repo` in the lifespan**

In `sentinel/api/app.py`, the lifespan already imports `PostgresEvalRunRepository`? If not, add it to the existing `from sentinel.persistence.repositories import (...)` block:

```python
    PostgresEvalRunRepository,
```

Near where `incident_repo`/`resolution_repo` are constructed + stashed (~line 157–165), add:

```python
    app.state.eval_run_repo = PostgresEvalRunRepository(session_factory)
```

- [ ] **Step 4: Mount the router**

Add the import alongside the other route imports near the top of `app.py`:

```python
from sentinel.api.routes.evals import router as evals_router
```

In `build_app()`, after the other `app.include_router(...)` calls, add:

```python
    app.include_router(evals_router)
```

- [ ] **Step 5: Update the conftest truncation**

In `tests/integration/api/conftest.py`, change the `_reset_state` truncate to include the eval tables:

```python
            await conn.execute(
                text("TRUNCATE incidents, diagnoses, outbox_events, eval_runs, eval_case_results CASCADE")
            )
```

- [ ] **Step 6: Run to verify it passes**

Run: `pytest tests/integration/api/test_evals_api.py -v -m integration --no-cov`
Expected: PASS (both tests).

- [ ] **Step 7: Regression + lint + typecheck**

Run: `pytest tests/unit -q` (green), then `make lint && make typecheck`.

- [ ] **Step 8: Commit**

```bash
git add sentinel/api/app.py tests/integration/api/
git commit -m "$(cat <<'EOF'
feat(api): wire eval_run_repo + mount evals router; e2e read test

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: ADR 0014 — defer `POST /evals/runs`

**Files:**
- Create: `docs/adr/0014-defer-eval-run-trigger-endpoint.md`

- [ ] **Step 1: Write the ADR**

Create `docs/adr/0014-defer-eval-run-trigger-endpoint.md` (match the format of `docs/adr/0013-replay-backed-post-execution.md`):

```markdown
# 0014 — Defer POST /evals/runs (eval runs are CLI-triggered)

**Status:** Accepted
**Date:** 2026-06-15

## Context
The spec's API contract lists `POST /evals/runs`. The eval runner's canonical
shot fires a real webhook through the full pipeline, creating incident +
diagnosis rows, and the corpus run depends on a clean Kafka topic (the workflow
runs `make evals-reset` — delete topic + flush Redis + truncate Postgres —
before each run). It also only functions in `eval_mode` (cassette replay + the
in-process ASGI client + the diagnosis consumer running).

## Decision
Ship the eval **read** surface (`GET /evals/runs`, `/runs/{id}`, `/baseline`)
and **defer** `POST /evals/runs`. Triggering a corpus run stays a CLI operation
(`make evals` / `sentinel.evals.cli`):
- An API-triggered run would write eval incidents into whatever DB the app
  points at (prod pollution), and cannot guarantee the `evals-reset`
  precondition, so stale Kafka messages could corrupt results.
- It duplicates the CLI, which already assembles the run deterministically.
- The eval dashboard's value is *reading* run history + the baseline, which the
  read endpoints fully serve.

## Consequences
- The dashboard (Work Area L) reads runs/baseline over HTTP; runs are produced
  by CI (nightly + the evals-gate) and the local CLI, then surfaced read-only.
- If an API trigger is ever needed, it must be `eval_mode`-gated, run against a
  dedicated eval database, and own the topic/Redis reset — a larger change with
  its own ADR.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0014-defer-eval-run-trigger-endpoint.md
git commit -m "$(cat <<'EOF'
docs(adr): 0014 defer POST /evals/runs (eval runs are CLI-triggered)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final verification (before PR)

- [ ] `make lint && make typecheck && pytest tests/unit -q` → all green.
- [ ] `make compose-up && pytest tests/integration/api tests/integration/persistence/test_eval_run_repository_e2e.py -v -m integration --no-cov` → green.
- [ ] Mandatory subagent code review before the PR; address findings, re-run the gate.
- [ ] Open PR: `feat(api): evals read surface (Work Area I, PR 2b)`. Body notes the deferred POST (ADR 0014) and the `started_at` restoration.

## Self-review notes (planning)

- **Spec coverage:** `GET /evals/runs` ✅(T3), `GET /evals/runs/{id}` ✅(T4), `GET /evals/baseline` ✅(T5). `POST /evals/runs` deferred ✅(T7, ADR 0014, your explicit decision). Wiring ✅(T6). `started_at` gap closed ✅(T1).
- **Type consistency:** `EvalRunRecord` gains `started_at: datetime`; `_summary`/`_detail` map it; `EvalRunSummary.summary = record.metrics or None`; `EvalRunDetail` mirrors `EvalRunRecord`'s status/trigger Literals + metrics dicts; repo methods used (`list_recent(limit=)`, `get_run(id)`, `get_latest_ok_run(trigger="baseline")`) match the verified signatures.
- **No placeholders:** every code step is complete. The one conditional ("if `PostgresEvalRunRepository` not already imported in app.py") is a concrete check, not a vague directive.
- **Invariants:** Pydantic on every boundary (`regression_detail: dict[str, Any] | None` is the documented opaque exception, like `raw_payload`); no raw SQL outside `persistence/` (the conftest TRUNCATE is test code); read-only routes (no writes, no remediation).
- **YAGNI:** per-case drill-down deferred (no repo read method for `EvalCaseResultRecord`); no pagination envelope for the run list (few runs; `limit` suffices).
