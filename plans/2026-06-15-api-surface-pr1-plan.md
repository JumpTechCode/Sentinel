# API Surface PR 1 — Read Surface + Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the read/CRUD surface for incidents (`GET /incidents`, `GET /incidents/{id}`, `POST /incidents`), expose Prometheus at `GET /metrics`, extend `/readyz` with Postgres + Redis probes, and wire the incident repository into `app.state` — with zero Anthropic spend.

**Architecture:** New route modules under `sentinel/api/routes/` mounted in `build_app()`, following the established `request.app.state.<repo>` DI pattern (no FastAPI `Depends` graph — mirror `resolve.py`/`webhooks.py`). Handlers are thin; querying lives in `PostgresIncidentRepository`. `POST /incidents` routes through the same fingerprint + `ingest()` path as webhooks so manual incidents dedup and flow through the outbox→Kafka→enrich→diagnose pipeline identically. `GET /incidents/{id}` returns the incident plus its enrichment context; **diagnosis population is deferred to PR 2** (the wire `Diagnosis` model requires `evidence: min_length=1`, which a fully-hallucinated `PersistedDiagnosis` cannot satisfy — PR 2 introduces the relaxed view model that owns this).

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy async, prometheus_client, pytest, ruff, mypy --strict.

**Spec references:** `sentinel-claude-code-prompt.md` §"API contracts" (lines 321–337). Design: `plans/2026-06-15-api-surface-design.md`.

**Out of scope (later PRs):** `/diagnose`, `/evals/*`, SSE `/stream`, WS `/ws/incidents`, diagnosis population in detail (PR 2); Kafka-as-explicit-probe (represented here by consumer aliveness).

---

## File structure

| File | Action | Responsibility |
|---|---|---|
| `sentinel/schemas/api.py` | modify | add `IncidentListResponse` envelope |
| `sentinel/persistence/repositories.py` | modify | add `list_incidents(...)` to `IncidentRepository` protocol + `PostgresIncidentRepository` |
| `sentinel/api/routes/incidents.py` | create | `GET /incidents`, `GET /incidents/{id}`, `POST /incidents` |
| `sentinel/api/routes/metrics.py` | create | `GET /metrics` (Prometheus exposition) |
| `sentinel/api/routes/health.py` | modify | extend `/readyz` with Postgres + Redis probes |
| `sentinel/api/app.py` | modify | stash `incident_repo` in `app.state`; mount `incidents_router` + `metrics_router` |
| `scripts/export_openapi.py` | create | dump `build_app().openapi()` to `openapi.json` |
| `Makefile` | modify | real `openapi` target (replace placeholder) |
| `tests/unit/api/test_incidents_route.py` | create | handler unit tests (fake repo) |
| `tests/unit/api/test_metrics_route.py` | create | metrics endpoint unit test |
| `tests/unit/test_health.py` | modify | update `/readyz` test for new probe behavior |
| `tests/unit/schemas/test_api.py` | modify | `IncidentListResponse` model test |
| `tests/integration/api/test_incidents_api.py` | create | end-to-end against testcontainers (real wiring) |
| `tests/unit/api/test_openapi.py` | create | `build_app().openapi()` generates + contains new paths |

---

## Task 1: Add `IncidentListResponse` schema

**Files:**
- Modify: `sentinel/schemas/api.py` (after `IncidentListItem`, ~line 70)
- Test: `tests/unit/schemas/test_api.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/schemas/test_api.py`:

```python
def test_incident_list_response_round_trips() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from sentinel.schemas.api import IncidentListItem, IncidentListResponse

    item = IncidentListItem(
        id=uuid4(),
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        opened_at=datetime.now(UTC),
    )
    resp = IncidentListResponse(items=[item], total=1, limit=50, offset=0)
    dumped = resp.model_dump(mode="json")
    assert dumped["total"] == 1
    assert dumped["limit"] == 50
    assert dumped["offset"] == 0
    assert len(dumped["items"]) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/schemas/test_api.py::test_incident_list_response_round_trips -v`
Expected: FAIL with `ImportError: cannot import name 'IncidentListResponse'`

- [ ] **Step 3: Add the model**

In `sentinel/schemas/api.py`, immediately after the `IncidentListItem` class (line 70), add:

```python
class IncidentListResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    items: list[IncidentListItem]
    total: int
    limit: int
    offset: int
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/schemas/test_api.py::test_incident_list_response_round_trips -v`
Expected: PASS

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/schemas/api.py tests/unit/schemas/test_api.py && ruff check sentinel/schemas/api.py && mypy sentinel/schemas/api.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/schemas/api.py tests/unit/schemas/test_api.py
git commit -m "feat(api): add IncidentListResponse pagination envelope"
```

---

## Task 2: Add `list_incidents()` to the incident repository

**Files:**
- Modify: `sentinel/persistence/repositories.py` — `IncidentRepository` protocol (~line 394) and `PostgresIncidentRepository` (after `list_recent`, ~line 505)
- Test: `tests/integration/persistence/test_incident_repo.py` (create if absent; this needs a real Postgres for the SQL)

> **Why integration, not unit:** the filter/pagination logic is SQL (`WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`/`COUNT`). Testing it against a fake adds no confidence; the existing persistence tests run against testcontainers Postgres. Follow `tests/integration/persistence/*` fixtures (look for the `incident_repo` / `session_factory` fixture already used by resolution/enrichment repo tests).

- [ ] **Step 1: Write the failing test**

Create/append `tests/integration/persistence/test_incident_repo.py`:

```python
import pytest

pytestmark = pytest.mark.integration


async def test_list_incidents_filters_and_paginates(incident_repo, seed_incident) -> None:
    # seed_incident is a fixture that inserts an incident and returns its NormalizedAlert
    # inputs; create three with distinct service/severity/status via repo.ingest(...).
    # (Reuse whatever seeding helper the sibling repo tests use; if none, insert via
    #  incident_repo.create_from_alert with crafted NormalizedAlerts.)
    items, total = await incident_repo.list_incidents(service="checkout", limit=2, offset=0)
    assert total >= 1
    assert all(i.service == "checkout" for i in items)
    assert len(items) <= 2

    items_sev, _ = await incident_repo.list_incidents(severity="SEV1")
    assert all(i.severity == "SEV1" for i in items_sev)
```

> If `tests/integration/persistence/` has no incident-repo fixture, add one in that directory's `conftest.py` mirroring the resolution-repo fixture: build `PostgresIncidentRepository(session_factory)` from the testcontainers `session_factory`, and seed via `create_from_alert`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make compose-up && pytest tests/integration/persistence/test_incident_repo.py -v -m integration`
Expected: FAIL with `AttributeError: 'PostgresIncidentRepository' object has no attribute 'list_incidents'`

- [ ] **Step 3: Add the protocol method**

In `sentinel/persistence/repositories.py`, in the `IncidentRepository(Protocol)` block, after the `list_recent` line (394), add:

```python
    async def list_incidents(
        self,
        *,
        status: IncidentStatusType | None = None,
        service: str | None = None,
        severity: SeverityType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[IncidentListItem], int]: ...
```

Ensure `IncidentStatusType` and `SeverityType` are imported at the top of `repositories.py` (they are used by the API schemas; add `from sentinel.schemas.enums import IncidentStatusType, SeverityType` to the existing enums import if not already present).

- [ ] **Step 4: Add the implementation**

In `PostgresIncidentRepository`, immediately after `list_recent` (after line 505), add:

```python
    async def list_incidents(
        self,
        *,
        status: IncidentStatusType | None = None,
        service: str | None = None,
        severity: SeverityType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[IncidentListItem], int]:
        async with self._session_factory() as s:
            conditions = []
            if status is not None:
                conditions.append(IncidentModel.status == status)
            if service is not None:
                conditions.append(IncidentModel.service == service)
            if severity is not None:
                conditions.append(IncidentModel.severity == severity)

            stmt = select(IncidentModel)
            if conditions:
                stmt = stmt.where(*conditions)
            stmt = stmt.order_by(IncidentModel.opened_at.desc()).limit(limit).offset(offset)
            rows = (await s.execute(stmt)).scalars().all()

            count_stmt = select(func.count()).select_from(IncidentModel)
            if conditions:
                count_stmt = count_stmt.where(*conditions)
            total = (await s.execute(count_stmt)).scalar_one()

            items = [
                IncidentListItem(
                    id=row.id,
                    service=row.service,
                    severity=row.severity,
                    status=row.status,
                    title=row.title,
                    opened_at=row.opened_at,
                    resolved_at=row.resolved_at,
                )
                for row in rows
            ]
            return items, total
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/integration/persistence/test_incident_repo.py -v -m integration`
Expected: PASS

- [ ] **Step 6: Lint + typecheck**

Run: `ruff format sentinel/persistence/repositories.py && ruff check sentinel/persistence/repositories.py && mypy sentinel/persistence/repositories.py`
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add sentinel/persistence/repositories.py tests/integration/persistence/
git commit -m "feat(persistence): list_incidents with status/service/severity filters + pagination"
```

---

## Task 3: `GET /incidents` route

**Files:**
- Create: `sentinel/api/routes/incidents.py`
- Test: `tests/unit/api/test_incidents_route.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/api/test_incidents_route.py`:

```python
"""Incident routes — list/detail/create against a fake repository."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.incidents import router as incidents_router
from sentinel.schemas.api import IncidentListItem


def _app(repo: object) -> FastAPI:
    app = FastAPI()
    app.state.incident_repo = repo
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(incidents_router)
    return app


def _item() -> IncidentListItem:
    return IncidentListItem(
        id=uuid4(),
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        opened_at=datetime.now(UTC),
    )


def test_list_returns_envelope() -> None:
    item = _item()
    repo = type("R", (), {})()
    repo.list_incidents = AsyncMock(return_value=([item], 1))
    client = TestClient(_app(repo))
    resp = client.get("/incidents?service=checkout&limit=10&offset=0")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 1
    assert data["limit"] == 10
    assert data["offset"] == 0
    assert data["items"][0]["service"] == "checkout"
    repo.list_incidents.assert_awaited_once_with(
        status=None, service="checkout", severity="SEV1" if False else None, limit=10, offset=0
    )


def test_list_rejects_bad_limit() -> None:
    repo = type("R", (), {})()
    repo.list_incidents = AsyncMock(return_value=([], 0))
    client = TestClient(_app(repo))
    resp = client.get("/incidents?limit=0")
    assert resp.status_code == 422
```

> Note: the `assert_awaited_once_with` above pins the default kwargs; keep `severity=None` (drop the inline conditional — it's there only to read clearly). Simplify to `severity=None` when writing.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_incidents_route.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sentinel.api.routes.incidents'`

- [ ] **Step 3: Create the route module with the list handler**

Create `sentinel/api/routes/incidents.py`:

```python
# sentinel/api/routes/incidents.py
"""Incident read/create routes.

`GET /incidents`        — filtered, paginated list.
`GET /incidents/{id}`   — detail incl. enrichment context (diagnoses land in PR 2).
`POST /incidents`       — manual create through the fingerprint + ingest path.

All handlers are thin: querying lives in `IncidentRepository`. The repo is
retrieved from `request.app.state.incident_repo` (lifespan-stashed), matching
the `resolve.py` / `webhooks.py` dependency pattern.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from sentinel.persistence.repositories import IncidentRepository
from sentinel.schemas.api import (
    IncidentDetailResponse,
    IncidentListResponse,
)
from sentinel.schemas.enums import IncidentStatusType, SeverityType

router = APIRouter(tags=["incidents"])


@router.get("/incidents", response_model=IncidentListResponse)
async def list_incidents(
    request: Request,
    status: IncidentStatusType | None = Query(default=None),
    service: str | None = Query(default=None),
    severity: SeverityType | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> IncidentListResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    items, total = await repo.list_incidents(
        status=status,
        service=service,
        severity=severity,
        limit=limit,
        offset=offset,
    )
    return IncidentListResponse(items=items, total=total, limit=limit, offset=offset)
```

> When writing the test, simplify the `assert_awaited_once_with` to `severity=None` and call `GET /incidents?service=checkout&severity=SEV1&limit=10&offset=0` so the asserted kwargs match the query.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/api/test_incidents_route.py::test_list_returns_envelope tests/unit/api/test_incidents_route.py::test_list_rejects_bad_limit -v`
Expected: PASS

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && ruff check sentinel/api/routes/incidents.py && mypy sentinel/api/routes/incidents.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py
git commit -m "feat(api): GET /incidents list with filters + pagination"
```

---

## Task 4: `GET /incidents/{id}` detail route (incident + context)

**Files:**
- Modify: `sentinel/api/routes/incidents.py`
- Test: `tests/unit/api/test_incidents_route.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_incidents_route.py`:

```python
def _detail(incident_id) -> object:
    from sentinel.schemas.api import IncidentDetailResponse

    return IncidentDetailResponse(
        id=incident_id,
        source="sentry",
        external_id="ext-1",
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        fingerprint="f" * 64,
        opened_at=datetime.now(UTC),
    )


def test_detail_returns_incident_with_no_context() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get(f"/incidents/{incident_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == str(incident_id)
    assert data["context"] is None
    assert data["diagnoses"] == []


def test_detail_returns_404_when_missing() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=None)
    repo.get_enrichment_context = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get(f"/incidents/{incident_id}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_incidents_route.py::test_detail_returns_incident_with_no_context -v`
Expected: FAIL with 404/405 (route not defined)

- [ ] **Step 3: Add the detail handler**

Append to `sentinel/api/routes/incidents.py` (after `list_incidents`):

```python
@router.get(
    "/incidents/{incident_id}",
    response_model=IncidentDetailResponse,
    responses={404: {"description": "Incident not found"}},
)
async def get_incident(incident_id: UUID, request: Request) -> IncidentDetailResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    detail = await repo.get(incident_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="incident_not_found")
    # Enrichment context is stored separately (JSONB read-back); fold it in.
    # Diagnoses are deferred to PR 2 — the wire `Diagnosis` model requires
    # evidence min_length=1, which a fully-hallucinated PersistedDiagnosis
    # cannot satisfy. PR 2 introduces the relaxed view model.
    stored = await repo.get_enrichment_context(incident_id)
    if stored is not None:
        detail = detail.model_copy(update={"context": stored.context})
    return detail
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/api/test_incidents_route.py -k detail -v`
Expected: PASS (both detail tests)

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && ruff check sentinel/api/routes/incidents.py && mypy sentinel/api/routes/incidents.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py
git commit -m "feat(api): GET /incidents/{id} detail with enrichment context"
```

---

## Task 5: `POST /incidents` create route (through fingerprint + ingest)

**Files:**
- Modify: `sentinel/api/routes/incidents.py`
- Test: `tests/unit/api/test_incidents_route.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_incidents_route.py`:

```python
def _create_body() -> dict:
    return {
        "source": "generic",
        "external_id": "manual-1",
        "service": "checkout",
        "severity": "SEV2",
        "title": "manual incident",
        "raw_payload": {"note": "filed by operator"},
    }


def test_create_returns_201_for_new_incident() -> None:
    from sentinel.persistence.repositories import IngestResult

    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.ingest = AsyncMock(
        return_value=IngestResult(incident_id=incident_id, event_kind="opened")
    )
    client = TestClient(_app(repo))
    resp = client.post("/incidents", json=_create_body())
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["incident_id"] == str(incident_id)
    # Verify it went through ingest with a fingerprint + payload_hash.
    kwargs = repo.ingest.await_args.kwargs
    assert kwargs["outbox_topic"] == "sentinel.incidents"
    assert len(kwargs["fingerprint"]) == 64
    assert len(kwargs["payload_hash"]) == 64


def test_create_returns_200_for_recurred() -> None:
    from sentinel.persistence.repositories import IngestResult

    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.ingest = AsyncMock(
        return_value=IngestResult(incident_id=incident_id, event_kind="recurred")
    )
    client = TestClient(_app(repo))
    resp = client.post("/incidents", json=_create_body())
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "recurred"


def test_create_rejects_unknown_field() -> None:
    repo = type("R", (), {})()
    repo.ingest = AsyncMock()
    client = TestClient(_app(repo))
    body = _create_body() | {"bogus": 1}
    resp = client.post("/incidents", json=body)
    assert resp.status_code == 422  # CreateIncidentRequest has extra="forbid"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_incidents_route.py -k create -v`
Expected: FAIL (route not defined → 404/405)

- [ ] **Step 3: Add the create handler**

Update the imports at the top of `sentinel/api/routes/incidents.py`:

```python
import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from sentinel.ingestion.fingerprint import fingerprint, normalize_title
from sentinel.persistence.repositories import IncidentRepository
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import (
    CreateIncidentRequest,
    IncidentDetailResponse,
    IncidentListResponse,
    WebhookAcceptedResponse,
)
from sentinel.schemas.enums import IncidentStatusType, SeverityType
```

Append the handler:

```python
@router.post(
    "/incidents",
    response_model=None,
    status_code=201,
    responses={
        200: {"model": WebhookAcceptedResponse, "description": "Recurred (deduped)"},
        201: {"model": WebhookAcceptedResponse, "description": "Created"},
        422: {"description": "Invalid payload"},
    },
)
async def create_incident(body: CreateIncidentRequest, request: Request) -> JSONResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    outbox_topic: str = request.app.state.outbox_topic
    # Build the canonical alert from the validated request. received_at is set
    # server-side (manual creation has no wire timestamp).
    alert = NormalizedAlert(
        source=body.source,
        external_id=body.external_id,
        service=body.service,
        severity=body.severity,
        title=body.title,
        received_at=datetime.now(UTC),
        raw_payload=body.raw_payload,
    )
    # Same fingerprint contract as the webhook path (fingerprint.py): manual
    # incidents dedup against webhook incidents within the 1h window.
    fp = fingerprint(alert.service, normalize_title(alert.title), alert.severity)
    # Manual creation has no raw HTTP body; hash the canonical JSON payload so
    # the ingest contract (payload_hash) is satisfied deterministically.
    payload_hash = sha256(
        json.dumps(body.raw_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    result = await repo.ingest(
        alert,
        fingerprint=fp,
        outbox_topic=outbox_topic,
        payload_hash=payload_hash,
    )
    wire_status = "recurred" if result.event_kind == "recurred" else "accepted"
    status_code = 200 if result.event_kind == "recurred" else 201
    return JSONResponse(
        status_code=status_code,
        content=WebhookAcceptedResponse(
            status=wire_status, incident_id=result.incident_id
        ).model_dump(mode="json"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/api/test_incidents_route.py -v`
Expected: PASS (all incident-route tests)

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && ruff check sentinel/api/routes/incidents.py && mypy sentinel/api/routes/incidents.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py
git commit -m "feat(api): POST /incidents manual create through fingerprint+ingest path"
```

---

## Task 6: `GET /metrics` route

**Files:**
- Create: `sentinel/api/routes/metrics.py`
- Test: `tests/unit/api/test_metrics_route.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/api/test_metrics_route.py`:

```python
"""GET /metrics exposes the default Prometheus registry."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.metrics import router as metrics_router


def test_metrics_exposes_registry() -> None:
    # Touch a known metric so it appears in the exposition output.
    from sentinel.observability.metrics import webhooks_total

    webhooks_total.labels(source="generic", status="accepted").inc()

    app = FastAPI()
    app.include_router(metrics_router)
    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "sentinel_webhooks_total" in resp.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_metrics_route.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sentinel.api.routes.metrics'`

- [ ] **Step 3: Create the route**

Create `sentinel/api/routes/metrics.py`:

```python
# sentinel/api/routes/metrics.py
"""GET /metrics — Prometheus exposition of the default registry.

The metrics module (`sentinel/observability/metrics.py`) registers all
collectors on the prometheus_client default REGISTRY at import time. This route
is a read-only scrape; it must never block request handlers.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["observability"])


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/api/test_metrics_route.py -v`
Expected: PASS

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/metrics.py tests/unit/api/test_metrics_route.py && ruff check sentinel/api/routes/metrics.py && mypy sentinel/api/routes/metrics.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/metrics.py tests/unit/api/test_metrics_route.py
git commit -m "feat(api): GET /metrics Prometheus exposition"
```

---

## Task 7: Extend `/readyz` with Postgres + Redis probes

**Files:**
- Modify: `sentinel/api/routes/health.py`
- Test: `tests/unit/test_health.py` (update existing) + `tests/unit/api/test_readyz_probes.py` (create)

> **Behavior change:** `/readyz` previously returned 503/"unknown" whenever `consumer_alive` was empty. New behavior: it probes Postgres (`app.state.engine`) and Redis (`app.state.redis`) and includes per-consumer aliveness; status is `ok` (200) iff every check is `ok`, else `degraded` (503). Kafka readiness is represented by consumer aliveness — a dead broker flips consumers dead (documented; an explicit Kafka probe is deferred). A bare `build_app()` with no lifespan has no engine/redis on state, so both probe as `dead` → still 503 (rollout-safe default preserved).

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/api/test_readyz_probes.py`:

```python
"""readyz dependency probes — Postgres + Redis + consumer aliveness."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.health import router as health_router


class _FakeConn:
    async def execute(self, *_a, **_k) -> None:
        return None


class _FakeEngine:
    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    @asynccontextmanager
    async def connect(self):
        if self._fail:
            raise RuntimeError("pg down")
        yield _FakeConn()


def _app(*, engine, redis, consumers) -> FastAPI:
    app = FastAPI()
    app.state.engine = engine
    app.state.redis = redis
    app.state.consumer_alive = consumers
    app.include_router(health_router)
    return app


def test_readyz_ok_when_all_healthy() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(), redis=redis, consumers={"diagnosis": True})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"] == "ok"
    assert body["checks"]["consumer:diagnosis"] == "ok"


def test_readyz_503_when_postgres_down() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(fail=True), redis=redis, consumers={})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["checks"]["postgres"] == "dead"


def test_readyz_503_when_consumer_dead() -> None:
    redis = type("Rd", (), {})()
    redis.ping = AsyncMock(return_value=True)
    app = _app(engine=_FakeEngine(), redis=redis, consumers={"diagnosis": False})
    resp = TestClient(app).get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["checks"]["consumer:diagnosis"] == "dead"
```

Update `tests/unit/test_health.py::test_readyz_reports_dependencies` to match the new bare-app behavior:

```python
def test_readyz_503_without_lifespan() -> None:
    """Bare build_app() has no engine/redis on state → probes fail → 503."""
    app = build_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"] == "dead"
    assert body["checks"]["redis"] == "dead"
```

(Delete the old `test_readyz_reports_dependencies` body — replace it with the above.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/api/test_readyz_probes.py tests/unit/test_health.py -v`
Expected: FAIL (probes not implemented; old test asserted "unknown")

- [ ] **Step 3: Rewrite the readyz handler**

Replace the body of `sentinel/api/routes/health.py` from the `readyz` function down with:

```python
import asyncio

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_PROBE_TIMEOUT_S = 2.0


async def _check_postgres(engine: AsyncEngine | None) -> CheckStatus:
    if engine is None:
        return "dead"
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return "ok"
    except Exception:
        return "dead"


async def _check_redis(redis: Redis | None) -> CheckStatus:
    if redis is None:
        return "dead"
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            await redis.ping()
        return "ok"
    except Exception:
        return "dead"


@router.get(
    "/readyz",
    response_model=Readyz,
    responses={
        200: {"description": "All dependency checks passed", "model": Readyz},
        503: {"description": "One or more dependency checks failing", "model": Readyz},
    },
)
async def readyz(request: Request) -> JSONResponse:
    state = request.app.state
    engine = getattr(state, "engine", None)
    redis = getattr(state, "redis", None)
    alive = getattr(state, "consumer_alive", None)

    pg, rd = await asyncio.gather(_check_postgres(engine), _check_redis(redis))
    checks: dict[str, CheckStatus] = {"postgres": pg, "redis": rd}
    if isinstance(alive, dict):
        for name, ok in alive.items():
            checks[f"consumer:{name}"] = "ok" if ok else "dead"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        content=Readyz(status="ok" if all_ok else "degraded", checks=checks).model_dump(),
        status_code=200 if all_ok else 503,
    )
```

Add the new imports at the top of the file alongside the existing ones (`Request` is already imported; add `asyncio`, `Redis`, `text`, `AsyncEngine`). Update the module docstring's final line (currently "Pg/redis/kafka dependency probes land alongside this in Work Area I.") to state that Postgres and Redis are now probed directly and Kafka readiness is represented by consumer aliveness.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/api/test_readyz_probes.py tests/unit/test_health.py -v`
Expected: PASS

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/health.py tests/unit/api/test_readyz_probes.py tests/unit/test_health.py && ruff check sentinel/api/routes/health.py && mypy sentinel/api/routes/health.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/health.py tests/unit/api/test_readyz_probes.py tests/unit/test_health.py
git commit -m "feat(api): readyz probes Postgres + Redis (Kafka via consumer aliveness)"
```

---

## Task 8: Wire repos + routers into the app, with an integration test

**Files:**
- Modify: `sentinel/api/app.py` (lifespan ~line 162; `build_app` ~line 506; imports ~line 57)
- Test: `tests/integration/api/test_incidents_api.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/api/test_incidents_api.py`:

```python
"""End-to-end API surface against the real app + testcontainers."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_incidents_surface_end_to_end(api_client) -> None:
    # api_client: an httpx.AsyncClient bound to the booted app (testcontainers
    # PG+Redis+Kafka), following tests/integration/**/conftest.py. If no such
    # fixture exists, add one that runs the app via lifespan + ASGITransport.

    # 1. Create an incident manually.
    body = {
        "source": "generic",
        "external_id": "it-1",
        "service": "checkout",
        "severity": "SEV2",
        "title": "integration incident",
        "raw_payload": {"k": "v"},
    }
    created = await api_client.post("/incidents", json=body)
    assert created.status_code == 201, created.text
    incident_id = created.json()["incident_id"]

    # 2. It appears in the list.
    listed = await api_client.get("/incidents?service=checkout")
    assert listed.status_code == 200
    assert listed.json()["total"] >= 1

    # 3. Detail resolves.
    detail = await api_client.get(f"/incidents/{incident_id}")
    assert detail.status_code == 200
    assert detail.json()["id"] == incident_id
    assert detail.json()["diagnoses"] == []

    # 4. Metrics + readyz.
    metrics = await api_client.get("/metrics")
    assert metrics.status_code == 200
    assert "sentinel_" in metrics.text

    ready = await api_client.get("/readyz")
    assert ready.status_code == 200, ready.text
    assert ready.json()["checks"]["postgres"] == "ok"
    assert ready.json()["checks"]["redis"] == "ok"
```

> If `tests/integration/api/` lacks an `api_client`/booted-app fixture, add `tests/integration/api/conftest.py` that boots `build_app()` under its lifespan against the testcontainers services (reuse the container fixtures from `tests/integration/conftest.py` / sibling suites) and yields an `httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test")`. Disable the diagnosis + memory consumers (`diagnosis_consumer_enabled=False`, `memory_consumer_enabled=False`) so no Anthropic calls occur — mirror `tests/load/conftest.py`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make compose-up && pytest tests/integration/api/test_incidents_api.py -v -m integration`
Expected: FAIL — `POST /incidents` 404 (router not mounted) or `app.state.incident_repo` AttributeError.

- [ ] **Step 3: Stash `incident_repo` in the lifespan**

In `sentinel/api/app.py`, after the existing `incident_repo = PostgresIncidentRepository(session_factory)` (line 157) and near where `resolution_repo` is stashed (line 162), add:

```python
    app.state.incident_repo = incident_repo
```

- [ ] **Step 4: Mount the new routers**

In `sentinel/api/app.py`, add imports alongside the existing route imports (near the top, with `from sentinel.api.routes.health import router as health_router` etc.):

```python
from sentinel.api.routes.incidents import router as incidents_router
from sentinel.api.routes.metrics import router as metrics_router
```

Then in `build_app()` (after line 508), add:

```python
    app.include_router(incidents_router)
    app.include_router(metrics_router)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/integration/api/test_incidents_api.py -v -m integration`
Expected: PASS

- [ ] **Step 6: Run the full unit suite (regression)**

Run: `pytest tests/unit -q`
Expected: PASS (no regressions; existing health test updated in Task 7)

- [ ] **Step 7: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: no errors

- [ ] **Step 8: Commit**

```bash
git add sentinel/api/app.py tests/integration/api/
git commit -m "feat(api): mount incidents + metrics routers; stash incident_repo in app.state"
```

---

## Task 9: Real `make openapi` export

**Files:**
- Create: `scripts/export_openapi.py`
- Modify: `Makefile` (replace the `openapi` placeholder)
- Test: `tests/unit/api/test_openapi.py`

> **Note on invariant 6:** "no `dict[str, Any]` on the boundary" has documented exceptions for genuinely opaque payloads (`CreateIncidentRequest.raw_payload`, `EvalRunSummary.summary`). The test therefore asserts the schema *generates and is publishable and contains the new paths* — not the absence of every free-form object.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/api/test_openapi.py`:

```python
"""The app's OpenAPI schema must generate cleanly and cover the new routes."""

from __future__ import annotations

import json

from sentinel.api.app import build_app


def test_openapi_generates_and_covers_new_paths() -> None:
    app = build_app()
    schema = app.openapi()
    # Publishable: round-trips through JSON.
    json.dumps(schema)
    paths = schema["paths"]
    assert "/incidents" in paths
    assert "/incidents/{incident_id}" in paths
    assert "/metrics" in paths
    assert "get" in paths["/incidents"]
    assert "post" in paths["/incidents"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_openapi.py -v`
Expected: FAIL — `/incidents` not in paths (until Task 8 routers are mounted; if Task 8 is already merged, this passes for paths but the export script + Makefile are still missing — keep the test, it guards regressions).

> If Task 8 is complete this test may already pass; that's fine — it locks the contract. Proceed to wire the export script.

- [ ] **Step 3: Create the export script**

Create `scripts/export_openapi.py`:

```python
# scripts/export_openapi.py
"""Export the FastAPI OpenAPI schema to openapi.json (publishable artifact)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sentinel.api.app import build_app


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("openapi.json")
    schema = build_app().openapi()
    out.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out} ({len(schema['paths'])} paths)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Replace the Makefile placeholder**

In `Makefile`, replace the `openapi` target body with:

```make
openapi:
	$(PYTHON) scripts/export_openapi.py openapi.json
```

(Use whatever `$(PYTHON)`/venv variable the Makefile already defines for other targets; match the sibling targets' invocation style.)

- [ ] **Step 5: Run the export + test to verify they pass**

Run: `make openapi && pytest tests/unit/api/test_openapi.py -v`
Expected: `wrote openapi.json (...)` then PASS.

- [ ] **Step 6: Decide whether to track `openapi.json`**

Check `.gitignore`. If the repo intends to publish the spec, commit `openapi.json`; otherwise add it to `.gitignore`. Default: **commit it** (the README/spec calls for a publishable OpenAPI). Confirm with the repo owner if unsure.

- [ ] **Step 7: Lint + typecheck**

Run: `ruff format scripts/export_openapi.py tests/unit/api/test_openapi.py && ruff check scripts/export_openapi.py && mypy scripts/export_openapi.py`
Expected: no errors

- [ ] **Step 8: Commit**

```bash
git add scripts/export_openapi.py Makefile tests/unit/api/test_openapi.py openapi.json
git commit -m "feat(api): real make openapi export + publishable-schema test"
```

---

## Final verification (before PR)

- [ ] **Run the full quality gate**

Run: `make lint && make typecheck && pytest tests/unit -q`
Expected: all green.

- [ ] **Run integration (needs compose)**

Run: `make compose-up && pytest tests/integration/api tests/integration/persistence -v -m integration`
Expected: all green.

- [ ] **Mandatory code review**

Per project policy, run a subagent code review (`superpowers:requesting-code-review`) before opening the PR. Address findings, re-run the gate.

- [ ] **Open PR 1**

Title: `feat(api): read surface + foundation (Work Area I, PR 1)`. Body summarizes: routes added, the `/readyz` behavior change, diagnoses deferred to PR 2 (with the min_length=1 rationale), and the design-doc link.

---

## Self-review notes (planning)

- **Spec coverage:** `GET /incidents` ✅(T2/T3), `GET /incidents/{id}` ✅(T4, context only — diagnoses → PR 2), `POST /incidents` ✅(T5), `GET /metrics` ✅(T6), `/readyz` PG+Redis ✅(T7), publishable OpenAPI ✅(T9). Deferred to later PRs (documented in design): `/diagnose`, `/evals/*`, SSE, WS, explicit Kafka probe.
- **Refinement vs design doc:** the design said PR 1 detail would populate `diagnoses`; planning surfaced that `PersistedDiagnosis` permits empty evidence (incompatible with wire `Diagnosis.evidence: min_length=1`), so diagnoses population moves to PR 2 with a relaxed view model. Update the design doc's PR-1 row + the "populate diagnoses" note accordingly.
- **Type consistency:** `IngestResult(incident_id, event_kind)`, `IncidentListItem`/`IncidentDetailResponse`/`WebhookAcceptedResponse` reused verbatim from `schemas/api.py`; `fingerprint(service, normalized_title, severity)` + `normalize_title(title)` from `ingestion/fingerprint.py`; `NormalizedAlert` fields match `schemas/alert.py` (incl. tz-aware `received_at`); `CheckStatus = Literal["ok","dead"]` reused in `health.py`.
- **No placeholders:** every code step shows complete code. The one "fill-in" — the integration `api_client` fixture — is conditional on whether it already exists; the inline instruction specifies exactly how to build it from existing container fixtures.
