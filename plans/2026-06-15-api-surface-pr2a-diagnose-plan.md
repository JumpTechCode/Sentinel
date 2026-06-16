# API Surface PR 2a — Diagnose + Diagnosis Presentation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /incidents/{id}/diagnose` (replay/fake-backed re-diagnosis) and populate diagnoses in `GET /incidents/{id}`, via a new relaxed `DiagnosisView` model — with zero live Anthropic spend.

**Architecture:** The wire `Diagnosis` model requires `evidence: min_length=1`, which a fully-hallucinated `PersistedDiagnosis` (verified-evidence = `[]`, `hallucinated_evidence=True`) cannot satisfy. We introduce `DiagnosisView` — a relaxed read model that allows empty evidence and carries persisted metadata — used by both the detail response and the diagnose response. `POST /diagnose` reuses the out-of-band pipeline's `diagnose()` agent + `save_with_outbox()` persistence; it runs through the existing cassette transport in replay mode (409 on a cassette miss) and is unit-tested with the established fake LLM. A small lifespan refactor builds the diagnosis agent deps + repo independent of the Kafka consumer so the HTTP route works regardless of `diagnosis_consumer_enabled`.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy async, pytest, ruff, mypy --strict.

**Spec references:** `sentinel-claude-code-prompt.md` §"API contracts" line 326. Design: `plans/2026-06-15-api-surface-design.md` (decision: replay-backed POST execution → ADR 0013; diagnoses deferred from PR 1 to here).

**Out of scope (PR 2b / later):** `/evals/*` routes, `eval_run_repo` wiring, SSE/WS, corpus expansion, live-LLM demo recording.

---

## File structure

| File | Action | Responsibility |
|---|---|---|
| `sentinel/schemas/api.py` | modify | add `DiagnosisView`; retype `IncidentDetailResponse.diagnoses` + `DiagnoseResponse` |
| `sentinel/diagnosis/persisted.py` | modify | add `to_view(record) -> DiagnosisView` converter |
| `sentinel/api/routes/incidents.py` | modify | populate diagnoses in detail handler; add `POST /incidents/{id}/diagnose` |
| `sentinel/api/app.py` | modify | hoist diagnosis deps/repo out of the consumer-gated block; stash `diagnosis_repo`, `diagnosis_prompt_version`, `diagnosis_model` |
| `docs/adr/0013-replay-backed-post-execution.md` | create | ADR for replay/fake-backed `/diagnose` |
| `tests/unit/diagnosis/test_persisted_view.py` | create | `to_view` unit tests (incl. empty-evidence) |
| `tests/unit/schemas/test_api.py` | modify | `DiagnosisView` + retyped responses |
| `tests/unit/api/test_incidents_route.py` | modify | detail-with-diagnosis tests; `/diagnose` handler tests (fakes) |
| `tests/integration/api/test_diagnose_api.py` | create | `/diagnose` end-to-end with a fake LLM + real DB; detail read-back |

---

## Task 1: `DiagnosisView` schema + `to_view` converter

**Files:**
- Modify: `sentinel/schemas/api.py`
- Modify: `sentinel/diagnosis/persisted.py`
- Test: `tests/unit/diagnosis/test_persisted_view.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/diagnosis/test_persisted_view.py`:

```python
"""PersistedDiagnosis -> DiagnosisView conversion, incl. the empty-evidence case."""

from __future__ import annotations

from decimal import Decimal

from sentinel.diagnosis.persisted import PersistedDiagnosis, to_view
from sentinel.schemas.api import DiagnosisView
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction


def _persisted(*, evidence: list[EvidenceRef], hallucinated: bool) -> PersistedDiagnosis:
    return PersistedDiagnosis(
        hypothesis="db connection pool exhausted",
        confidence=Decimal("0.82"),
        reasoning="pool saturated after deploy",
        evidence=evidence,
        suggested_actions=[
            SuggestedAction(description="roll back", risk="medium", rationale="recent deploy")
        ],
        likely_category="deploy",
        hallucinated_evidence=hallucinated,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=1234,
        token_usage={"input": 100, "output": 50},
    )


def test_to_view_maps_all_fields() -> None:
    ev = [EvidenceRef(kind="deploy", id="deploy:abc123", note="deploy 10m before")]
    view = to_view(_persisted(evidence=ev, hallucinated=False))
    assert isinstance(view, DiagnosisView)
    assert view.hypothesis == "db connection pool exhausted"
    assert view.confidence == 0.82  # Decimal -> float
    assert view.evidence == ev
    assert view.likely_category == "deploy"
    assert view.hallucinated_evidence is False
    assert view.model == "claude-sonnet-4-5"
    assert view.prompt_version == "v1"


def test_to_view_allows_empty_evidence() -> None:
    # A fully-hallucinated diagnosis stores evidence=[]; DiagnosisView must allow it
    # (the strict wire Diagnosis would reject min_length=1).
    view = to_view(_persisted(evidence=[], hallucinated=True))
    assert view.evidence == []
    assert view.hallucinated_evidence is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/diagnosis/test_persisted_view.py -v --no-cov`
Expected: FAIL — `ImportError: cannot import name 'DiagnosisView'` / `to_view`.

- [ ] **Step 3: Add `DiagnosisView` to `sentinel/schemas/api.py`**

Add `EvidenceRef`, `SuggestedAction` to the existing diagnosis import, and add the model after `IncidentListItem` (near the other incident models). The existing import line is `from sentinel.schemas.diagnosis import Diagnosis` — change it to:

```python
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction
```

Add the model:

```python
class DiagnosisView(BaseModel):
    """Read model for a PERSISTED diagnosis on API responses.

    Mirrors `Diagnosis` but relaxes `evidence` to allow an empty list: a
    fully-hallucinated diagnosis is stored with verified-evidence `[]` and
    `hallucinated_evidence=True`, which the strict wire `Diagnosis` (evidence
    min_length=1) cannot represent. Carries the persisted metadata the UI shows.
    """

    model_config = ConfigDict(frozen=True)
    hypothesis: str
    confidence: float
    reasoning: str
    evidence: list[EvidenceRef]
    suggested_actions: list[SuggestedAction]
    likely_category: CategoryType
    hallucinated_evidence: bool
    model: str
    prompt_version: str
```

(`CategoryType` is already imported in `api.py`.)

- [ ] **Step 4: Add the `to_view` converter to `sentinel/diagnosis/persisted.py`**

Append:

```python
def to_view(record: PersistedDiagnosis) -> "DiagnosisView":
    """Convert a persisted diagnosis into the relaxed API read model.

    Local import keeps `persisted.py` free of an API-schema import at module
    load (mirrors the local-import style in the repository layer).
    """
    from sentinel.schemas.api import DiagnosisView

    return DiagnosisView(
        hypothesis=record.hypothesis,
        confidence=float(record.confidence),
        reasoning=record.reasoning,
        evidence=record.evidence,
        suggested_actions=record.suggested_actions,
        likely_category=record.likely_category,
        hallucinated_evidence=record.hallucinated_evidence,
        model=record.model,
        prompt_version=record.prompt_version,
    )
```

Add a `TYPE_CHECKING` import so the return annotation type-checks without a runtime cycle. At the top of `persisted.py`, under the existing `from __future__ import annotations`:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.schemas.api import DiagnosisView
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/diagnosis/test_persisted_view.py -v --no-cov`
Expected: PASS (both tests)

- [ ] **Step 6: Lint + typecheck**

Run: `ruff format sentinel/schemas/api.py sentinel/diagnosis/persisted.py tests/unit/diagnosis/test_persisted_view.py && ruff check sentinel/schemas/api.py sentinel/diagnosis/persisted.py tests/unit/diagnosis/test_persisted_view.py && mypy sentinel/schemas/api.py sentinel/diagnosis/persisted.py`
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add sentinel/schemas/api.py sentinel/diagnosis/persisted.py tests/unit/diagnosis/test_persisted_view.py
git commit -m "$(cat <<'EOF'
feat(api): DiagnosisView read model + PersistedDiagnosis->view converter

Relaxes evidence min_length so a fully-hallucinated persisted diagnosis
(empty verified evidence) can be served on API responses.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Retype `IncidentDetailResponse.diagnoses` and `DiagnoseResponse`

**Files:**
- Modify: `sentinel/schemas/api.py`
- Test: `tests/unit/schemas/test_api.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/schemas/test_api.py`:

```python
def test_incident_detail_diagnoses_uses_view_and_allows_empty_evidence() -> None:
    from sentinel.schemas.api import DiagnosisView, IncidentDetailResponse

    view = DiagnosisView(
        hypothesis="h",
        confidence=0.4,
        reasoning="r",
        evidence=[],  # empty — only DiagnosisView permits this
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=True,
        model="claude-sonnet-4-5",
        prompt_version="v1",
    )
    detail = IncidentDetailResponse(
        id=uuid4(),
        source="sentry",
        external_id="e",
        service="s",
        severity="SEV1",
        status="diagnosed",
        title="t",
        fingerprint="f" * 64,
        opened_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        diagnoses=[view],
    )
    assert detail.diagnoses[0].hallucinated_evidence is True


def test_diagnose_response_carries_view_and_persisted_status() -> None:
    from sentinel.schemas.api import DiagnoseResponse, DiagnosisView

    view = DiagnosisView(
        hypothesis="h",
        confidence=0.82,
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
    )
    resp = DiagnoseResponse(incident_id=uuid4(), diagnosis=view, persisted="new")
    assert resp.persisted == "new"
    assert resp.diagnosis.confidence == 0.82
```

Add `DiagnosisView` to the file's top-level `from sentinel.schemas.api import (...)` block alongside the existing names.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/schemas/test_api.py -k "view" -v --no-cov`
Expected: FAIL — `DiagnoseResponse` has no `persisted` field / `diagnoses` rejects empty-evidence view (currently typed `list[Diagnosis]`).

- [ ] **Step 3: Retype the two response models**

In `sentinel/schemas/api.py`, change `IncidentDetailResponse.diagnoses`:

```python
    diagnoses: list[DiagnosisView] = Field(default_factory=list)
```

Replace the `DiagnoseResponse` model with:

```python
class DiagnoseResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    diagnosis: DiagnosisView
    persisted: Literal["new", "duplicate"]
```

(`Literal` is already imported in `api.py`. `DiagnosisView` is defined above `IncidentDetailResponse`/`DiagnoseResponse` — confirm ordering; move the `DiagnosisView` class above `IncidentDetailResponse` if needed so the annotation resolves.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/schemas/test_api.py -v --no-cov`
Expected: PASS (all schema tests)

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/schemas/api.py tests/unit/schemas/test_api.py && ruff check sentinel/schemas/api.py tests/unit/schemas/test_api.py && mypy sentinel/schemas/api.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/schemas/api.py tests/unit/schemas/test_api.py
git commit -m "$(cat <<'EOF'
feat(api): IncidentDetailResponse.diagnoses + DiagnoseResponse use DiagnosisView

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Lifespan — build diagnosis deps/repo independent of the Kafka consumer

**Files:**
- Modify: `sentinel/api/app.py`
- Test: covered by Task 7 integration (lifespan can't be unit-tested without infra; the route unit tests inject `app.state` directly)

> **Why:** today `diagnosis_repo`, the `AnthropicClient`, `PromptBundle`, audit logger, cassette transport, and `agent_diagnosis_deps` are all constructed inside `if settings.diagnosis_consumer_enabled:` (app.py ~294-360). The HTTP `/diagnose` route and the detail-diagnoses read must work even when the Kafka consumer is disabled (e.g. the creditless API test deployment). Hoist that construction so it always runs; the consumer then reuses it.

- [ ] **Step 1: Read the current lifespan block**

Read `sentinel/api/app.py` lines ~290-375. Identify: the `if settings.diagnosis_consumer_enabled:` block that builds `cassette_transport`, `llm_client`, `prompt_bundle`, `diagnosis_repo`, `audit_logger`, `diagnosis_deps` (a `DiagnosisConsumerDeps`), and `agent_diagnosis_deps`; and the unconditional `app.state.cassette_transport` / `app.state.diagnosis_deps` assignments (~353-360).

- [ ] **Step 2: Hoist the agent-deps construction out of the conditional**

Restructure so the following run **unconditionally** (before the `if settings.diagnosis_consumer_enabled:` consumer-start block):

```python
    # Diagnosis agent deps are needed by BOTH the out-of-band consumer and the
    # synchronous POST /incidents/{id}/diagnose route, so build them regardless
    # of whether the Kafka consumer is enabled.
    cassette_transport: CassetteTransport | None = None
    cassette_http_client: httpx.AsyncClient | None = None
    if settings.eval_mode and settings.eval_cassette_dir is not None:
        from sentinel.evals.cassette import CassetteTransport

        cassette_transport = CassetteTransport(
            mode=settings.eval_cassette_mode,
            cassette_dir=settings.eval_cassette_dir,
        )
        cassette_http_client = httpx.AsyncClient(transport=cassette_transport)

    llm_client = AnthropicClient(
        api_key=settings.anthropic_api_key,
        model=settings.anthropic_model,
        timeout_s=settings.diagnosis_llm_timeout_seconds,
        max_output_tokens=settings.diagnosis_max_output_tokens,
        http_client=cassette_http_client,
    )
    prompt_bundle = PromptBundle.load(settings.diagnosis_prompt_version)
    diagnosis_repo = PostgresDiagnosisRepository(session_factory)
    audit_logger = LLMAuditLogger(settings.llm_audit_log_path)
    agent_diagnosis_deps = DiagnosisDeps(
        llm=llm_client,
        prompt=prompt_bundle,
        max_input_tokens=settings.diagnosis_max_input_tokens,
        audit_logger=audit_logger,
    )

    app.state.cassette_transport = cassette_transport
    app.state.diagnosis_deps = agent_diagnosis_deps
    app.state.diagnosis_repo = diagnosis_repo
    app.state.diagnosis_prompt_version = settings.diagnosis_prompt_version
    app.state.diagnosis_model = settings.anthropic_model
```

Then in the `if settings.diagnosis_consumer_enabled:` block, build the `DiagnosisConsumerDeps` from the already-constructed pieces (reuse `llm_client`, `prompt_bundle`, `diagnosis_repo`, `audit_logger`, `incident_repo`) and start the consumer as before. Remove the now-duplicated construction from inside the conditional.

> Import note: `DiagnosisDeps` is `from sentinel.diagnosis.deps import DiagnosisDeps` — confirm it's imported at the top of app.py (it may currently be imported only where `agent_deps()` was called; add the import if missing). `DiagnosisConsumerDeps` is the consumer-deps type (verify its exact name/import in the current app.py — it is aliased there).

- [ ] **Step 3: Run the full unit suite + a smoke import**

Run: `python -c "from sentinel.api.app import build_app; build_app()"` (must not raise)
Run: `pytest tests/unit -q`
Expected: still green (this is a pure refactor; behavior unchanged for the consumer path).

- [ ] **Step 4: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add sentinel/api/app.py
git commit -m "$(cat <<'EOF'
refactor(api): build diagnosis agent deps/repo independent of the Kafka consumer

Hoists AnthropicClient/prompt/diagnosis_repo/cassette out of the
consumer-enabled block and stashes diagnosis_repo + prompt_version + model on
app.state so the synchronous /diagnose route works with consumers disabled.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Populate diagnoses in `GET /incidents/{id}`

**Files:**
- Modify: `sentinel/api/routes/incidents.py`
- Test: `tests/unit/api/test_incidents_route.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_incidents_route.py`:

```python
def test_detail_includes_latest_diagnosis() -> None:
    from decimal import Decimal

    from sentinel.diagnosis.persisted import PersistedDiagnosis
    from sentinel.schemas.diagnosis import EvidenceRef

    incident_id = uuid4()
    pd = PersistedDiagnosis(
        hypothesis="pool exhausted",
        confidence=Decimal("0.82"),
        reasoning="saturated after deploy",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="10m before")],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=10,
        token_usage={},
    )
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    diag_repo = type("D", (), {})()
    diag_repo.get_by_incident_id = AsyncMock(return_value=pd)

    app = FastAPI()
    app.state.incident_repo = repo
    app.state.diagnosis_repo = diag_repo
    app.include_router(incidents_router)
    resp = TestClient(app).get(f"/incidents/{incident_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["diagnoses"]) == 1
    assert data["diagnoses"][0]["hypothesis"] == "pool exhausted"
    assert data["diagnoses"][0]["hallucinated_evidence"] is False


def test_detail_empty_diagnoses_when_none() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    diag_repo = type("D", (), {})()
    diag_repo.get_by_incident_id = AsyncMock(return_value=None)

    app = FastAPI()
    app.state.incident_repo = repo
    app.state.diagnosis_repo = diag_repo
    app.include_router(incidents_router)
    resp = TestClient(app).get(f"/incidents/{incident_id}")
    assert resp.status_code == 200
    assert resp.json()["diagnoses"] == []
```

> The existing `_app(repo)` helper sets only `incident_repo`. These two tests build the app inline so they can also set `diagnosis_repo`. Update the shared `_app` helper to also accept/stash a `diagnosis_repo` if you prefer — but the existing detail tests (Task 4 of PR 1) call `_app(repo)` without a diagnosis repo, so the detail handler must tolerate a **missing** `diagnosis_repo` on app.state (treat as "no diagnoses"). Keep `_app` backward compatible.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/api/test_incidents_route.py -k "detail_includes_latest or detail_empty_diagnoses" -v --no-cov`
Expected: FAIL — diagnoses still `[]` (handler doesn't read the diagnosis repo yet).

- [ ] **Step 3: Update the detail handler**

In `sentinel/api/routes/incidents.py`, add the import:

```python
from sentinel.diagnosis.persisted import to_view
```

In `get_incident`, after folding in the enrichment context, add:

```python
    # Latest diagnosis (0 or 1) rendered via the relaxed view model. The repo is
    # optional on app.state (e.g. minimal test apps) — absent means "no diagnoses".
    diag_repo = getattr(request.app.state, "diagnosis_repo", None)
    if diag_repo is not None:
        persisted = await diag_repo.get_by_incident_id(incident_id)
        if persisted is not None:
            detail = detail.model_copy(update={"diagnoses": [to_view(persisted)]})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/api/test_incidents_route.py -v --no-cov`
Expected: PASS (all incident-route tests, including PR 1's that don't set `diagnosis_repo`).

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && ruff check sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && mypy sentinel/api/routes/incidents.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py
git commit -m "$(cat <<'EOF'
feat(api): populate GET /incidents/{id} diagnoses (latest, via DiagnosisView)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `POST /incidents/{id}/diagnose`

**Files:**
- Modify: `sentinel/api/routes/incidents.py`
- Test: `tests/unit/api/test_incidents_route.py`

> **Behavior:** re-runs `diagnose()` over the incident + its stored enrichment context, through the cassette transport (replay) when present, and persists via `save_with_outbox` (idempotent on `(incident_id, prompt_version, model)` → `persisted: "new" | "duplicate"`). Failure modes: 404 unknown incident; 409 `incident_already_resolved`; 409 `context_not_assembled` (no enrichment context); 409 `no_recording_available` (cassette miss in replay mode); 422 `diagnosis_schema_invalid`; 502 `diagnosis_llm_error`; 503 `diagnosis_unavailable` (deps not wired). Does NOT call `mark_diagnosing` — a manual recompute must not regress the incident's lifecycle status.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/api/test_incidents_route.py`. First, a small helper to build an app with all the diagnose deps as fakes:

```python
def _diagnose_app(*, incident, context_stored, persisted_result, diagnose_fn):
    from types import SimpleNamespace

    app = FastAPI()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=incident)
    repo.get_enrichment_context = AsyncMock(return_value=context_stored)
    app.state.incident_repo = repo
    diag_repo = type("D", (), {})()
    # save_with_outbox returns (diagnosis_id, status)
    diag_repo.save_with_outbox = AsyncMock(return_value=(uuid4(), persisted_result))
    app.state.diagnosis_repo = diag_repo
    app.state.diagnosis_deps = SimpleNamespace()  # opaque; diagnose_fn is monkeypatched
    app.state.cassette_transport = None
    app.state.diagnosis_prompt_version = "v1"
    app.state.diagnosis_model = "claude-sonnet-4-5"
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(incidents_router)
    return app, repo, diag_repo
```

```python
def test_diagnose_persists_and_returns_view(monkeypatch) -> None:
    from decimal import Decimal

    from sentinel.diagnosis.persisted import PersistedDiagnosis
    from sentinel.schemas.diagnosis import EvidenceRef

    incident_id = uuid4()
    incident = _detail(incident_id)  # status "open"
    ctx = SimpleNamespace(context=make_context(incident_id=incident_id))
    record = PersistedDiagnosis(
        hypothesis="pool exhausted",
        confidence=Decimal("0.82"),
        reasoning="r",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="n")],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=5,
        token_usage={},
    )

    async def fake_diagnose(_inc, _ctx, _deps):
        return record

    # The route calls sentinel.api.routes.incidents.diagnose(...)
    monkeypatch.setattr("sentinel.api.routes.incidents.diagnose", fake_diagnose)
    app, _repo, diag_repo = _diagnose_app(
        incident=incident, context_stored=ctx, persisted_result="new", diagnose_fn=fake_diagnose
    )
    resp = TestClient(app).post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["persisted"] == "new"
    assert data["diagnosis"]["hypothesis"] == "pool exhausted"
    diag_repo.save_with_outbox.assert_awaited_once()


def test_diagnose_404_when_incident_missing(monkeypatch) -> None:
    app = FastAPI()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=None)
    repo.get_enrichment_context = AsyncMock(return_value=None)
    app.state.incident_repo = repo
    app.state.diagnosis_repo = type("D", (), {})()
    app.state.diagnosis_deps = SimpleNamespace()
    app.state.cassette_transport = None
    app.state.diagnosis_prompt_version = "v1"
    app.state.diagnosis_model = "m"
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(incidents_router)
    resp = TestClient(app).post(f"/incidents/{uuid4()}/diagnose")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"


def test_diagnose_409_when_no_context(monkeypatch) -> None:
    incident_id = uuid4()
    app, _repo, _diag = _diagnose_app(
        incident=_detail(incident_id), context_stored=None, persisted_result="new",
        diagnose_fn=None,
    )
    resp = TestClient(app).post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 409
    assert resp.json()["detail"] == "context_not_assembled"


def test_diagnose_409_on_cassette_miss(monkeypatch) -> None:
    from sentinel.evals.cassette import CassetteMiss

    incident_id = uuid4()
    ctx = SimpleNamespace(context=make_context(incident_id=incident_id))

    async def boom(_i, _c, _d):
        raise CassetteMiss("no cassette")

    monkeypatch.setattr("sentinel.api.routes.incidents.diagnose", boom)
    app, _repo, _diag = _diagnose_app(
        incident=_detail(incident_id), context_stored=ctx, persisted_result="new", diagnose_fn=boom
    )
    resp = TestClient(app).post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 409
    assert resp.json()["detail"] == "no_recording_available"


def test_diagnose_503_when_deps_unavailable() -> None:
    incident_id = uuid4()
    app = FastAPI()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    app.state.incident_repo = repo
    app.state.diagnosis_repo = None
    app.state.diagnosis_deps = None  # consumers disabled AND deps not built
    app.state.cassette_transport = None
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(incidents_router)
    resp = TestClient(app).post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 503
    assert resp.json()["detail"] == "diagnosis_unavailable"
```

(`SimpleNamespace` and `make_context` are already imported at the top of the test file from PR 1; if not, add `from types import SimpleNamespace` and `from tests.unit.diagnosis.fakes import make_context`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/api/test_incidents_route.py -k diagnose -v --no-cov`
Expected: FAIL — route not defined (404/405).

- [ ] **Step 3: Add the handler**

In `sentinel/api/routes/incidents.py`, add imports (mirror the exception imports `sentinel/diagnosis/consumer.py` uses — grep it for `DiagnosisInvalid` and `LLMError` and copy those exact import lines):

```python
from datetime import UTC, datetime  # already imported from PR 1 — keep
from uuid import UUID, uuid4

from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.persisted import to_view  # added in Task 4
from sentinel.evals.cassette import CassetteContext, CassetteMiss
from sentinel.persistence.repositories import IncidentRepository, OutboxEvent
from sentinel.schemas.api import DiagnoseResponse
# plus, copied verbatim from consumer.py's imports:
#   the DiagnosisInvalid and LLMError exception imports
```

Append the handler:

```python
@router.post(
    "/incidents/{incident_id}/diagnose",
    response_model=DiagnoseResponse,
    responses={
        404: {"description": "Incident not found"},
        409: {"description": "Already resolved / no context / no recording"},
        422: {"description": "Diagnosis failed schema validation"},
        502: {"description": "LLM error"},
        503: {"description": "Diagnosis deps unavailable"},
    },
)
async def diagnose_incident(incident_id: UUID, request: Request) -> DiagnoseResponse:
    state = request.app.state
    deps = getattr(state, "diagnosis_deps", None)
    diag_repo = getattr(state, "diagnosis_repo", None)
    if deps is None or diag_repo is None:
        raise HTTPException(status_code=503, detail="diagnosis_unavailable")

    repo: IncidentRepository = state.incident_repo
    incident = await repo.get(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident_not_found")
    if incident.status == "resolved":
        raise HTTPException(status_code=409, detail="incident_already_resolved")
    stored = await repo.get_enrichment_context(incident_id)
    if stored is None:
        raise HTTPException(status_code=409, detail="context_not_assembled")

    # Cassette replay (eval/demo mode): key on this incident's id so an arbitrary
    # incident with no recording fails closed with 409 rather than a live call.
    cassette = getattr(state, "cassette_transport", None)
    if cassette is not None:
        cassette.set_context(
            CassetteContext(
                prompt_version=state.diagnosis_prompt_version,
                model_id=state.diagnosis_model,
                case_id=str(incident_id),
                shot_index=0,
            )
        )

    try:
        record = await diagnose(incident, stored.context, deps)
    except CassetteMiss as e:
        raise HTTPException(status_code=409, detail="no_recording_available") from e
    except DiagnosisInvalid as e:
        raise HTTPException(status_code=422, detail="diagnosis_schema_invalid") from e
    except LLMError as e:
        raise HTTPException(status_code=502, detail="diagnosis_llm_error") from e

    outbox_id = uuid4()
    outbox_event = OutboxEvent(
        id=outbox_id,
        topic=state.outbox_topic,
        key=str(incident_id),
        payload={
            "event_id": str(outbox_id),
            "event": "incident.diagnosed",
            "incident_id": str(incident_id),
            "ts": datetime.now(UTC).isoformat(),
        },
        attempts=0,
        created_at=datetime.now(UTC),
    )
    # upstream_event_id is synthesized for the manual trigger (no Kafka envelope).
    _diagnosis_id, persisted = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=record,
        upstream_event_id=uuid4(),
        outbox_event=outbox_event,
    )
    return DiagnoseResponse(incident_id=incident_id, diagnosis=to_view(record), persisted=persisted)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/api/test_incidents_route.py -k diagnose -v --no-cov`
Expected: PASS (all 5 diagnose tests). Then run the whole route file: `pytest tests/unit/api/test_incidents_route.py -v --no-cov`.

- [ ] **Step 5: Lint + typecheck**

Run: `ruff format sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && ruff check sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py && mypy sentinel/api/routes/incidents.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add sentinel/api/routes/incidents.py tests/unit/api/test_incidents_route.py
git commit -m "$(cat <<'EOF'
feat(api): POST /incidents/{id}/diagnose (replay-backed, persists via outbox)

Re-runs the diagnosis agent over stored context through the cassette transport
(409 no_recording_available on miss), persists via save_with_outbox. Suggest-
only invariant preserved (no remediation execution).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: ADR 0013 — replay-backed POST execution

**Files:**
- Create: `docs/adr/0013-replay-backed-post-execution.md`

- [ ] **Step 1: Write the ADR**

Create `docs/adr/0013-replay-backed-post-execution.md` (match the format of an existing ADR, e.g. `0009-stability-at-llm-call-level.md` — Status / Context / Decision / Consequences):

```markdown
# 0013 — Replay-backed POST execution for /diagnose

**Status:** Accepted

## Context
`POST /incidents/{id}/diagnose` re-runs the LLM diagnosis on demand. The project
holds a hard zero-live-Anthropic-spend constraint for local/dev/CI/demo, and the
single-shot diagnosis pipeline already records/replays LLM HTTP via the cassette
transport (`sentinel/evals/cassette.py`), keyed by
`(prompt_version, model_id, case_id, shot_index)`.

## Decision
`/diagnose` reuses the out-of-band `diagnose()` agent and `save_with_outbox()`
persistence. When the app runs in eval/cassette mode the call goes through the
cassette transport in replay mode; the route stamps a `CassetteContext` keyed on
the incident id (`shot_index=0`). A cassette miss fails closed with **409
`no_recording_available`** rather than making a live call. Tests inject a fake
LLM client; CI never spends credits. A live call happens only in a deployment
that is explicitly not in cassette mode and has a real API key.

Diagnosis agent deps + repo are built independent of `diagnosis_consumer_enabled`
so the HTTP route works with the Kafka consumer disabled.

## Consequences
- Demo `/diagnose` on a brand-new (non-corpus) incident needs a recorded
  cassette for that incident, or it returns 409. The compose demo uses seeded
  incidents with committed cassettes.
- Persistence is idempotent on `(incident_id, prompt_version, model)`; re-running
  with unchanged prompt+model returns `persisted: "duplicate"`.
- The suggest-only invariant is untouched: the route never executes actions.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0013-replay-backed-post-execution.md
git commit -m "$(cat <<'EOF'
docs(adr): 0013 replay-backed POST execution for /diagnose

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Integration test — `/diagnose` end-to-end with a fake LLM

**Files:**
- Create: `tests/integration/api/test_diagnose_api.py`

> **Why a fake LLM, not a cassette:** an arbitrary integration-test incident has no recorded cassette (that's the 409 path, unit-tested in Task 5). To exercise the real persist + read-back path end-to-end against a live DB without spend, inject the established fake Anthropic client. Reference `tests/integration/diagnosis/test_end_to_end.py` for the `_FakeAnthropicClient` and how it's wired into the diagnosis deps.

- [ ] **Step 1: Write the failing test**

Create `tests/integration/api/test_diagnose_api.py`. It boots the app (reuse `tests/integration/api/conftest.py`'s container fixtures), then overrides `app.state.diagnosis_deps` with deps whose `llm` is the fake client, seeds an incident + enrichment context directly via the repos, calls `POST /diagnose`, and asserts the diagnosis persists and surfaces in `GET /incidents/{id}`:

```python
"""POST /incidents/{id}/diagnose end-to-end with a fake LLM (creditless)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_diagnose_persists_and_appears_in_detail(client, app, seed_incident_with_context):
    # seed_incident_with_context: a fixture (add to this file or conftest) that
    # inserts an incident + writes its enrichment context via the real repos on
    # app.state, returning the incident_id. Build it from the existing repo
    # fixtures used by tests/integration/persistence and the enrichment-context
    # repo test.
    incident_id = await seed_incident_with_context()

    # Override diagnosis deps with a fake LLM (mirror tests/integration/diagnosis/
    # test_end_to_end.py::_FakeAnthropicClient). Build DiagnosisDeps with the fake
    # llm + the real prompt/audit so diagnose() returns a valid PersistedDiagnosis.
    app.state.diagnosis_deps = _fake_diagnosis_deps()  # defined in this test module

    resp = await client.post(f"/incidents/{incident_id}/diagnose")
    assert resp.status_code == 200, resp.text
    assert resp.json()["persisted"] in ("new", "duplicate")

    detail = await client.get(f"/incidents/{incident_id}")
    assert detail.status_code == 200
    assert len(detail.json()["diagnoses"]) == 1
```

> Implementer: define `_fake_diagnosis_deps()` and `seed_incident_with_context` in this module, reusing `_FakeAnthropicClient` from the diagnosis e2e test and `PromptBundle.load(...)`. The fake must return a tool-call payload that parses into a valid `PersistedDiagnosis` (copy the fake's canned response from the existing e2e test). If wiring the fake proves heavy, an acceptable alternative is to record a cassette for the seeded incident id and run in replay mode — but the fake-LLM path is preferred (no committed cassette to maintain).

- [ ] **Step 2: Run it**

Run: `pytest tests/integration/api/test_diagnose_api.py -v -m integration --no-cov`
Expected: FAIL first (until the fake wiring is in place), then PASS.

- [ ] **Step 3: Lint + typecheck + commit**

Run: `ruff format tests/integration/api/test_diagnose_api.py && ruff check tests/integration/api/test_diagnose_api.py && mypy tests/integration/api/test_diagnose_api.py`

```bash
git add tests/integration/api/test_diagnose_api.py
git commit -m "$(cat <<'EOF'
test(api): /diagnose end-to-end with a fake LLM + detail read-back

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final verification (before PR)

- [ ] `make lint && make typecheck && pytest tests/unit -q` → all green.
- [ ] `make compose-up && pytest tests/integration/api -v -m integration --no-cov` → green.
- [ ] Mandatory subagent code review (`superpowers:requesting-code-review`) before the PR; address findings, re-run the gate.
- [ ] Open PR: `feat(api): diagnose + diagnosis presentation (Work Area I, PR 2a)`. Body notes the lifespan refactor, the `DiagnosisView` rationale, ADR 0013, and that `/evals/*` is PR 2b.

## Self-review notes (planning)

- **Spec coverage:** `POST /incidents/{id}/diagnose` ✅ (T5); diagnoses populated in detail ✅ (T4); `DiagnosisView` resolves the PR-1-deferred empty-evidence blocker ✅ (T1/T2); ADR 0013 ✅ (T6). `/evals/*` intentionally PR 2b.
- **Type consistency:** `to_view` returns `DiagnosisView`; `DiagnoseResponse.diagnosis: DiagnosisView` + `persisted: Literal["new","duplicate"]` matches `save_with_outbox`'s return; `OutboxEvent(id,topic,key,payload,attempts,created_at)` matches the dataclass; `save_with_outbox(incident_id=, record=, upstream_event_id=, outbox_event=)` matches the repo signature.
- **Two implementer-resolved specifics (concrete, not placeholders):** (a) the `DiagnosisInvalid`/`LLMError` import lines — copy verbatim from `sentinel/diagnosis/consumer.py`'s imports; (b) the integration fake-LLM wiring — reuse `_FakeAnthropicClient` from `tests/integration/diagnosis/test_end_to_end.py`. Both name an exact existing source to copy.
- **YAGNI:** no `mark_diagnosing` on manual re-diagnose (avoids lifecycle regression); latency_ms/token_usage stay out of `DiagnosisView` (internal metrics, exposed via `/metrics`).
</content>
