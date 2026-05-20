"""Unit tests for sentinel.evals.fetcher_override (corpus-replay fetchers)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

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
from sentinel.schemas.context import FetcherResult


class _CorpusFetcherLike(Protocol):
    name: str
    timeout_s: float

    def __init__(self, registry: Any) -> None: ...

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[Any]: ...


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

    cases: list[tuple[type[_CorpusFetcherLike], str, str]] = [
        (CorpusDeploysFetcher, "deploys", "deploy:abc"),
        (CorpusRelatedAlertsFetcher, "related_alerts", "related:1"),
        (CorpusSimilarIncidentsFetcher, "similar_incidents", "similar:1"),
        (CorpusRunbooksFetcher, "runbooks", "runbook:1"),
        (CorpusRecentLogsFetcher, "recent_logs", "log:1"),
        (CorpusActiveAlertsFetcher, "active_alerts", "active:1"),
    ]
    pairs: list[tuple[str, int]] = []
    for fetcher_cls, expected_name, expected_first_id in cases:
        f: _CorpusFetcherLike = fetcher_cls(reg)
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
