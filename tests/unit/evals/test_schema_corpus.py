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
            source="splunk",  # not in the allowed set
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
            severity="SEV99",  # not in SeverityType
            title="t",
            timestamp=datetime.now(UTC),
            raw_payload={},
        )
