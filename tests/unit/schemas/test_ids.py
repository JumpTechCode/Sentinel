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
