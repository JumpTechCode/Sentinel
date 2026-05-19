from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from sentinel.ingestion.fingerprint import fingerprint, normalize_title


# Golden table — defines the stability contract.
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("OOM in pod web-7d4f8b69-x9k2j at 2026-05-18T14:23:01Z", "oom in pod web- at"),
        ("Connection refused (request_id=abcdef1234567890)", "connection refused (request_id="),
        ("Error 500 from upstream service-1234", "error from upstream service-"),
        ("Job a1b2c3d4e5f6 failed", "job failed"),
        ("Issue with [ctx:foo] on shard 5", "issue with on shard 5"),
        ("https://example.com/path/abc?x=1 returned 502", "returned"),
    ],
)
def test_normalize_title_golden(raw: str, expected: str) -> None:
    assert normalize_title(raw) == expected


def test_fingerprint_is_deterministic() -> None:
    f1 = fingerprint("svc", "title", "SEV2")
    f2 = fingerprint("svc", "title", "SEV2")
    assert f1 == f2
    assert len(f1) == 64


def test_fingerprint_differs_by_service() -> None:
    a = fingerprint("svc-a", "t", "SEV2")
    b = fingerprint("svc-b", "t", "SEV2")
    assert a != b


def test_fingerprint_differs_by_severity() -> None:
    assert fingerprint("svc", "t", "SEV1") != fingerprint("svc", "t", "SEV2")


@given(
    uuid_part=st.uuids().map(str),
    ts_part=st.datetimes().map(lambda d: d.isoformat() + "Z"),
)
def test_fingerprint_stable_across_uuid_and_timestamp_permutations(
    uuid_part: str, ts_part: str
) -> None:
    t1 = f"OOM in pod {uuid_part} at {ts_part}"
    t2 = "OOM in pod 00000000-0000-0000-0000-000000000000 at 2026-01-01T00:00:00Z"
    assert fingerprint("svc", normalize_title(t1), "SEV2") == fingerprint(
        "svc", normalize_title(t2), "SEV2"
    )
