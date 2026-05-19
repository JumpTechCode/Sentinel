# tests/unit/persistence/test_ingest_method.py
"""Unit tests for IngestResult dataclass — shape and immutability only.

Real DB exercise (dedup, outbox insert, etc.) lives in the integration
test Task IT2.  These tests are import-only / value-construction; they
never touch the database.
"""

import dataclasses
from uuid import uuid4

import pytest
from sentinel.persistence.repositories import IngestResult


def test_ingest_result_is_frozen() -> None:
    r = IngestResult(incident_id=uuid4(), event_kind="opened")
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.event_kind = "recurred"  # type: ignore[misc]


def test_ingest_result_event_kind_values() -> None:
    # mypy enforces the Literal at type-check time; this just verifies runtime
    # construction accepts both documented values.
    IngestResult(incident_id=uuid4(), event_kind="opened")
    IngestResult(incident_id=uuid4(), event_kind="recurred")
