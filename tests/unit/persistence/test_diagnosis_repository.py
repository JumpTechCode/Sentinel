# tests/unit/persistence/test_diagnosis_repository.py
"""Unit tests for DiagnosisRepository surface (Protocol + concrete signature shape).

Behaviour is exercised by the e2e test in
tests/integration/persistence/test_diagnosis_repository_e2e.py; these unit
tests guard the contract the eval runner depends on without spinning Postgres.
"""

from __future__ import annotations

import inspect
import typing
from uuid import UUID

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.persistence.repositories import (
    DiagnosisRepository,
    PostgresDiagnosisRepository,
)


def test_protocol_exposes_get_by_incident_id() -> None:
    assert "get_by_incident_id" in dir(
        DiagnosisRepository
    ), "DiagnosisRepository.get_by_incident_id is required by the eval runner"


def test_get_by_incident_id_signature_matches_runner_contract() -> None:
    """Single positional UUID arg, returns PersistedDiagnosis | None, async.

    The eval runner polls this method with the incident_id returned from the
    webhook handler; any signature drift breaks the polling loop.
    `repositories.py` uses `from __future__ import annotations`, so annotations
    are strings at runtime — resolve via get_type_hints to compare against the
    real types.
    """
    method = PostgresDiagnosisRepository.get_by_incident_id
    assert inspect.iscoroutinefunction(method), "must be async"

    hints = typing.get_type_hints(method)
    assert hints["incident_id"] is UUID
    assert hints["return"] == PersistedDiagnosis | None

    params = list(inspect.signature(method).parameters)
    # self + incident_id, in that order
    assert params == ["self", "incident_id"], params
