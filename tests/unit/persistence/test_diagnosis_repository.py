# tests/unit/persistence/test_diagnosis_repository.py
"""Unit tests for DiagnosisRepository surface (Protocol shape)."""

from __future__ import annotations


def test_diagnosis_repository_exposes_get_by_incident_id() -> None:
    from sentinel.persistence.repositories import DiagnosisRepository

    assert "get_by_incident_id" in dir(
        DiagnosisRepository
    ), "DiagnosisRepository.get_by_incident_id is required by the eval runner"
