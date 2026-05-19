# tests/unit/persistence/test_models_metadata.py
"""Static metadata checks — no DB needed."""

from sentinel.persistence.models import (
    Base,
    DeployModel,
    DiagnosisModel,
    EvalRunModel,
    IncidentModel,
    OutboxEventModel,
    ResolutionModel,
    RunbookModel,
)
from sqlalchemy import Table


def _table(model: type) -> Table:
    t: Table = model.__table__  # type: ignore[attr-defined]  # SA's DeclarativeBase __table__ is not visible in mypy stubs
    return t


def test_all_tables_registered_on_base() -> None:
    expected = {
        "incidents",
        "diagnoses",
        "resolutions",
        "deploys",
        "runbooks",
        "eval_runs",
        "outbox_events",
    }
    actual = set(Base.metadata.tables.keys())
    assert expected.issubset(actual), f"missing: {expected - actual}"


def test_incident_has_vector_column() -> None:
    cols = {c.name: c for c in _table(IncidentModel).columns}
    assert "embedding" in cols
    assert "VECTOR" in str(cols["embedding"].type).upper()


def test_runbook_has_vector_column() -> None:
    cols = {c.name: c for c in _table(RunbookModel).columns}
    assert "embedding" in cols
    assert "VECTOR" in str(cols["embedding"].type).upper()


def test_unique_source_external_id_on_incidents() -> None:
    table = _table(IncidentModel)
    unique = [c for c in table.constraints if c.__class__.__name__ == "UniqueConstraint"]
    cols = {tuple(sorted(c.name for c in uc.columns)) for uc in unique}  # type: ignore[attr-defined]  # SA's generic Constraint has no .columns in stubs
    assert ("external_id", "source") in cols


def test_diagnosis_fk_cascade() -> None:
    fk = next(iter(_table(DiagnosisModel).foreign_keys))
    assert fk.ondelete == "CASCADE"


def test_resolution_pk_is_incident_id() -> None:
    pk_cols = [c.name for c in _table(ResolutionModel).primary_key.columns]
    assert pk_cols == ["incident_id"]


def test_deploy_index_exists() -> None:
    idx_names = {ix.name for ix in _table(DeployModel).indexes}
    assert "idx_deploys_service_time" in idx_names


def test_eval_run_table_exists() -> None:
    assert EvalRunModel.__tablename__ == "eval_runs"


def test_outbox_events_table_in_metadata() -> None:
    assert "outbox_events" in Base.metadata.tables


def test_outbox_event_columns() -> None:
    table = Base.metadata.tables["outbox_events"]
    cols = {c.name: c for c in table.columns}
    assert {
        "id",
        "topic",
        "key",
        "payload",
        "created_at",
        "published_at",
        "attempts",
        "last_attempt_at",
        "last_error",
    }.issubset(cols)
    assert cols["published_at"].nullable is True
    assert cols["attempts"].nullable is False
    assert OutboxEventModel.__tablename__ == "outbox_events"
