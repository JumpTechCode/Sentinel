# sentinel/persistence/__init__.py
"""Persistence layer — async SQLAlchemy 2.x models, repositories, sessions.

This is the only module allowed to perform raw SQL operations.
"""

from sentinel.persistence.models import (
    Base,
    DeployModel,
    DiagnosisModel,
    EvalRunModel,
    IncidentModel,
    ResolutionModel,
    RunbookModel,
)
from sentinel.persistence.repositories import (
    DeployRepository,
    DeployRow,
    DiagnosisRepository,
    EvalRunRepository,
    IncidentRepository,
    PostgresDeployRepository,
    PostgresIncidentRepository,
    ResolutionRepository,
    RunbookRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory

__all__ = [
    "Base",
    "DeployModel",
    "DeployRepository",
    "DeployRow",
    "DiagnosisModel",
    "DiagnosisRepository",
    "EvalRunModel",
    "EvalRunRepository",
    "IncidentModel",
    "IncidentRepository",
    "PostgresDeployRepository",
    "PostgresIncidentRepository",
    "ResolutionModel",
    "ResolutionRepository",
    "RunbookModel",
    "RunbookRepository",
    "make_async_engine",
    "make_session_factory",
]
