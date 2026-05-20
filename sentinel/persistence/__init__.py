# sentinel/persistence/__init__.py
"""Persistence layer — async SQLAlchemy 2.x models, repositories, sessions.

This is the only module allowed to perform raw SQL operations.
"""

from sentinel.persistence.errors import (
    EvalRunNotFoundOrAlreadyFinalized,
)
from sentinel.persistence.models import (
    Base,
    DeployModel,
    DiagnosisModel,
    EvalCaseResultModel,
    EvalRunModel,
    IncidentModel,
    ResolutionModel,
    RunbookModel,
)
from sentinel.persistence.repositories import (
    DeployRepository,
    DeployRow,
    DiagnosisRepository,
    EvalCaseResultRecord,
    EvalRunRecord,
    EvalRunRepository,
    IncidentRepository,
    PostgresDeployRepository,
    PostgresEvalRunRepository,
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
    "EvalCaseResultModel",
    "EvalCaseResultRecord",
    "EvalRunModel",
    "EvalRunNotFoundOrAlreadyFinalized",
    "EvalRunRecord",
    "EvalRunRepository",
    "IncidentModel",
    "IncidentRepository",
    "PostgresDeployRepository",
    "PostgresEvalRunRepository",
    "PostgresIncidentRepository",
    "ResolutionModel",
    "ResolutionRepository",
    "RunbookModel",
    "RunbookRepository",
    "make_async_engine",
    "make_session_factory",
]
