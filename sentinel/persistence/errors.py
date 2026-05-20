# sentinel/persistence/errors.py
"""Domain exceptions raised by persistence repositories.

Mirrors the sentinel/diagnosis/errors.py precedent: small, explicit exception
types that map cleanly to HTTP statuses at the route layer.
"""

from __future__ import annotations

from uuid import UUID


class IncidentNotFound(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident not found: {incident_id}")
        self.incident_id = incident_id


class IncidentAlreadyResolved(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident already resolved: {incident_id}")
        self.incident_id = incident_id


class EvalRunNotFoundOrAlreadyFinalized(Exception):
    """Raised by EvalRunRepository.finalize_run when the UPDATE matched zero rows.

    Means either the run_id is wrong (programming bug) or the run was already
    finalized (status != 'running' — concurrent finalize, also a bug in a
    single-process runner). Surface to caller; do not swallow silently.
    """

    def __init__(self, run_id: UUID) -> None:
        super().__init__(f"eval run not found or already finalized: {run_id}")
        self.run_id = run_id
