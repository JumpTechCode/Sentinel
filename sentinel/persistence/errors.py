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
