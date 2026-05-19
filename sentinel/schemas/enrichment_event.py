# sentinel/schemas/enrichment_event.py
"""Kafka envelope the enrichment consumer reads from sentinel.incidents.

Note: the `event` field is `str` (not `Literal`) so unknown event types parse
cleanly. The consumer filters at the application layer rather than at parse
time — this lets us add new event types upstream without breaking the parser.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class IncidentEvent(BaseModel):
    """One Kafka message on `sentinel.incidents` (post Phase 3 patch)."""

    model_config = ConfigDict(frozen=True, extra="allow")

    event_id: UUID
    event: str  # filtered to {"incident.opened", "incident.recurred"} in the consumer
    incident_id: UUID
    fingerprint: str
    source: str
    ts: datetime
