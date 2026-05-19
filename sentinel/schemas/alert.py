# sentinel/schemas/alert.py
"""NormalizedAlert — the canonical wire shape every adapter produces."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel.schemas.enums import SeverityType, SourceType


class NormalizedAlert(BaseModel):
    """Output of every adapter's `normalize(raw_payload) -> NormalizedAlert`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: SourceType
    external_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    received_at: datetime
    raw_payload: dict[str, Any]

    @field_validator("received_at")
    @classmethod
    def _require_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        return v
