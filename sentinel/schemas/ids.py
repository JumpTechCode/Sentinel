# sentinel/schemas/ids.py
"""Stable, parseable IDs for context items referenced from diagnoses.

The diagnosis prompt sees context items prefixed with `[kind:id]`. The validation
gate parses every EvidenceRef.id with `parse_context_id` and confirms the ID
appears in the supplied context — invented citations fail the gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

ContextKind = Literal["deploy", "similar", "runbook", "log", "related"]
_KNOWN_KINDS: frozenset[str] = frozenset({"deploy", "similar", "runbook", "log", "related"})


@dataclass(frozen=True, slots=True)
class ContextID:
    kind: ContextKind
    id: str


def deploy_id(sha: str) -> str:
    return f"deploy:{sha}"


def similar_id(uuid: UUID) -> str:
    return f"similar:{uuid}"


def runbook_id(uuid: UUID) -> str:
    return f"runbook:{uuid}"


def log_id(idx: int) -> str:
    return f"log:{idx}"


def related_id(uuid: UUID) -> str:
    return f"related:{uuid}"


def parse_context_id(value: str) -> ContextID:
    if ":" not in value:
        raise ValueError(f"malformed context id: {value!r}")
    kind, _, rest = value.partition(":")
    if kind not in _KNOWN_KINDS:
        raise ValueError(f"unknown kind {kind!r} in context id {value!r}")
    if not rest:
        raise ValueError(f"empty id portion in context id {value!r}")
    return ContextID(kind=kind, id=rest)  # type: ignore[arg-type]  # narrowed by _KNOWN_KINDS membership check above
