# sentinel/diagnosis/truncation.py
"""Context truncation under an input-token budget. Pure function — no I/O.

Token count: len(text) // 4 with 10% safety margin (the Anthropic SDK has no
local tokenizer). Observe truncation stats and tighten margin if reality drifts.

Drop priority (highest first):
  recent_logs       — oldest first
  related_alerts    — oldest first
  active_alerts     — oldest first
  runbooks          — lowest cosine first
  similar_incidents — lowest cosine first
  recent_deploys    — oldest first, but always preserve the newest

A section with ≥1 item retains ≥1 after truncation (so the model sees the
section exists).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sentinel.schemas.context import IncidentContext

_SAFETY_MARGIN = 0.10
_CHARS_PER_TOKEN = 4
# Default json.dumps separator between list items (separators=(", ", ": ")).
# Popping the last element of a list removes exactly this separator plus the
# element's own serialization — the basis for the O(n) running-total below.
_ITEM_SEP = ", "

_DROP_ORDER: tuple[str, ...] = (
    "recent_logs",
    "related_alerts",
    "active_alerts",
    "runbooks",
    "similar_incidents",
    "recent_deploys",
)


@dataclass(frozen=True, slots=True)
class TruncationStats:
    dropped: dict[str, int] = field(default_factory=dict)

    def total_dropped(self) -> int:
        return sum(self.dropped.values())

    def to_dict(self) -> dict[str, int]:
        return dict(self.dropped)


def _estimate_tokens(blob_json: str) -> int:
    return _estimate_tokens_for_chars(len(blob_json))


def _estimate_tokens_for_chars(n_chars: int) -> int:
    return int(n_chars / _CHARS_PER_TOKEN * (1 + _SAFETY_MARGIN))


def truncate_for_budget(
    ctx: IncidentContext, *, max_input_tokens: int
) -> tuple[IncidentContext, TruncationStats]:
    """Return a (possibly reduced) copy of *ctx* that fits within *max_input_tokens*.

    The original context is never mutated.  When no truncation is needed the
    original object is returned unchanged and ``TruncationStats.total_dropped()``
    is 0.
    """
    raw_json = ctx.model_dump_json()
    if _estimate_tokens(raw_json) <= max_input_tokens:
        return ctx, TruncationStats()

    # Work on a mutable dict representation; sort each section so the
    # highest-priority items are first (index 0) and the lowest-priority items
    # are last (popped first).
    blob: dict[str, Any] = json.loads(raw_json)
    _sort_sections_in_place(ctx, blob)

    # Track the serialized length with a running total rather than re-dumping the
    # whole blob on every pop (issue #70: that re-serialization was O(n^2) on
    # exactly the large-context case truncation exists for). Popping the last
    # element of a JSON list removes its leading ", " separator plus its own
    # serialization, so the total stays byte-exact against json.dumps(blob).
    current_chars = len(json.dumps(blob))
    dropped: dict[str, int] = {}
    while True:
        progress = False
        for section in _DROP_ORDER:
            items: list[Any] = blob[section]["data"]
            if len(items) <= 1:
                continue
            popped = items.pop()
            dropped[section] = dropped.get(section, 0) + 1
            progress = True
            current_chars -= len(_ITEM_SEP) + len(json.dumps(popped))
            if _estimate_tokens_for_chars(current_chars) <= max_input_tokens:
                return IncidentContext.model_validate(blob), TruncationStats(dropped=dropped)
        if not progress:
            # Every section is already at ≤1 item; nothing left to drop.
            return IncidentContext.model_validate(blob), TruncationStats(dropped=dropped)


def _sort_sections_in_place(ctx: IncidentContext, blob: dict[str, Any]) -> None:
    """Reorder data lists so highest-priority items are first, lowest-priority last.

    Sorting is done against the typed Pydantic objects (for attribute access)
    and the result is written back into *blob* as plain dicts (for JSON round-
    tripping).  *blob* is mutated in place; *ctx* is never mutated.
    """
    # Logs / related / active alerts — newest first (drop oldest from tail).
    for name in ("recent_logs", "related_alerts", "active_alerts"):
        items = list(getattr(ctx, name).data)
        items.sort(
            key=lambda x: x.timestamp if hasattr(x, "timestamp") else x.opened_at,
            reverse=True,
        )
        blob[name]["data"] = [m.model_dump(mode="json") for m in items]

    # Runbooks / similar incidents — highest cosine first (drop lowest from tail).
    for name in ("runbooks", "similar_incidents"):
        items = list(getattr(ctx, name).data)
        items.sort(key=lambda x: x.cosine_similarity, reverse=True)
        blob[name]["data"] = [m.model_dump(mode="json") for m in items]

    # Deploys — newest first; always preserve index 0 (the newest).
    items = list(ctx.recent_deploys.data)
    items.sort(key=lambda x: x.deployed_at, reverse=True)
    blob["recent_deploys"]["data"] = [m.model_dump(mode="json") for m in items]
