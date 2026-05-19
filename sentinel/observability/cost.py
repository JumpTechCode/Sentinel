# sentinel/observability/cost.py
"""LLM cost in USD from token counts. Update the price table as Anthropic publishes.

Rates are USD per 1M tokens. If a model isn't in the table, we return zero and
log a single WARNING (so cost goes uncounted rather than miscounted).
"""

from __future__ import annotations

import logging
from decimal import Decimal

_LOG = logging.getLogger("sentinel.cost")

# Per 1M tokens; placeholder values, easy to update.
_PRICES: dict[str, tuple[Decimal, Decimal]] = {
    "claude-sonnet-4-5": (Decimal("3.00"), Decimal("15.00")),
    "claude-opus-4-7": (Decimal("15.00"), Decimal("75.00")),
    "claude-haiku-4-5": (Decimal("0.80"), Decimal("4.00")),
}

_WARNED_MODELS: set[str] = set()
_PER_MILLION = Decimal("1000000")


def usd_cost(model: str, input_tokens: int, output_tokens: int) -> Decimal:
    rates = _PRICES.get(model)
    if rates is None:
        if model not in _WARNED_MODELS:
            _WARNED_MODELS.add(model)
            _LOG.warning("no price table entry for model %r — cost reported as 0", model)
        return Decimal("0")
    input_rate, output_rate = rates
    return (
        Decimal(input_tokens) * input_rate / _PER_MILLION
        + Decimal(output_tokens) * output_rate / _PER_MILLION
    )
