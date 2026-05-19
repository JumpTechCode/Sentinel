# tests/unit/observability/test_cost.py
"""Token-to-USD calculator for LLM cost tracking."""

from __future__ import annotations

import logging
from collections.abc import Generator
from decimal import Decimal

import pytest
from sentinel.observability.cost import _WARNED_MODELS, usd_cost


@pytest.fixture(autouse=True)
def _reset_warnings() -> Generator[None, None, None]:
    _WARNED_MODELS.clear()
    yield
    _WARNED_MODELS.clear()


def test_known_model_known_cost() -> None:
    # claude-sonnet-4-5: $3.00 per 1M input, $15.00 per 1M output (placeholder rates)
    cost = usd_cost("claude-sonnet-4-5", input_tokens=1_000_000, output_tokens=0)
    assert cost == Decimal("3.00")


def test_known_model_combined_input_output() -> None:
    cost = usd_cost("claude-sonnet-4-5", input_tokens=1_000_000, output_tokens=1_000_000)
    assert cost == Decimal("18.00")


def test_unknown_model_returns_zero_and_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="sentinel.cost")
    assert usd_cost("totally-fake-model", 1_000, 1_000) == Decimal("0")
    assert usd_cost("totally-fake-model", 1_000, 1_000) == Decimal("0")
    warnings = [r for r in caplog.records if "totally-fake-model" in r.getMessage()]
    assert len(warnings) == 1
