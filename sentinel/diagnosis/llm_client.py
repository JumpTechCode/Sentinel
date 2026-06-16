# sentinel/diagnosis/llm_client.py
"""Anthropic streaming + forced tool-use wrapper. Only file importing ``anthropic``.

Timeout: wraps the stream with ``asyncio.wait_for(self.timeout_s)``.
Retry policy: one short-backoff retry on ``APIStatusError`` (5xx/429); no other retries.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, cast

import anthropic
import httpx
from anthropic import AsyncAnthropic
from anthropic.types import MessageParam, ToolParam
from pydantic import SecretStr

from sentinel.diagnosis.errors import LLMNoToolCall, LLMTimeout, LLMTransport
from sentinel.observability.tracing import span_for_llm

_BACKOFF_S = 0.5


@dataclass(frozen=True, slots=True)
class LLMResult:
    tool_input: dict[str, Any]
    input_tokens: int
    output_tokens: int
    stop_reason: str
    latency_ms: int
    # id of the returned tool_use block; needed to replay the turn in a repair
    # exchange (see RepairTurn). Empty only for hand-built test fixtures.
    tool_use_id: str = ""


@dataclass(frozen=True, slots=True)
class RepairTurn:
    """One round of schema-repair context for a follow-up diagnosis call.

    Replayed as a proper multi-turn exchange — the prior assistant tool_use
    turn plus a tool_result describing the validation error — rather than prose
    appended to the user message.
    """

    tool_use_id: str
    tool_input: dict[str, Any]
    error: str


class AnthropicClient:
    def __init__(
        self,
        *,
        api_key: SecretStr,
        model: str,
        timeout_s: float,
        max_output_tokens: int,
        client: AsyncAnthropic | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.model = model
        self.timeout_s = timeout_s
        self.max_output_tokens = max_output_tokens
        if client is not None:
            self._client = client
        elif http_client is not None:
            self._client = AsyncAnthropic(
                api_key=api_key.get_secret_value(),
                http_client=http_client,
            )
        else:
            self._client = AsyncAnthropic(api_key=api_key.get_secret_value())

    async def diagnose_call(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
        repair: RepairTurn | None = None,
    ) -> LLMResult:
        # Wrap the whole call (incl. timeout + retry) in one span. gh#64:
        # `span_for_llm` previously had zero call sites, so no LLM spans existed.
        with span_for_llm(self.model):
            try:
                return await asyncio.wait_for(
                    self._call_with_retry(
                        system=system,
                        user=user,
                        tool_schema=tool_schema,
                        tool_name=tool_name,
                        repair=repair,
                    ),
                    timeout=self.timeout_s,
                )
            except TimeoutError as e:
                raise LLMTimeout(f"LLM call exceeded {self.timeout_s}s") from e

    async def _call_with_retry(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
        repair: RepairTurn | None,
    ) -> LLMResult:
        try:
            return await self._stream_once(
                system=system,
                user=user,
                tool_schema=tool_schema,
                tool_name=tool_name,
                repair=repair,
            )
        except anthropic.APIStatusError:
            await asyncio.sleep(_BACKOFF_S)
            try:
                return await self._stream_once(
                    system=system,
                    user=user,
                    tool_schema=tool_schema,
                    tool_name=tool_name,
                    repair=repair,
                )
            except anthropic.APIStatusError as inner:
                raise LLMTransport(str(inner)) from inner

    @staticmethod
    def _build_messages(user: str, tool_name: str, repair: RepairTurn | None) -> list[MessageParam]:
        if repair is None:
            return [{"role": "user", "content": user}]
        # Replay the original prompt, the model's prior (invalid) tool_use turn,
        # and a tool_result carrying the validation error — a real multi-turn
        # repair exchange rather than prose appended to the user message.
        return cast(
            list[MessageParam],
            [
                {"role": "user", "content": user},
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": repair.tool_use_id,
                            "name": tool_name,
                            "input": repair.tool_input,
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": repair.tool_use_id,
                            "content": repair.error,
                            "is_error": True,
                        }
                    ],
                },
            ],
        )

    async def _stream_once(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
        repair: RepairTurn | None,
    ) -> LLMResult:
        start = time.monotonic()
        stream = self._client.messages.stream(
            model=self.model,
            max_tokens=self.max_output_tokens,
            # temperature=0 for run-to-run stability; the API exposes no seed, so
            # this is as deterministic as a single-shot call can be.
            temperature=0,
            system=system,
            messages=self._build_messages(user, tool_name, repair),
            tools=[cast(ToolParam, tool_schema)],
            tool_choice={"type": "tool", "name": tool_name},
        )
        async with stream as s:
            final = await s.get_final_message()
        latency_ms = int((time.monotonic() - start) * 1000)

        for block in final.content:
            kind = getattr(block, "type", None)
            name = getattr(block, "name", None)
            if kind == "tool_use" and name == tool_name:
                tool_input = getattr(block, "input", None) or {}
                usage = final.usage
                return LLMResult(
                    tool_input=tool_input,
                    input_tokens=getattr(usage, "input_tokens", 0),
                    output_tokens=getattr(usage, "output_tokens", 0),
                    stop_reason=getattr(final, "stop_reason", "tool_use"),
                    latency_ms=latency_ms,
                    tool_use_id=getattr(block, "id", "") or "",
                )
        raise LLMNoToolCall("model did not call the required tool")
