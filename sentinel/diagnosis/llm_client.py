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
from anthropic.types import ToolParam
from pydantic import SecretStr

from sentinel.diagnosis.errors import LLMNoToolCall, LLMTimeout, LLMTransport

_BACKOFF_S = 0.5


@dataclass(frozen=True, slots=True)
class LLMResult:
    tool_input: dict[str, Any]
    input_tokens: int
    output_tokens: int
    stop_reason: str
    latency_ms: int


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
    ) -> LLMResult:
        try:
            return await asyncio.wait_for(
                self._call_with_retry(
                    system=system,
                    user=user,
                    tool_schema=tool_schema,
                    tool_name=tool_name,
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
    ) -> LLMResult:
        try:
            return await self._stream_once(
                system=system,
                user=user,
                tool_schema=tool_schema,
                tool_name=tool_name,
            )
        except anthropic.APIStatusError:
            await asyncio.sleep(_BACKOFF_S)
            try:
                return await self._stream_once(
                    system=system,
                    user=user,
                    tool_schema=tool_schema,
                    tool_name=tool_name,
                )
            except anthropic.APIStatusError as inner:
                raise LLMTransport(str(inner)) from inner

    async def _stream_once(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
    ) -> LLMResult:
        start = time.monotonic()
        stream = self._client.messages.stream(
            model=self.model,
            max_tokens=self.max_output_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
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
                )
        raise LLMNoToolCall("model did not call the required tool")
