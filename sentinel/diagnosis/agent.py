# sentinel/diagnosis/agent.py
"""Single-shot diagnosis agent.

Pure-async, no I/O beyond the LLM call. Consumer/Repo wiring lives in
``sentinel/diagnosis/consumer.py`` and ``sentinel/persistence/repositories.py``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from sentinel.diagnosis.deps import DiagnosisDeps
from sentinel.diagnosis.errors import DiagnosisInvalid
from sentinel.diagnosis.llm_client import RepairTurn
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.diagnosis.prompt import serialize
from sentinel.diagnosis.truncation import truncate_for_budget
from sentinel.diagnosis.validation import verify_evidence
from sentinel.observability.cost import usd_cost
from sentinel.observability.metrics import (
    diagnosis_confidence,
    diagnosis_input_truncated_total,
    diagnosis_latency_seconds,
    diagnosis_llm_tokens_total,
    hallucinated_evidence_total,
    llm_cost_usd_total,
    llm_tokens_total,
)
from sentinel.schemas.api import IncidentDetailResponse
from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis

_LOG = logging.getLogger("sentinel.diagnosis.agent")

_TOOL_NAME = "submit_diagnosis"
_HALLUCINATION_CAP = Decimal("0.4")


def _tool_schema() -> dict[str, Any]:
    return {
        "name": _TOOL_NAME,
        "description": "Submit your structured diagnosis.",
        "input_schema": Diagnosis.model_json_schema(),
    }


async def diagnose(
    incident: IncidentDetailResponse,
    context: IncidentContext,
    deps: DiagnosisDeps,
) -> PersistedDiagnosis:
    ctx, trunc = truncate_for_budget(context, max_input_tokens=deps.max_input_tokens)
    for section, n in trunc.dropped.items():
        diagnosis_input_truncated_total.labels(section=section).inc(n)

    system = deps.prompt.system_text
    user = serialize(incident, ctx)
    tool_schema = _tool_schema()

    diagnosis_obj: Diagnosis | None = None
    last_result = None
    repair: RepairTurn | None = None
    for attempt in (1, 2):
        last_result = await deps.llm.diagnose_call(
            system=system,
            user=user,
            tool_schema=tool_schema,
            tool_name=_TOOL_NAME,
            repair=repair,
        )
        # Meter tokens and cost per attempt: a retried attempt still spent
        # tokens against the Anthropic bill, so all attempts must be counted
        # (audit logger already records one line per attempt).
        attempt_cost = usd_cost(deps.llm.model, last_result.input_tokens, last_result.output_tokens)
        diagnosis_llm_tokens_total.labels(kind="input").inc(last_result.input_tokens)
        diagnosis_llm_tokens_total.labels(kind="output").inc(last_result.output_tokens)
        llm_tokens_total.labels(model=deps.llm.model, kind="input").inc(last_result.input_tokens)
        llm_tokens_total.labels(model=deps.llm.model, kind="output").inc(last_result.output_tokens)
        llm_cost_usd_total.labels(model=deps.llm.model).inc(float(attempt_cost))
        deps.audit_logger.record(
            incident_id=str(incident.id),
            model=deps.llm.model,
            prompt_version=deps.prompt.version,
            prompt_sha=deps.prompt.sha256_hex,
            input_tokens=last_result.input_tokens,
            output_tokens=last_result.output_tokens,
            cost_usd=str(attempt_cost),
            retry_attempt=attempt,
        )
        try:
            diagnosis_obj = Diagnosis.model_validate(last_result.tool_input)
            break
        except ValidationError as e:
            if attempt == 2:
                raise DiagnosisInvalid(str(e)) from e
            # Repair as a proper multi-turn exchange: replay the model's invalid
            # tool_use turn and answer it with a tool_result error, rather than
            # appending prose to the user message.
            repair = RepairTurn(
                tool_use_id=last_result.tool_use_id,
                tool_input=last_result.tool_input,
                error=(
                    f"Your previous response failed schema validation: {e}. "
                    f"Return a corrected {_TOOL_NAME} call."
                ),
            )

    # Both variables are always set after the loop: the loop runs at least once
    # and exits either via `break` (success) or by raising DiagnosisInvalid on
    # attempt 2. This check exists purely to satisfy the type-checker.
    if diagnosis_obj is None or last_result is None:  # pragma: no cover
        raise DiagnosisInvalid("internal: loop exited without a result")

    verdict = verify_evidence(diagnosis_obj, ctx)
    confidence = Decimal(str(diagnosis_obj.confidence))
    if verdict.hallucinated:
        confidence = min(confidence, _HALLUCINATION_CAP)
        hallucinated_evidence_total.inc()

    diagnosis_latency_seconds.observe(last_result.latency_ms / 1000.0)
    diagnosis_confidence.observe(float(confidence))
    # Token/cost counters are incremented per-attempt inside the retry loop;
    # `attempt_cost` here is the successful attempt's cost (from the final loop
    # iteration), reused for the `token_usage` field on the persisted record.
    cost = attempt_cost

    return PersistedDiagnosis(
        hypothesis=diagnosis_obj.hypothesis,
        confidence=confidence,
        reasoning=diagnosis_obj.reasoning,
        evidence=verdict.verified,
        suggested_actions=list(diagnosis_obj.suggested_actions),
        likely_category=diagnosis_obj.likely_category,
        hallucinated_evidence=verdict.hallucinated,
        model=deps.llm.model,
        prompt_version=deps.prompt.version,
        latency_ms=last_result.latency_ms,
        token_usage={
            "input": last_result.input_tokens,
            "output": last_result.output_tokens,
            "cost_usd": str(cost),
            "prompt_sha": deps.prompt.sha256_hex,
            "truncated": trunc.to_dict(),
        },
    )
