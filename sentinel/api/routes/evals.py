# sentinel/api/routes/evals.py
"""Eval-run read routes — list / detail / baseline.

Read-only over `EvalRunRepository` (lifespan-stashed as
`request.app.state.eval_run_repo`). Triggering a run is intentionally not an
API operation — see docs/adr/0014-defer-eval-run-trigger-endpoint.md.
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from sentinel.persistence.repositories import EvalRunRecord, EvalRunRepository
from sentinel.schemas.api import EvalRunDetail, EvalRunSummary

router = APIRouter(tags=["evals"])


def _summary(record: EvalRunRecord) -> EvalRunSummary:
    return EvalRunSummary(
        id=record.id,
        started_at=record.started_at,
        completed_at=record.completed_at,
        model=record.model,
        prompt_version=record.prompt_version,
        corpus_version=record.corpus_version,
        summary=record.metrics or None,
    )


def _detail(record: EvalRunRecord) -> EvalRunDetail:
    return EvalRunDetail(
        id=record.id,
        status=record.status,
        trigger=record.trigger,
        git_sha=record.git_sha,
        model=record.model,
        prompt_version=record.prompt_version,
        embedding_model_id=record.embedding_model_id,
        corpus_version=record.corpus_version,
        corpus_size=record.corpus_size,
        shots_per_case=record.shots_per_case,
        started_at=record.started_at,
        completed_at=record.completed_at,
        metrics=record.metrics,
        metrics_stability=record.metrics_stability,
        regression_passed=record.regression_passed,
        regression_baseline_sha=record.regression_baseline_sha,
        regression_detail=record.regression_detail,
    )


@router.get("/evals/runs", response_model=list[EvalRunSummary])
async def list_eval_runs(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[EvalRunSummary]:
    repo: EvalRunRepository = request.app.state.eval_run_repo
    records = await repo.list_recent(limit=limit)
    return [_summary(r) for r in records]
