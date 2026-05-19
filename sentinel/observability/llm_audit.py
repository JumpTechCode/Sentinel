# sentinel/observability/llm_audit.py
"""One-line-per-call JSON audit log for every LLM invocation.

Spec invariant: every LLM call is audit-logged. The path is configurable via
``Settings.llm_audit_log_path``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class LLMAuditLogger:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, **fields: Any) -> None:
        payload: dict[str, Any] = {"ts": datetime.now(UTC).isoformat(), **fields}
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
