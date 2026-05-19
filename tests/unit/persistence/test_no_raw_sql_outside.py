# tests/unit/persistence/test_no_raw_sql_outside.py
"""Enforce the architectural rule: only `sentinel/persistence/` writes SQL.

If you need DB access from another module, add a repository method, not a query.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3] / "sentinel"
_FORBIDDEN = re.compile(r"\b(select|insert|update|delete|text)\s*\(", re.IGNORECASE)
_ALLOWED_PREFIXES = ("persistence/",)


def _is_allowed(path: Path) -> bool:
    rel = path.relative_to(_ROOT).as_posix()
    return any(rel.startswith(p) for p in _ALLOWED_PREFIXES)


def test_no_raw_sql_outside_persistence() -> None:
    offenders: list[str] = []
    for py in _ROOT.rglob("*.py"):
        if _is_allowed(py):
            continue
        src = py.read_text(encoding="utf-8")
        # Strip comments and strings (rough): drop lines starting with #
        sanitized = "\n".join(line for line in src.splitlines() if not line.strip().startswith("#"))
        if _FORBIDDEN.search(sanitized):
            offenders.append(py.relative_to(_ROOT).as_posix())
    assert offenders == [], (
        "Files outside sentinel/persistence/ contain raw SQL constructs "
        f"(select/insert/update/delete/text()): {offenders}. "
        "Add a repository method instead."
    )
