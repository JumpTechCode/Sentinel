# tests/unit/persistence/test_no_raw_sql_outside.py
"""Enforce the architectural rule: only `sentinel/persistence/` writes SQL.

If you need DB access from another module, add a repository method, not a query.
"""

from __future__ import annotations

from pathlib import Path

from tests.unit.persistence._sql_guard import scan_tree

_ROOT = Path(__file__).resolve().parents[3] / "sentinel"
_ALLOWED_PREFIXES = ("persistence/",)


def test_no_raw_sql_outside_persistence() -> None:
    offenders = scan_tree(_ROOT, allowed_prefixes=_ALLOWED_PREFIXES)
    assert offenders == [], (
        "Files outside sentinel/persistence/ invoke SQLAlchemy constructs "
        "(select/insert/update/delete/text); add a repository method instead. "
        f"Offenders: {[f'{v.file}:{v.line} {v.name}()' for v in offenders]}"
    )
