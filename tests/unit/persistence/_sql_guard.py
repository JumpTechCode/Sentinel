"""AST-based detector for SQLAlchemy construct calls outside persistence/.

Replaces the previous regex-based check, which false-positived on any method
named `update()` / `delete()` / etc. and on string literals containing
`select(`. This detector only flags calls to names that are actually imported
from `sqlalchemy` (or `sqlalchemy.*`) at the top of the file.

Patterns recognized:
- ``from sqlalchemy import select, insert, update, delete, text`` → bare calls
  like ``select(...)``, ``update(...)`` are flagged.
- ``import sqlalchemy as sa`` (or ``import sqlalchemy``) → ``sa.select(...)``
  is flagged.
- Aliased imports like ``from sqlalchemy import select as sa_select`` → calls
  to the alias name are flagged.

A method call like ``some_dict.update(value)`` is NOT flagged because the
function being called is an ``Attribute`` whose value is not the sqlalchemy
module alias.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

# The SQLAlchemy construct names that constitute "raw SQL" when used outside
# the persistence layer. `func` and `case` are deliberately excluded — they're
# expression builders, not standalone constructs, and tend to be embedded in
# defaults or check-constraint definitions that legitimately live in models.
_SA_CONSTRUCTS: frozenset[str] = frozenset({"select", "insert", "update", "delete", "text"})


@dataclass(frozen=True, slots=True)
class Violation:
    file: str
    line: int
    name: str


def find_raw_sql_calls(source: str, filename: str) -> list[Violation]:
    """Return every call site inside ``source`` that invokes a SQLAlchemy
    construct from ``_SA_CONSTRUCTS``. Returns an empty list if the file does
    not import ``sqlalchemy`` at all.
    """
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        # Not parseable Python (shouldn't happen for tracked .py files); skip.
        return []

    construct_aliases, module_aliases = _collect_sqlalchemy_imports(tree)
    if not construct_aliases and not module_aliases:
        return []

    violations: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _called_construct_name(node.func, construct_aliases, module_aliases)
        if name is not None:
            violations.append(Violation(file=filename, line=node.lineno, name=name))
    return violations


def _collect_sqlalchemy_imports(
    tree: ast.AST,
) -> tuple[dict[str, str], frozenset[str]]:
    """Return ``(construct_aliases, module_aliases)``.

    - ``construct_aliases``: ``{local_name: canonical_construct}`` for every
      ``from sqlalchemy(.*) import select [as foo]`` style import where the
      imported name is in ``_SA_CONSTRUCTS``.
    - ``module_aliases``: names bound by ``import sqlalchemy [as sa]`` or
      ``import sqlalchemy.foo as bar``.
    """
    construct_aliases: dict[str, str] = {}
    module_aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root == "sqlalchemy":
                    module_aliases.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            if not (node.module == "sqlalchemy" or node.module.startswith("sqlalchemy.")):
                continue
            for alias in node.names:
                if alias.name in _SA_CONSTRUCTS:
                    construct_aliases[alias.asname or alias.name] = alias.name
    return construct_aliases, frozenset(module_aliases)


def _called_construct_name(
    func: ast.expr,
    construct_aliases: dict[str, str],
    module_aliases: frozenset[str],
) -> str | None:
    """If ``func`` is a SQLAlchemy construct invocation, return its canonical
    name (``select``/``insert``/etc.); otherwise return None.
    """
    if isinstance(func, ast.Name):
        return construct_aliases.get(func.id)
    if isinstance(func, ast.Attribute):
        # `sa.select(...)` — value is a Name matching a module alias and attr
        # matches a construct.
        if (
            isinstance(func.value, ast.Name)
            and func.value.id in module_aliases
            and func.attr in _SA_CONSTRUCTS
        ):
            return func.attr
    return None


def scan_tree(root: Path, *, allowed_prefixes: tuple[str, ...]) -> list[Violation]:
    """Scan every ``.py`` file under ``root`` that doesn't live under one of
    ``allowed_prefixes`` (relative to ``root``). Returns all violations.
    """
    out: list[Violation] = []
    for py in sorted(root.rglob("*.py")):
        rel = py.relative_to(root).as_posix()
        if any(rel.startswith(p) for p in allowed_prefixes):
            continue
        src = py.read_text(encoding="utf-8")
        out.extend(find_raw_sql_calls(src, rel))
    return out
