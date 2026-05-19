"""Meta-tests for the AST-based no-raw-SQL detector.

These guard against regressions in the guard itself — false positives on
common Python patterns like `dict.update(...)`, and false negatives on
genuine SQLAlchemy usage.
"""

from __future__ import annotations

from tests.unit.persistence._sql_guard import find_raw_sql_calls


def _scan(src: str) -> list[tuple[int, str]]:
    return [(v.line, v.name) for v in find_raw_sql_calls(src, "<test>")]


# --- True positives --------------------------------------------------------- #


def test_flags_from_sqlalchemy_import_select() -> None:
    src = """
from sqlalchemy import select
async def get_all(s):
    return await s.execute(select(Model))
"""
    assert _scan(src) == [(4, "select")]


def test_flags_each_construct_from_direct_import() -> None:
    src = """
from sqlalchemy import select, insert, update, delete, text
def all_of_them(s):
    s.execute(select(M))
    s.execute(insert(M).values(x=1))
    s.execute(update(M).where(M.id == 1))
    s.execute(delete(M))
    s.execute(text("SELECT 1"))
"""
    names = [n for _, n in _scan(src)]
    assert sorted(names) == ["delete", "insert", "select", "text", "update"]


def test_flags_aliased_module_call() -> None:
    src = """
import sqlalchemy as sa
def fetch(s):
    s.execute(sa.select(M))
"""
    assert _scan(src) == [(4, "select")]


def test_flags_aliased_construct_import() -> None:
    src = """
from sqlalchemy import select as sa_select
def fetch(s):
    s.execute(sa_select(M))
"""
    assert _scan(src) == [(4, "select")]


def test_flags_from_sqlalchemy_submodule() -> None:
    src = """
from sqlalchemy.sql import select
def fetch(s):
    s.execute(select(M))
"""
    assert _scan(src) == [(4, "select")]


# --- True negatives --------------------------------------------------------- #


def test_does_not_flag_dict_update_method_call() -> None:
    src = """
def merge(a, b):
    a.update(b)
    return a
"""
    assert _scan(src) == []


def test_does_not_flag_user_defined_function_named_update() -> None:
    src = """
def update(thing):
    return thing + 1

def caller():
    return update(5)
"""
    assert _scan(src) == []


def test_does_not_flag_method_named_delete_on_instance() -> None:
    src = """
class Repo:
    def delete(self, x):
        ...

def caller(r):
    r.delete(42)
"""
    assert _scan(src) == []


def test_does_not_flag_select_inside_string_literal() -> None:
    src = """
def help_text():
    return "Use select() in repositories only"
"""
    assert _scan(src) == []


def test_does_not_flag_select_inside_docstring() -> None:
    src = '''
def f():
    """Calls select() on the session, eventually."""
    return None
'''
    assert _scan(src) == []


def test_does_not_flag_select_inside_comment() -> None:
    src = """
def f():
    # this would call select() if it were uncommented
    return None
"""
    assert _scan(src) == []


def test_file_without_sqlalchemy_import_is_skipped() -> None:
    src = """
def select():
    return 1

def caller():
    return select()
"""
    # No sqlalchemy import → no flags, even though the bare name matches.
    assert _scan(src) == []


def test_invalid_python_does_not_raise() -> None:
    assert find_raw_sql_calls("def broken(:", "<bad>") == []
