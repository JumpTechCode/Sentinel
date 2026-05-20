"""Unit tests for sentinel.evals.corpus_loader."""

from __future__ import annotations

from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"


def test_load_case_round_trips_a_valid_yaml() -> None:
    from sentinel.evals.corpus_loader import load_case

    case = load_case(_FIXTURES / "cloudflare-bgp.yaml")
    assert case.id == "cloudflare-bgp-test-fixture"
    assert case.alert.service == "edge-network"
    assert case.alert.severity == "SEV1"
    assert len(case.context_seed.deploys) == 1
    assert case.context_seed.deploys[0].id == "deploy:abc123"
    assert case.ground_truth.category == "config"


def test_load_case_raises_on_missing_required_section() -> None:
    from sentinel.evals.corpus_loader import CorpusValidationError, load_case

    with pytest.raises(CorpusValidationError) as exc:
        load_case(_FIXTURES / "broken-missing-ground-truth.yaml")
    assert "ground_truth" in str(exc.value).lower()
    # The exception carries the source path for operator clarity
    assert "broken-missing-ground-truth.yaml" in str(exc.value)


def test_load_case_raises_on_missing_file() -> None:
    from sentinel.evals.corpus_loader import load_case

    with pytest.raises(FileNotFoundError):
        load_case(_FIXTURES / "does-not-exist.yaml")


def test_load_corpus_dir_returns_sorted_by_id(tmp_path: Path) -> None:
    """load_corpus_dir loads every *.yaml under a directory, sorted by case.id.
    Sorted order matters for the deterministic 5-case smoke subset (design §7).
    """
    from sentinel.evals.corpus_loader import load_corpus_dir

    # Copy the fixture into a fresh dir + a renamed copy to verify ordering
    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "z-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "z-case"))
    (tmp_path / "a-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "a-case"))
    (tmp_path / "m-case.yaml").write_text(src.replace("cloudflare-bgp-test-fixture", "m-case"))

    cases = load_corpus_dir(tmp_path)
    assert [c.id for c in cases] == ["a-case", "m-case", "z-case"]


def test_load_corpus_dir_raises_on_duplicate_ids(tmp_path: Path) -> None:
    from sentinel.evals.corpus_loader import CorpusValidationError, load_corpus_dir

    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "one.yaml").write_text(src)  # id: cloudflare-bgp-test-fixture
    (tmp_path / "two.yaml").write_text(src)  # same id

    with pytest.raises(CorpusValidationError, match="duplicate"):
        load_corpus_dir(tmp_path)


def test_load_corpus_dir_skips_non_yaml(tmp_path: Path) -> None:
    """README.md and other non-YAML files in the corpus directory are ignored."""
    from sentinel.evals.corpus_loader import load_corpus_dir

    src = (_FIXTURES / "cloudflare-bgp.yaml").read_text()
    (tmp_path / "case.yaml").write_text(src)
    (tmp_path / "README.md").write_text("# Corpus README")
    (tmp_path / "notes.txt").write_text("ignore me")

    cases = load_corpus_dir(tmp_path)
    assert len(cases) == 1
