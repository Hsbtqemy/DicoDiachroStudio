from __future__ import annotations

from pathlib import Path

from dicodiachro.core.compare.workflow import (
    alignment_letter_counts,
    apply_compare_run,
    coverage_letter_counts,
    diff_letter_counts,
    preview_alignment,
    preview_coverage,
    preview_diff,
)
from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths


def _entry(corpus_id: str, headword: str, line_no: int) -> ParsedEntry:
    return ParsedEntry(
        dict_id=corpus_id,
        section="AA",
        syllables=1,
        headword_raw=headword,
        pos_raw="v",
        pron_raw=headword,
        source_path=f"{corpus_id}.txt",
        line_no=line_no,
        raw_line=f"1 {headword}, v",
    )


def test_compare_preview_alpha_filter(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")
    store.insert_entries([_entry("A", "alpha", 1), _entry("A", "beta", 2)])
    store.insert_entries([_entry("B", "alpha", 1), _entry("B", "bravo", 2)])

    coverage = preview_coverage(db_path, ["A", "B"], alpha_bucket="A", limit=100)
    assert coverage["rows"]
    assert all(str(row["headword_key"]).startswith("a") for row in coverage["rows"])

    alignment = preview_alignment(
        db_path,
        "A",
        "B",
        mode="exact",
        threshold=90,
        limit=200,
        include_unmatched=True,
        alpha_bucket="B",
    )
    assert alignment["rows"]
    assert all(str(row["headword_key"]).startswith("b") for row in alignment["rows"])

    diff = preview_diff(
        db_path,
        run_settings={"alignment_rows": alignment["rows"], "corpus_a": "A", "corpus_b": "B"},
        limit=200,
        alpha_bucket="A",
    )
    assert diff["rows"] == []


def test_compare_letter_counts(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")
    store.insert_entries(
        [
            _entry("A", "alpha", 1),
            _entry("A", "beta", 2),
            _entry("A", "delta", 3),
        ]
    )
    store.insert_entries(
        [
            _entry("B", "alpha", 1),
            _entry("B", "bravo", 2),
            _entry("B", "echo", 3),
        ]
    )

    result = apply_compare_run(
        db_path=db_path,
        corpus_ids=["A", "B"],
        corpus_a="A",
        corpus_b="B",
        settings={
            "key_field": "headword_norm_effective",
            "mode": "exact",
            "fuzzy_threshold": 90,
            "algorithm": "greedy",
        },
    )
    run_id = str(result["run_id"])

    coverage_counts = coverage_letter_counts(db_path, run_id)
    alignment_counts = alignment_letter_counts(db_path, run_id)
    diff_counts = diff_letter_counts(db_path, run_id)

    assert coverage_counts["A"] >= 1
    assert coverage_counts["B"] >= 1
    assert alignment_counts["A"] >= 1
    assert alignment_counts["B"] >= 1
    assert diff_counts["A"] >= 1
