from __future__ import annotations

from pathlib import Path

import pytest

from dicodiachro.core.compare.workflow import (
    CompareWorkflowError,
    apply_compare_run,
    load_compare_run_data,
    preview_alignment,
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


def _seed_for_algorithm_case(store: SQLiteStore) -> None:
    store.ensure_dictionary("A")
    store.ensure_dictionary("B")
    store.insert_entries(
        [
            _entry("A", "bfdf", 1),
            _entry("A", "dfec", 2),
            _entry("A", "efde", 3),
        ]
    )
    store.insert_entries(
        [
            _entry("B", "efad", 1),
            _entry("B", "defe", 2),
            _entry("B", "dcea", 3),
        ]
    )


def test_compare_preview_algorithm_branching_changes_result(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)
    _seed_for_algorithm_case(store)

    greedy = preview_alignment(
        db_path=db_path,
        corpus_a="A",
        corpus_b="B",
        mode="exact+fuzzy",
        threshold=70,
        limit=500,
        include_unmatched=False,
        algorithm="greedy",
    )
    mutual = preview_alignment(
        db_path=db_path,
        corpus_a="A",
        corpus_b="B",
        mode="exact+fuzzy",
        threshold=70,
        limit=500,
        include_unmatched=False,
        algorithm="mutual_best",
    )

    greedy_pairs = {
        (str(row.get("headword_norm_a") or ""), str(row.get("headword_norm_b") or ""))
        for row in greedy["rows"]
        if row.get("method") == "fuzzy"
    }
    mutual_pairs = {
        (str(row.get("headword_norm_a") or ""), str(row.get("headword_norm_b") or ""))
        for row in mutual["rows"]
        if row.get("method") == "fuzzy"
    }

    assert greedy["algorithm"] == "greedy"
    assert mutual["algorithm"] == "mutual_best"
    assert greedy["counts"]["matched_fuzzy"] == 2
    assert mutual["counts"]["matched_fuzzy"] == 1
    assert ("efde", "efad") in greedy_pairs
    assert ("efde", "efad") not in mutual_pairs


def test_compare_apply_run_uses_algorithm_for_alignment(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)
    _seed_for_algorithm_case(store)

    payload = apply_compare_run(
        db_path=db_path,
        corpus_ids=["A", "B"],
        corpus_a="A",
        corpus_b="B",
        settings={
            "key_field": "headword_norm_effective",
            "mode": "exact+fuzzy",
            "fuzzy_threshold": 70,
            "algorithm": "mutual_best",
        },
    )

    loaded = load_compare_run_data(db_path, str(payload["run_id"]))
    fuzzy_rows = [row for row in loaded["alignment"]["rows"] if row.get("method") == "fuzzy"]

    assert payload["algorithm"] == "mutual_best"
    assert loaded["run"]["algorithm"] == "mutual_best"
    assert len(fuzzy_rows) == 1


def test_compare_preview_invalid_algorithm_rejected(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)
    _seed_for_algorithm_case(store)

    with pytest.raises(CompareWorkflowError, match="algorithm must be one of"):
        preview_alignment(
            db_path=db_path,
            corpus_a="A",
            corpus_b="B",
            mode="exact+fuzzy",
            threshold=70,
            limit=500,
            include_unmatched=False,
            algorithm="unknown",
        )
