from __future__ import annotations

from pathlib import Path

from dicodiachro.core.compare.workflow import apply_compare_run, load_compare_run_data
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


def test_compare_run_storage_writes_tables_and_stats(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")

    store.insert_entries([_entry("A", "alpha", 1), _entry("A", "beta", 2)])
    store.insert_entries([_entry("B", "alpha", 1), _entry("B", "gamma", 2)])

    result = apply_compare_run(
        db_path=db_path,
        corpus_ids=["A", "B"],
        corpus_a="A",
        corpus_b="B",
        settings={
            "key_field": "headword_norm_effective",
            "mode": "exact+fuzzy",
            "fuzzy_threshold": 90,
            "algorithm": "greedy",
        },
    )

    run_id = result["run_id"]
    assert run_id

    run = store.compare_run_by_id(run_id)
    assert run is not None

    coverage_items = store.compare_coverage_items(run_id)
    assert coverage_items

    alignment_pairs = store.compare_alignment_pairs(run_id)
    assert alignment_pairs

    payload = load_compare_run_data(db_path, run_id)
    assert payload["run"]["run_id"] == run_id
    assert payload["coverage"]["rows"]
