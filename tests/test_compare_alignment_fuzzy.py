from __future__ import annotations

from pathlib import Path

from dicodiachro.core.compare.workflow import preview_alignment
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


def test_compare_alignment_fuzzy_greedy_no_duplicate(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")

    store.insert_entries([_entry("A", "color", 1), _entry("A", "colur", 2)])
    store.insert_entries([_entry("B", "colour", 1), _entry("B", "beta", 2)])

    preview = preview_alignment(
        db_path=project_paths(project_dir).db_path,
        corpus_a="A",
        corpus_b="B",
        mode="exact+fuzzy",
        threshold=80,
        limit=500,
    )

    fuzzy_rows = [row for row in preview["rows"] if row["method"] == "fuzzy"]
    assert len(fuzzy_rows) == 1
    assert preview["counts"]["matched_fuzzy"] == 1

    matched_b_ids = [row["entry_id_b"] for row in fuzzy_rows if row.get("entry_id_b")]
    assert len(set(matched_b_ids)) == len(matched_b_ids)
