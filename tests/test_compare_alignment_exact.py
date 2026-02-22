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


def test_compare_alignment_exact_join(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")

    store.insert_entries([_entry("A", "alpha", 1), _entry("A", "beta", 2)])
    store.insert_entries([_entry("B", "alpha", 1), _entry("B", "delta", 2)])

    preview = preview_alignment(
        db_path=project_paths(project_dir).db_path,
        corpus_a="A",
        corpus_b="B",
        mode="exact",
        threshold=90,
        limit=500,
    )

    assert preview["counts"]["matched_exact"] == 1
    assert preview["counts"]["matched_fuzzy"] == 0

    exact_rows = [row for row in preview["rows"] if row["method"] == "exact"]
    assert len(exact_rows) == 1
    assert exact_rows[0]["headword_key"] == "alpha"
