from __future__ import annotations

from pathlib import Path

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.overrides import soft_delete_entries
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro.core.utils import alpha_bucket_of


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


def test_alpha_bucket_of() -> None:
    assert alpha_bucket_of("alpha") == "A"
    assert alpha_bucket_of(" Éclair") == "E"
    assert alpha_bucket_of("3alpha") == "A"
    assert alpha_bucket_of("123") == "#"


def test_list_entries_alpha_bucket_filter(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("corpus_test")
    store.insert_entries(
        [
            _entry("corpus_test", "alpha", 1),
            _entry("corpus_test", "Éclair", 2),
            _entry("corpus_test", "123", 3),
            _entry("corpus_test", "beta", 4),
        ]
    )

    rows_a = store.list_entries("corpus_test", alpha_bucket="A", include_deleted=True)
    rows_e = store.list_entries("corpus_test", alpha_bucket="E", include_deleted=True)
    rows_hash = store.list_entries("corpus_test", alpha_bucket="#", include_deleted=True)

    assert [row["headword_raw"] for row in rows_a] == ["alpha"]
    assert [row["headword_raw"] for row in rows_e] == ["Éclair"]
    assert [row["headword_raw"] for row in rows_hash] == ["123"]


def test_alpha_counts_respects_deleted(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("corpus_test")
    store.insert_entries(
        [
            _entry("corpus_test", "alpha", 1),
            _entry("corpus_test", "beta", 2),
            _entry("corpus_test", "beta2", 3),
        ]
    )

    visible_before = store.alpha_counts("corpus_test", include_deleted=False)
    assert visible_before["A"] == 1
    assert visible_before["B"] == 2

    rows_b = store.list_entries("corpus_test", alpha_bucket="B", include_deleted=False)
    deleted = soft_delete_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[str(rows_b[0]["entry_id"])],
        reason="test",
    )
    assert deleted == 1

    visible_after = store.alpha_counts("corpus_test", include_deleted=False)
    all_after = store.alpha_counts("corpus_test", include_deleted=True)

    assert visible_after["B"] == 1
    assert all_after["B"] == 2
