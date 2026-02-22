from __future__ import annotations

from pathlib import Path
from typing import Any

from PySide6.QtWidgets import QApplication

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.compare_tab import CompareTab


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


def test_compare_tab_smoke(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("A")
    store.ensure_dictionary("B")
    store.insert_entries([_entry("A", "alpha", 1), _entry("A", "beta", 2)])
    store.insert_entries([_entry("B", "alpha", 1), _entry("B", "gamma", 2)])

    app = QApplication.instance() or QApplication([])

    state = AppState()
    state.open_project(project_dir)

    tab = CompareTab(state)
    tab.show()
    app.processEvents()

    def _coverage(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "corpus_ids": ["A", "B"],
            "rows": [{"headword_key": "alpha", "presence": {"A": True, "B": True}}],
            "counts": {"union": 1, "common_all": 1, "unique_a": 0, "unique_b": 0},
        }

    def _alignment(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "rows": [
                {
                    "headword_key": "alpha",
                    "headword_a": "alpha",
                    "headword_b": "alpha",
                    "headword_norm_a": "alpha",
                    "headword_norm_b": "alpha",
                    "entry_id_a": "e1",
                    "entry_id_b": "e2",
                    "status_a": "auto",
                    "status_b": "auto",
                    "score": 100.0,
                    "method": "exact",
                    "reason": "",
                }
            ],
            "counts": {"matched_exact": 1, "matched_fuzzy": 0, "unmatched": 0},
        }

    def _diff(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "rows": [
                {
                    "headword_key": "alpha",
                    "pron_render_a": "alpha",
                    "pron_render_b": "alpha",
                    "pron_norm_a": "alpha",
                    "pron_norm_b": "alpha",
                    "features_a": {},
                    "features_b": {},
                    "delta": {"syll_count_diff": 0, "stress_schema_diff": False},
                }
            ],
            "counts": {"total_pairs": 1, "displayed": 1},
        }

    monkeypatch.setattr("dicodiachro_studio.ui.tabs.compare_tab.preview_coverage", _coverage)
    monkeypatch.setattr("dicodiachro_studio.ui.tabs.compare_tab.preview_alignment", _alignment)
    monkeypatch.setattr("dicodiachro_studio.ui.tabs.compare_tab.preview_diff", _diff)

    tab.refresh()
    app.processEvents()
    tab.preselect_corpora(["A", "B"])
    tab.preview_run()
    app.processEvents()

    assert tab.coverage_table.rowCount() >= 1

    tab.close()
    app.processEvents()
