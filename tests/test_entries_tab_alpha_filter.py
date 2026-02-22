from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.entries_tab import EntriesTab


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


def test_entries_tab_alpha_filter_changes_rows(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    store.ensure_dictionary("corpus_test", label="Corpus test")
    store.set_active_dict_id("corpus_test")
    store.insert_entries([_entry("corpus_test", "alpha", 1), _entry("corpus_test", "beta", 2)])

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.show()
    tab.refresh()
    app.processEvents()

    assert tab.model.rowCount() == 2

    button_b = tab.alpha_bar.button_for_bucket("B")
    assert button_b is not None
    button_b.click()
    app.processEvents()

    assert tab.model.rowCount() == 1
    assert tab.model.item(0, 2).text() == "beta"

    tab.close()
    app.processEvents()
