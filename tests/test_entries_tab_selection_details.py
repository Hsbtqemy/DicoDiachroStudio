from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from dicodiachro.core.overrides import create_entry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.entries_tab import EntriesTab


def test_entries_tab_details_follow_selection_changes(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")
    store.set_active_dict_id("corpus_test")

    create_entry(store=store, corpus_id="corpus_test", headword_raw="alpha")
    create_entry(store=store, corpus_id="corpus_test", headword_raw="beta")

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.show()
    tab.refresh()
    app.processEvents()

    assert tab.model.rowCount() >= 2
    row0_id = str(tab.model.item(0, 0).data(Qt.ItemDataRole.UserRole))
    row1_id = str(tab.model.item(1, 0).data(Qt.ItemDataRole.UserRole))

    tab.table.selectRow(0)
    app.processEvents()
    details_first = tab.details.toPlainText()

    tab.table.selectRow(1)
    app.processEvents()
    details_second = tab.details.toPlainText()

    assert row0_id in details_first
    assert row1_id in details_second
    assert details_second != details_first

    selected_id = str(
        tab.model.item(tab.table.currentIndex().row(), 0).data(Qt.ItemDataRole.UserRole)
    )
    assert selected_id == row1_id

    tab.close()
    app.processEvents()
