from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from dicodiachro.core.overrides import create_entry, soft_delete_entries
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.entries_tab import EntriesTab


def test_entries_tab_show_deleted_toggle_changes_rows(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")
    store.set_active_dict_id("corpus_test")

    keep_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="alpha")
    drop_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="beta")
    soft_delete_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[drop_id],
        reason="test gui",
    )

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.show()
    app.processEvents()

    tab.refresh()
    app.processEvents()
    visible_ids = {
        str(tab.model.item(row_idx, 0).data(Qt.ItemDataRole.UserRole))
        for row_idx in range(tab.model.rowCount())
    }
    assert keep_id in visible_ids
    assert drop_id not in visible_ids
    assert tab.model.rowCount() == 1

    tab.show_deleted_check.setChecked(True)
    app.processEvents()
    all_ids = {
        str(tab.model.item(row_idx, 0).data(Qt.ItemDataRole.UserRole))
        for row_idx in range(tab.model.rowCount())
    }
    assert keep_id in all_ids
    assert drop_id in all_ids
    assert tab.model.rowCount() == 2

    tab.close()
    app.processEvents()
