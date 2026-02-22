from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication, QSplitter, QTableView, QToolBar

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.entries_tab import EntriesTab


def test_entries_tab_layout_toolbar_is_inside_splitter_left_panel(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")
    store.set_active_dict_id("corpus_test")
    store.insert_entries(
        [
            ParsedEntry(
                dict_id="corpus_test",
                section="JU",
                syllables=1,
                headword_raw="jut",
                pos_raw="v",
                pron_raw="jut",
                source_path=str(paths.raw_dir / "imports" / "sample.txt"),
                line_no=1,
                raw_line="1 jut, v",
            )
        ]
    )

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.show()
    app.processEvents()

    root_layout = tab.layout()
    assert root_layout is not None
    assert root_layout.count() == 1

    splitter = root_layout.itemAt(0).widget()
    assert isinstance(splitter, QSplitter)
    assert splitter.count() == 2

    left_panel = splitter.widget(0)
    assert left_panel is tab.left_panel
    toolbars = left_panel.findChildren(QToolBar)
    assert len(toolbars) == 3
    assert tab.toolbar_search in toolbars
    assert tab.toolbar_edit_structure in toolbars
    assert tab.toolbar_status_nav in toolbars
    assert left_panel.findChild(QTableView) is tab.table
    assert splitter.widget(1) is tab.details

    tab.reset_layout()
    app.processEvents()
    sizes = splitter.sizes()
    assert len(sizes) == 2
    assert sizes[0] > sizes[1] > 0

    tab.close()
    app.processEvents()
