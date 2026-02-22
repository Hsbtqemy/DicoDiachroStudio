from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.conventions_tab import ConventionsTab


def test_conventions_tab_smoke(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
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
                source_path="sample.txt",
                line_no=1,
                raw_line="1 jut, v",
            )
        ]
    )

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = ConventionsTab(state)
    tab.show()
    app.processEvents()

    tab.refresh()
    app.processEvents()
    assert tab.profile_list.count() >= 1

    tab.close()
    app.processEvents()
