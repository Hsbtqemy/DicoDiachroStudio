from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.templates_tab import TemplatesTab


def test_templates_tab_smoke(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    source = paths.raw_dir / "imports" / "sample.txt"
    source.write_text("alpha beta\n", encoding="utf-8")

    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")
    store.set_active_dict_id("corpus_test")

    app = QApplication.instance() or QApplication([])

    state = AppState()
    state.open_project(project_dir)

    tab = TemplatesTab(state)
    tab.show()
    app.processEvents()

    assert tab.template_list.count() >= 4
    tab.refresh()
    app.processEvents()
    assert tab.source_combo.count() >= 1

    tab.close()
    app.processEvents()


def test_templates_tab_diff_toggle_rerenders_preview(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")
    store.set_active_dict_id("corpus_test")

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = TemplatesTab(state)
    tab.show()
    app.processEvents()

    payload = {
        "records_count": 2,
        "entries_count": 2,
        "ignored_count": 0,
        "unrecognized_count": 0,
        "overridden_count": 0,
        "rows": [
            {
                "source": "alpha",
                "headword_raw": "alpha",
                "pron_raw": "",
                "definition_raw": "",
                "status": "OK",
                "reason": "",
                "override_op": "",
            },
            {
                "source": "beta",
                "headword_raw": "betta",
                "pron_raw": "",
                "definition_raw": "",
                "status": "OK",
                "reason": "",
                "override_op": "",
            },
        ],
    }

    tab.preview_payload = payload
    tab._render_preview(payload)
    assert tab.preview_table.rowCount() == 2

    tab.diff_only.setChecked(True)
    app.processEvents()
    assert tab.preview_table.rowCount() == 1

    tab.close()
    app.processEvents()
