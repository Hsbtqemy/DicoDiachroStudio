from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QItemSelectionModel
from PySide6.QtWidgets import QApplication

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.templates_tab import TemplatesTab


def test_templates_tab_bulk_skip_uses_unique_record_keys(tmp_path: Path, monkeypatch) -> None:
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
        "records_count": 3,
        "entries_count": 3,
        "ignored_count": 0,
        "unrecognized_count": 0,
        "overridden_count": 0,
        "rows": [
            {
                "source": "ligne alpha",
                "headword_raw": "alpha",
                "pron_raw": "",
                "definition_raw": "",
                "source_id": "sourceA",
                "record_key": "rk1",
                "status": "OK",
                "reason": "",
                "source_path": "sample.txt",
                "record_no": 10,
                "issue_code": None,
                "override_op": None,
            },
            {
                "source": "ligne alpha",
                "headword_raw": "beta",
                "pron_raw": "",
                "definition_raw": "",
                "source_id": "sourceA",
                "record_key": "rk1",
                "status": "OK",
                "reason": "",
                "source_path": "sample.txt",
                "record_no": 10,
                "issue_code": None,
                "override_op": None,
            },
            {
                "source": "ligne gamma",
                "headword_raw": "gamma",
                "pron_raw": "",
                "definition_raw": "",
                "source_id": "sourceA",
                "record_key": "rk2",
                "status": "OK",
                "reason": "",
                "source_path": "sample.txt",
                "record_no": 11,
                "issue_code": None,
                "override_op": None,
            },
        ],
    }

    tab.preview_payload = payload
    tab._render_preview(payload)
    app.processEvents()

    selection_model = tab.preview_table.selectionModel()
    assert selection_model is not None
    for row_idx in [0, 1, 2]:
        index = tab.preview_table.model().index(row_idx, 0)
        selection_model.select(
            index,
            QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows,
        )

    selected = tab._selected_preview_payloads()
    refs = tab._selected_record_refs(selected)
    assert len(refs) == 2

    calls: list[tuple[str, str]] = []

    def _fake_upsert(*, source_id: str, record_key: str, **kwargs):
        calls.append((source_id, record_key))
        return 1

    monkeypatch.setattr("dicodiachro_studio.ui.tabs.templates_tab.upsert_override_record", _fake_upsert)
    monkeypatch.setattr(tab, "preview_template", lambda: None)
    monkeypatch.setattr(tab, "refresh_history", lambda: None)

    tab._apply_skip_overrides_for_payloads(selected)

    assert sorted(calls) == [("sourceA", "rk1"), ("sourceA", "rk2")]

    tab.close()
    app.processEvents()
