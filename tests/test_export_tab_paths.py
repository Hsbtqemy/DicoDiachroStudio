from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.export_tab import ExportTab


def _build_state(project_dir: Path) -> AppState:
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)
    store.ensure_dictionary("A")

    state = AppState()
    state.open_project(project_dir)
    state.set_active_dict("A")
    return state


def test_export_csv_uses_selected_absolute_path(tmp_path: Path, monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    project_dir = tmp_path / "project"
    state = _build_state(project_dir)
    tab = ExportTab(state)

    selected = (tmp_path / "custom" / "entries.csv").resolve()
    captured: dict[str, Path] = {}

    monkeypatch.setattr(
        "dicodiachro_studio.ui.tabs.export_tab.QFileDialog.getSaveFileName",
        lambda *args, **kwargs: (str(selected), "CSV (*.csv)"),
    )

    def _fake_export(*_, out_path: Path) -> Path:
        captured["out_path"] = out_path
        return out_path

    monkeypatch.setattr(
        "dicodiachro_studio.ui.tabs.export_tab.export_entries_csv",
        lambda store, dict_id, out_path: _fake_export(out_path=out_path),
    )
    monkeypatch.setattr(
        "dicodiachro_studio.ui.tabs.export_tab.QMessageBox.information",
        lambda *args, **kwargs: None,
    )

    tab.export_csv()
    app.processEvents()

    assert captured["out_path"] == selected


def test_export_session_writes_to_selected_absolute_path(tmp_path: Path, monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    project_dir = tmp_path / "project"
    state = _build_state(project_dir)
    tab = ExportTab(state)

    selected = (tmp_path / "reports" / "session_custom.json").resolve()

    monkeypatch.setattr(
        "dicodiachro_studio.ui.tabs.export_tab.QFileDialog.getSaveFileName",
        lambda *args, **kwargs: (str(selected), "JSON (*.json)"),
    )
    monkeypatch.setattr(
        "dicodiachro_studio.ui.tabs.export_tab.QMessageBox.information",
        lambda *args, **kwargs: None,
    )

    tab.export_session()
    app.processEvents()

    assert selected.exists()
    payload = json.loads(selected.read_text(encoding="utf-8"))
    assert payload["active_dict"] == "A"
    assert payload["dict_ids"] == ["A"]

    derived_same_name = project_paths(project_dir).derived_dir / selected.name
    assert not derived_same_name.exists()
