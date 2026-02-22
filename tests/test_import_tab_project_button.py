from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.import_tab import ImportTab


def test_import_tab_project_button_actions_call_main_window_handlers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app = QApplication.instance() or QApplication([])

    state = AppState()
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    state.open_project(project_dir)

    tab = ImportTab(state)
    tab.show()
    app.processEvents()

    calls: list[str] = []

    def _fake(action_name: str) -> bool:
        calls.append(action_name)
        return True

    monkeypatch.setattr(tab, "_invoke_main_window_action", _fake)

    tab.project_open_action.trigger()
    tab.project_new_action.trigger()
    app.processEvents()

    assert calls == ["open_project_dialog", "new_project_dialog"]

    tab.close()
    app.processEvents()
