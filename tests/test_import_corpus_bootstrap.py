from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.import_tab import ImportTab


def test_import_tab_creates_corpus_via_prompt_mock(tmp_path: Path, monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])

    state = AppState()
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    state.open_project(project_dir)
    assert state.active_dict_id is None

    tab = ImportTab(state)
    seen: dict[str, str] = {}

    def _fake_prompt(name: str):
        seen["suggested"] = name
        return ("The Spelling Dictionary (1737)", True)

    monkeypatch.setattr(tab, "_prompt_create_corpus", _fake_prompt)

    ok = tab._ensure_corpus_or_prompt("The Spelling Dictionary (1737)")
    app.processEvents()

    assert ok is True
    assert seen["suggested"] == "The Spelling Dictionary (1737)"
    assert state.active_dict_id == "the_spelling_dictionary_1737"
    assert state.store is not None
    assert state.store.get_active_dict_id() == "the_spelling_dictionary_1737"


def test_import_tab_cancel_corpus_creation_aborts(tmp_path: Path, monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])

    state = AppState()
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    state.open_project(project_dir)
    assert state.active_dict_id is None

    tab = ImportTab(state)
    monkeypatch.setattr(tab, "_prompt_create_corpus", lambda _: None)

    ok = tab._ensure_corpus_or_prompt("Corpus test")
    app.processEvents()

    assert ok is False
    assert state.active_dict_id is None
