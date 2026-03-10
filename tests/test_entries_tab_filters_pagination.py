from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import QApplication

from dicodiachro.core.models import Issue, ParsedEntry
from dicodiachro.core.overrides import set_entry_status
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


def _id_by_headword(store: SQLiteStore, corpus_id: str) -> dict[str, str]:
    return {
        str(row["headword_raw"]): str(row["entry_id"])
        for row in store.entries_for_dict(corpus_id, include_deleted=True)
    }


def test_entries_tab_status_filter_applies_before_pagination(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    corpus_id = "corpus_test"
    store.ensure_dictionary(corpus_id, label="Corpus test")
    store.set_active_dict_id(corpus_id)
    store.insert_entries(
        [
            _entry(corpus_id, "alpha", 1),  # auto
            _entry(corpus_id, "beta", 2),  # reviewed
            _entry(corpus_id, "gamma", 3),  # reviewed
        ]
    )

    ids = _id_by_headword(store, corpus_id)
    set_entry_status(store=store, corpus_id=corpus_id, entry_id=ids["beta"], status="reviewed")
    set_entry_status(store=store, corpus_id=corpus_id, entry_id=ids["gamma"], status="reviewed")

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.PAGE_SIZE = 1
    tab.show()
    app.processEvents()

    tab.filter_auto_action.setChecked(False)
    tab.filter_reviewed_action.setChecked(True)
    tab.filter_validated_action.setChecked(False)
    app.processEvents()

    assert tab.model.rowCount() == 1
    assert tab.model.item(0, 2).text() == "beta"

    tab.next_page()
    app.processEvents()
    assert tab.model.rowCount() == 1
    assert tab.model.item(0, 2).text() == "gamma"

    tab.close()
    app.processEvents()


def test_entries_tab_flags_filter_applies_before_pagination(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    corpus_id = "corpus_test"
    store.ensure_dictionary(corpus_id, label="Corpus test")
    store.set_active_dict_id(corpus_id)
    store.insert_entries(
        [
            _entry(corpus_id, "alpha", 1),  # no issue
            _entry(corpus_id, "beta", 2),  # has issue
        ]
    )
    store.insert_issues(
        [
            Issue(
                dict_id=corpus_id,
                source_path=f"{corpus_id}.txt",
                line_no=2,
                kind="warning",
                code="TEST_FLAG",
                raw="beta",
            )
        ]
    )

    app = QApplication.instance() or QApplication([])
    state = AppState()
    state.open_project(project_dir)

    tab = EntriesTab(state)
    tab.PAGE_SIZE = 1
    tab.show()
    app.processEvents()

    tab.filter_flags_action.setChecked(True)
    app.processEvents()

    assert tab.model.rowCount() == 1
    assert tab.model.item(0, 2).text() == "beta"

    tab.close()
    app.processEvents()
