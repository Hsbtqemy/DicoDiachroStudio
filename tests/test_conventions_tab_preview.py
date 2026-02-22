from __future__ import annotations

import time
from pathlib import Path

from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro_studio.services.state import AppState
from dicodiachro_studio.ui.tabs.conventions_tab import ConventionsTab


def _wait_until(predicate, app: QApplication, timeout_ms: int = 15000) -> bool:
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        app.processEvents()
        if predicate():
            return True
        QTest.qWait(20)
    return False


def _seed_project(tmp_path: Path) -> Path:
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
            ),
            ParsedEntry(
                dict_id="corpus_test",
                section="JU",
                syllables=1,
                headword_raw="júvenal",
                pos_raw="v",
                pron_raw="júvenal",
                source_path=str(paths.raw_dir / "imports" / "sample.txt"),
                line_no=2,
                raw_line="1 júvenal, v",
            ),
            ParsedEntry(
                dict_id="corpus_test",
                section="KI",
                syllables=1,
                headword_raw="áʹlendar",
                pos_raw="v",
                pron_raw="áʹlendar",
                source_path=str(paths.raw_dir / "imports" / "sample.txt"),
                line_no=3,
                raw_line="1 áʹlendar, v",
            ),
        ]
    )
    return project_dir


def test_conventions_preview_sample_stable_without_resample(tmp_path: Path) -> None:
    app = QApplication.instance() or QApplication([])
    project_dir = _seed_project(tmp_path)

    state = AppState()
    state.open_project(project_dir)

    tab = ConventionsTab(state)
    tab.show()
    app.processEvents()
    tab.refresh()
    app.processEvents()

    tab.preview_selected()
    assert _wait_until(lambda: bool(tab.preview_payload), app)
    assert _wait_until(lambda: tab.preview_job is None or not tab.preview_job.isRunning(), app)

    sample_a = list(tab._preview_sample_ids)
    assert sample_a

    tab.preview_selected()
    assert _wait_until(lambda: tab.preview_job is None or not tab.preview_job.isRunning(), app)

    sample_b = list(tab._preview_sample_ids)
    assert sample_b == sample_a

    tab.close()
    app.processEvents()


def test_conventions_settings_change_triggers_debounced_auto_preview(tmp_path: Path) -> None:
    app = QApplication.instance() or QApplication([])
    project_dir = _seed_project(tmp_path)

    state = AppState()
    state.open_project(project_dir)

    tab = ConventionsTab(state)
    tab.show()
    app.processEvents()
    tab.refresh()
    app.processEvents()

    tab.preview_selected()
    assert _wait_until(lambda: bool(tab.preview_payload), app)
    assert _wait_until(lambda: tab.preview_job is None or not tab.preview_job.isRunning(), app)

    initial_generation = tab._preview_generation
    initial_sample = list(tab._preview_sample_ids)

    tab.lowercase_check.setChecked(not tab.lowercase_check.isChecked())

    assert _wait_until(
        lambda: tab._preview_generation > initial_generation,
        app,
    )
    assert _wait_until(lambda: tab.preview_job is None or not tab.preview_job.isRunning(), app)

    assert tab._preview_sample_ids == initial_sample

    tab.close()
    app.processEvents()
