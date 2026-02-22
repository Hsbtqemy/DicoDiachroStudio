from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths


class AppState(QObject):
    project_changed = Signal()
    data_changed = Signal()
    dictionary_changed = Signal(str)
    compare_requested = Signal(object)
    conventions_requested = Signal()
    export_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self.project_dir: Path | None = None
        self.store: SQLiteStore | None = None
        self.active_dict_id: str | None = None
        self.active_profile: str = "reading_v1"
        self.active_parser: str = "auto"

    @property
    def db_path(self) -> Path | None:
        if not self.project_dir:
            return None
        return project_paths(self.project_dir).db_path

    def open_project(self, project_dir: Path) -> None:
        self.project_dir = project_dir.resolve()
        init_project(self.project_dir)
        self.store = SQLiteStore(project_paths(self.project_dir).db_path)
        self.refresh_active_dict()
        self.project_changed.emit()
        self.data_changed.emit()

    def close_project(self) -> None:
        self.project_dir = None
        self.store = None
        self.active_dict_id = None
        self.project_changed.emit()
        self.data_changed.emit()

    def list_dictionaries(self):
        if not self.store:
            return []
        return self.store.list_dictionaries()

    def refresh_active_dict(self) -> str | None:
        if not self.store:
            self.active_dict_id = None
            return None

        saved = self.store.get_active_dict_id()
        ids = [row["dict_id"] for row in self.store.list_dictionaries()]
        if saved and saved in ids:
            self.active_dict_id = saved
            return saved

        self.active_dict_id = None
        return None

    def set_active_dict(self, dict_id: str) -> None:
        cleaned = dict_id.strip()
        if not cleaned:
            return
        self.active_dict_id = cleaned
        if self.store:
            self.store.set_active_dict_id(cleaned)
        self.dictionary_changed.emit(cleaned)
        self.data_changed.emit()

    def ensure_active_corpus(self, suggested_name: str = "Corpus") -> str:
        if not self.store:
            raise RuntimeError("Project is not opened.")
        dict_id = self.store.ensure_active_dict(suggested_name=suggested_name)
        self.active_dict_id = dict_id
        self.dictionary_changed.emit(dict_id)
        self.data_changed.emit()
        return dict_id

    def notify_data_changed(self) -> None:
        self.data_changed.emit()

    def request_compare(self, corpus_ids: list[str] | None = None) -> None:
        self.compare_requested.emit(list(corpus_ids or []))

    def request_conventions(self) -> None:
        self.conventions_requested.emit()

    def request_export(self) -> None:
        self.export_requested.emit()
