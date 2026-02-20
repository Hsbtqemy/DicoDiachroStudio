from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths


class AppState(QObject):
    project_changed = Signal()
    data_changed = Signal()
    dictionary_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.project_dir: Path | None = None
        self.store: SQLiteStore | None = None
        self.active_dict_id: str | None = None
        self.active_profile: str = "reading_v1"

    @property
    def db_path(self) -> Path | None:
        if not self.project_dir:
            return None
        return project_paths(self.project_dir).db_path

    def open_project(self, project_dir: Path) -> None:
        self.project_dir = project_dir.resolve()
        init_project(self.project_dir)
        self.store = SQLiteStore(project_paths(self.project_dir).db_path)
        dictionaries = self.list_dictionaries()
        if dictionaries and not self.active_dict_id:
            self.active_dict_id = dictionaries[0]["dict_id"]
        self.project_changed.emit()

    def list_dictionaries(self):
        if not self.store:
            return []
        return self.store.list_dictionaries()

    def set_active_dict(self, dict_id: str) -> None:
        self.active_dict_id = dict_id
        self.dictionary_changed.emit(dict_id)
        self.data_changed.emit()

    def notify_data_changed(self) -> None:
        self.data_changed.emit()
