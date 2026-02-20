from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.storage.sqlite import project_paths

from ...services.state import AppState


class ProjectTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.project_label = QLabel("No project opened")
        self.db_label = QLabel("-")
        self.dict_combo = QComboBox()
        self.profile_combo = QComboBox()

        open_btn = QPushButton("Open Project")
        create_btn = QPushButton("Create Project")
        open_btn.clicked.connect(self.open_project)
        create_btn.clicked.connect(self.create_project)

        self.dict_combo.currentTextChanged.connect(self.on_dict_changed)
        self.profile_combo.currentTextChanged.connect(self.on_profile_changed)

        row = QHBoxLayout()
        row.addWidget(create_btn)
        row.addWidget(open_btn)

        form = QFormLayout()
        form.addRow("Project", self.project_label)
        form.addRow("Database", self.db_label)
        form.addRow("Active dictionary", self.dict_combo)
        form.addRow("Active profile", self.profile_combo)

        layout = QVBoxLayout()
        layout.addLayout(row)
        layout.addLayout(form)
        layout.addStretch(1)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)

    def create_project(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Create project folder")
        if not path:
            return
        self.state.open_project(Path(path))

    def open_project(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Open project folder")
        if not path:
            return
        self.state.open_project(Path(path))

    def on_dict_changed(self, dict_id: str) -> None:
        if dict_id:
            self.state.set_active_dict(dict_id)

    def on_profile_changed(self, profile_name: str) -> None:
        if profile_name:
            self.state.active_profile = profile_name

    def refresh(self) -> None:
        if not self.state.project_dir:
            return

        self.project_label.setText(str(self.state.project_dir))
        self.db_label.setText(str(project_paths(self.state.project_dir).db_path))

        dictionaries = self.state.list_dictionaries()
        self.dict_combo.blockSignals(True)
        self.dict_combo.clear()
        for row in dictionaries:
            self.dict_combo.addItem(row["dict_id"])
        if self.state.active_dict_id:
            idx = self.dict_combo.findText(self.state.active_dict_id)
            if idx >= 0:
                self.dict_combo.setCurrentIndex(idx)
        self.dict_combo.blockSignals(False)

        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        rules_dir = project_paths(self.state.project_dir).rules_dir
        for p in sorted(rules_dir.glob("*.yml")):
            self.profile_combo.addItem(p.stem)
        if self.profile_combo.count() == 0:
            self.profile_combo.addItem("reading_v1")
        idx = self.profile_combo.findText(self.state.active_profile)
        if idx >= 0:
            self.profile_combo.setCurrentIndex(idx)
        self.profile_combo.blockSignals(False)
