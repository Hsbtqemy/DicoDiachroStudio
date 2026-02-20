from __future__ import annotations

import json

from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.pipeline import apply_profile_to_entries
from dicodiachro.core.profiles import apply_profile, load_profile
from dicodiachro.core.storage.sqlite import project_paths

from ...services.state import AppState


class ProfilesTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.list_widget = QListWidget()
        self.preview = QTextEdit()
        self.preview.setReadOnly(True)
        self.sample_n = QSpinBox()
        self.sample_n.setMinimum(1)
        self.sample_n.setMaximum(200)
        self.sample_n.setValue(10)

        refresh_btn = QPushButton("Refresh")
        preview_btn = QPushButton("Preview on sample")
        apply_btn = QPushButton("Apply profile")

        refresh_btn.clicked.connect(self.refresh)
        preview_btn.clicked.connect(self.preview_profile)
        apply_btn.clicked.connect(self.apply_profile)

        row = QHBoxLayout()
        row.addWidget(QLabel("Sample size"))
        row.addWidget(self.sample_n)
        row.addStretch(1)
        row.addWidget(refresh_btn)
        row.addWidget(preview_btn)
        row.addWidget(apply_btn)

        layout = QVBoxLayout()
        layout.addLayout(row)
        layout.addWidget(self.list_widget)
        layout.addWidget(self.preview)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)

    def refresh(self) -> None:
        self.list_widget.clear()
        if not self.state.project_dir:
            return
        for profile in sorted(project_paths(self.state.project_dir).rules_dir.glob("*.yml")):
            self.list_widget.addItem(profile.stem)

    def preview_profile(self) -> None:
        if not self.state.store or not self.state.active_dict_id or not self.state.project_dir:
            return
        item = self.list_widget.currentItem()
        if not item:
            return
        profile_name = item.text()
        profile_path = project_paths(self.state.project_dir).rules_dir / f"{profile_name}.yml"
        if not profile_path.exists():
            return

        profile = load_profile(profile_path)
        rows = self.state.store.list_entries(self.state.active_dict_id, limit=self.sample_n.value())
        payload = []
        for row in rows:
            raw = row["pron_raw"] or row["headword_raw"]
            applied = apply_profile(raw, profile)
            payload.append(
                {
                    "raw": raw,
                    "display": applied.form_display,
                    "norm": applied.form_norm,
                    "features": applied.features,
                }
            )
        self.preview.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))

    def apply_profile(self) -> None:
        if not self.state.active_dict_id or not self.state.project_dir:
            return
        item = self.list_widget.currentItem()
        if not item:
            return
        profile_name = item.text()
        summary = apply_profile_to_entries(
            self.state.project_dir, self.state.active_dict_id, profile_name
        )
        self.state.active_profile = profile_name
        self.state.notify_data_changed()
        QMessageBox.information(
            self, "Profile applied", json.dumps(summary, ensure_ascii=False, indent=2)
        )
