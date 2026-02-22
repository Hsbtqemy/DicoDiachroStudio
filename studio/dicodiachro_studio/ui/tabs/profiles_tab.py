from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.pipeline import (
    PipelineError,
    apply_profile_to_entries,
    preview_profile_entries,
)
from dicodiachro.core.profiles import ProfileValidationError, load_profile, profile_sha256_from_path
from dicodiachro.core.storage.sqlite import project_paths

from ...services.jobs import JobThread
from ...services.state import AppState


class ProfilesTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.current_job: JobThread | None = None

        self.list_widget = QListWidget()
        self.list_widget.itemSelectionChanged.connect(self.show_profile_meta)

        self.meta = QTextEdit()
        self.meta.setReadOnly(True)

        self.preview_table = QTableWidget(0, 5)
        self.preview_table.setHorizontalHeaderLabels(
            ["raw", "display", "norm", "features", "issues"]
        )
        self.preview_table.horizontalHeader().setStretchLastSection(True)

        self.history = QTextEdit()
        self.history.setReadOnly(True)

        self.sample_n = QSpinBox()
        self.sample_n.setMinimum(1)
        self.sample_n.setMaximum(1000)
        self.sample_n.setValue(20)

        self.diff_only = QCheckBox("Diff view")

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        refresh_btn = QPushButton("Refresh")
        preview_btn = QPushButton("Preview")
        apply_btn = QPushButton("Apply to corpus")

        refresh_btn.clicked.connect(self.refresh)
        preview_btn.clicked.connect(self.preview_profile)
        apply_btn.clicked.connect(self.apply_profile)

        row = QHBoxLayout()
        row.addWidget(QLabel("Sample size"))
        row.addWidget(self.sample_n)
        row.addWidget(self.diff_only)
        row.addStretch(1)
        row.addWidget(refresh_btn)
        row.addWidget(preview_btn)
        row.addWidget(apply_btn)

        layout = QVBoxLayout()
        layout.addLayout(row)
        layout.addWidget(QLabel("Available profiles"))
        layout.addWidget(self.list_widget)
        layout.addWidget(QLabel("Profile metadata"))
        layout.addWidget(self.meta)
        layout.addWidget(self.progress)
        layout.addWidget(QLabel("Preview"))
        layout.addWidget(self.preview_table)
        layout.addWidget(QLabel("Application history"))
        layout.addWidget(self.history)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh_history)
        self.state.dictionary_changed.connect(lambda _: self.refresh_history())

    def _selected_profile_path(self) -> Path | None:
        item = self.list_widget.currentItem()
        if not item:
            return None
        path = item.data(Qt.ItemDataRole.UserRole)
        if not path:
            return None
        return Path(str(path))

    def _profile_files(self) -> list[Path]:
        if not self.state.project_dir:
            return []
        rules_dir = project_paths(self.state.project_dir).rules_dir

        paths: set[Path] = set(rules_dir.glob("*.yml")) | set(rules_dir.glob("*.yaml"))
        if self.state.active_dict_id:
            dict_dir = rules_dir / self.state.active_dict_id
            if dict_dir.exists():
                paths |= set(dict_dir.glob("*.yml")) | set(dict_dir.glob("*.yaml"))

        return sorted(path.resolve() for path in paths)

    def refresh(self) -> None:
        self.list_widget.clear()
        for profile_path in self._profile_files():
            item = QListWidgetItem(profile_path.stem)
            item.setData(Qt.ItemDataRole.UserRole, str(profile_path))
            self.list_widget.addItem(item)

        if self.list_widget.count() > 0:
            self.list_widget.setCurrentRow(0)
        self.refresh_history()

    def show_profile_meta(self) -> None:
        profile_path = self._selected_profile_path()
        if not profile_path:
            self.meta.clear()
            return

        try:
            profile = load_profile(profile_path)
            payload = {
                "profile_id": profile.profile_id,
                "version": profile.version,
                "sha256": profile_sha256_from_path(profile_path),
                "path": str(profile_path),
                "validation_warnings": profile.validation_warnings,
            }
            self.meta.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))
        except ProfileValidationError as exc:
            self.meta.setPlainText(
                json.dumps(
                    {
                        "profile_path": str(profile_path),
                        "errors": exc.errors,
                        "warnings": exc.warnings,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )

    def preview_profile(self) -> None:
        if not self.state.project_dir or not self.state.active_dict_id:
            return

        profile_path = self._selected_profile_path()
        if not profile_path:
            return

        try:
            preview = preview_profile_entries(
                project_dir=self.state.project_dir,
                dict_id=self.state.active_dict_id,
                profile_name=str(profile_path),
                limit=self.sample_n.value(),
            )
        except (PipelineError, ProfileValidationError) as exc:
            QMessageBox.warning(self, "Profile preview error", str(exc))
            return

        self.preview_table.setRowCount(0)
        diff_only = self.diff_only.isChecked()

        for row in preview["rows"]:
            raw = row["raw"]
            display = row["display"]
            norm = row["norm"]
            if diff_only and display == raw and norm == display:
                continue

            table_row = self.preview_table.rowCount()
            self.preview_table.insertRow(table_row)

            raw_item = QTableWidgetItem(raw)
            display_item = QTableWidgetItem(display)
            norm_item = QTableWidgetItem(norm)
            features_item = QTableWidgetItem(
                json.dumps(row["features"], ensure_ascii=False, sort_keys=True)
            )
            issues_item = QTableWidgetItem(", ".join(row.get("issue_codes", [])))

            if display != raw:
                display_item.setBackground(QColor("#fff6bf"))
            if norm != display:
                norm_item.setBackground(QColor("#d9f0ff"))

            self.preview_table.setItem(table_row, 0, raw_item)
            self.preview_table.setItem(table_row, 1, display_item)
            self.preview_table.setItem(table_row, 2, norm_item)
            self.preview_table.setItem(table_row, 3, features_item)
            self.preview_table.setItem(table_row, 4, issues_item)

    def _on_apply_finished(self, summary: dict[str, object]) -> None:
        self.progress.hide()
        if isinstance(summary, dict) and summary.get("profile"):
            self.state.active_profile = str(summary["profile"])
        self.state.notify_data_changed()
        self.refresh_history()
        QMessageBox.information(
            self, "Profile applied", json.dumps(summary, ensure_ascii=False, indent=2)
        )

    def _on_apply_failed(self, trace: str) -> None:
        self.progress.hide()
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        friendly = lines[-1] if lines else "Unknown profile error"
        QMessageBox.critical(self, "Profile apply error", friendly)

    def apply_profile(self) -> None:
        if not self.state.project_dir or not self.state.active_dict_id:
            return

        profile_path = self._selected_profile_path()
        if not profile_path:
            return

        self.progress.show()
        self.current_job = JobThread(
            apply_profile_to_entries,
            self.state.project_dir,
            self.state.active_dict_id,
            str(profile_path),
        )
        self.current_job.signals.finished.connect(self._on_apply_finished)
        self.current_job.signals.failed.connect(self._on_apply_failed)
        self.current_job.start()

    def refresh_history(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            self.history.clear()
            return

        rows = self.state.store.list_profile_applications(self.state.active_dict_id, limit=30)
        payload = [dict(row) for row in rows]
        self.history.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))
