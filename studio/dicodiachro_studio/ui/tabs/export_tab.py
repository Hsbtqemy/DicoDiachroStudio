from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.exporters.csv_jsonl import export_entries_csv, export_entries_jsonl
from dicodiachro.core.exporters.excel_xlsx import export_comparison_xlsx
from dicodiachro.core.exporters.word_docx import export_comparison_docx
from dicodiachro.core.storage.sqlite import project_paths

from ...services.state import AppState


class ExportTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.dicts_edit = QLineEdit()
        self.dicts_edit.setPlaceholderText("dict1,dict2,... for comparison exports")

        csv_btn = QPushButton("Export entries CSV")
        jsonl_btn = QPushButton("Export entries JSONL")
        xlsx_btn = QPushButton("Export comparison XLSX")
        docx_btn = QPushButton("Export comparison DOCX")
        session_btn = QPushButton("Export session JSON")

        csv_btn.clicked.connect(self.export_csv)
        jsonl_btn.clicked.connect(self.export_jsonl)
        xlsx_btn.clicked.connect(self.export_xlsx)
        docx_btn.clicked.connect(self.export_docx)
        session_btn.clicked.connect(self.export_session)

        actions = QHBoxLayout()
        actions.addWidget(csv_btn)
        actions.addWidget(jsonl_btn)
        actions.addWidget(xlsx_btn)
        actions.addWidget(docx_btn)
        actions.addWidget(session_btn)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Comparison dictionaries"))
        layout.addWidget(self.dicts_edit)
        layout.addLayout(actions)
        layout.addStretch(1)
        self.setLayout(layout)

    def _require(self) -> bool:
        if not self.state.store or not self.state.project_dir:
            QMessageBox.warning(self, "Project required", "Open a project first.")
            return False
        if not self.state.active_dict_id:
            QMessageBox.warning(self, "Dictionary required", "Select an active dictionary.")
            return False
        return True

    def _dict_ids(self) -> list[str]:
        raw = self.dicts_edit.text().strip()
        if not raw:
            return [row["dict_id"] for row in self.state.list_dictionaries()]
        return [part.strip() for part in raw.split(",") if part.strip()]

    def export_csv(self) -> None:
        if not self._require():
            return
        out, _ = QFileDialog.getSaveFileName(self, "Export CSV", "entries.csv", "CSV (*.csv)")
        if not out:
            return
        path = export_entries_csv(
            self.state.store,
            self.state.active_dict_id,
            project_paths(self.state.project_dir).derived_dir / Path(out).name,
        )
        QMessageBox.information(self, "Exported", str(path))

    def export_jsonl(self) -> None:
        if not self._require():
            return
        out, _ = QFileDialog.getSaveFileName(
            self, "Export JSONL", "entries.jsonl", "JSONL (*.jsonl)"
        )
        if not out:
            return
        path = export_entries_jsonl(
            self.state.store,
            self.state.active_dict_id,
            project_paths(self.state.project_dir).derived_dir / Path(out).name,
        )
        QMessageBox.information(self, "Exported", str(path))

    def export_xlsx(self) -> None:
        if not self._require():
            return
        out, _ = QFileDialog.getSaveFileName(
            self, "Export XLSX", "comparison.xlsx", "XLSX (*.xlsx)"
        )
        if not out:
            return
        dict_ids = self._dict_ids()
        path = export_comparison_xlsx(
            self.state.store,
            dict_ids,
            project_paths(self.state.project_dir).derived_dir / Path(out).name,
        )
        QMessageBox.information(self, "Exported", str(path))

    def export_docx(self) -> None:
        if not self._require():
            return
        out, _ = QFileDialog.getSaveFileName(
            self, "Export DOCX", "comparison.docx", "DOCX (*.docx)"
        )
        if not out:
            return
        dict_ids = self._dict_ids()
        path = export_comparison_docx(
            self.state.store,
            dict_ids,
            project_paths(self.state.project_dir).derived_dir / Path(out).name,
        )
        QMessageBox.information(self, "Exported", str(path))

    def export_session(self) -> None:
        if not self._require():
            return
        out, _ = QFileDialog.getSaveFileName(
            self, "Export session JSON", "session.json", "JSON (*.json)"
        )
        if not out:
            return
        dict_ids = self._dict_ids()
        session = {
            "active_dict": self.state.active_dict_id,
            "profile": self.state.active_profile,
            "dict_ids": dict_ids,
        }
        session_id = self.state.store.save_comparison_session(session)
        target = project_paths(self.state.project_dir).derived_dir / Path(out).name
        target.write_text(
            json.dumps({"session_id": session_id, **session}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        QMessageBox.information(self, "Exported", str(target))
