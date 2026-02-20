from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QHBoxLayout,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.align.match import match_dictionaries

from ...services.state import AppState


class AlignTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.candidates = []

        self.dict_a = QComboBox()
        self.dict_b = QComboBox()
        self.min_score = QDoubleSpinBox()
        self.min_score.setRange(0, 100)
        self.min_score.setValue(85.0)

        run_btn = QPushButton("Run exact+fuzzy")
        validate_btn = QPushButton("Validate selected")
        run_btn.clicked.connect(self.run_match)
        validate_btn.clicked.connect(self.validate_selected)

        top = QHBoxLayout()
        top.addWidget(self.dict_a)
        top.addWidget(self.dict_b)
        top.addWidget(self.min_score)
        top.addWidget(run_btn)
        top.addWidget(validate_btn)

        self.table = QTableView()
        self.model = QStandardItemModel(0, 4)
        self.model.setHorizontalHeaderLabels(["label_a", "label_b", "score", "status"])
        self.table.setModel(self.model)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableView.SelectionMode.SingleSelection)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(self.table)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh_dicts)
        self.state.data_changed.connect(self.refresh_dicts)

    def refresh_dicts(self) -> None:
        dicts = [row["dict_id"] for row in self.state.list_dictionaries()]
        self.dict_a.clear()
        self.dict_b.clear()
        self.dict_a.addItems(dicts)
        self.dict_b.addItems(dicts)
        if len(dicts) > 1:
            self.dict_b.setCurrentIndex(1)

    def run_match(self) -> None:
        if not self.state.store:
            return
        a = self.dict_a.currentText().strip()
        b = self.dict_b.currentText().strip()
        if not a or not b or a == b:
            return

        self.candidates = match_dictionaries(self.state.store, a, b, self.min_score.value())
        self.model.removeRows(0, self.model.rowCount())
        for candidate in self.candidates:
            items = [
                QStandardItem(candidate.label_a),
                QStandardItem(candidate.label_b),
                QStandardItem(f"{candidate.score:.2f}"),
                QStandardItem(candidate.status),
            ]
            for item in items:
                item.setEditable(False)
            items[0].setData(candidate, Qt.ItemDataRole.UserRole)
            self.model.appendRow(items)

    def validate_selected(self) -> None:
        if not self.state.store:
            return
        index = self.table.currentIndex()
        if not index.isValid():
            return

        candidate = self.model.item(index.row(), 0).data(Qt.ItemDataRole.UserRole)
        if candidate is None:
            return

        lemma_label = min((candidate.label_a or "").lower(), (candidate.label_b or "").lower())
        group_id = self.state.store.upsert_lemma_group(lemma_label)
        self.state.store.add_lemma_member(
            group_id, candidate.dict_id_a, candidate.entry_id_a, candidate.score, "validated"
        )
        self.state.store.add_lemma_member(
            group_id, candidate.dict_id_b, candidate.entry_id_b, candidate.score, "validated"
        )
        self.state.notify_data_changed()
        QMessageBox.information(self, "Validated", f"Linked in group {group_id[:12]}...")
