from __future__ import annotations

from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from ...services.state import AppState


class CompareTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.dict_list = QListWidget()
        self.dict_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search lemma_label")

        refresh_btn = QPushButton("Refresh matrix")
        refresh_btn.clicked.connect(self.refresh_matrix)

        self.table = QTableView()
        self.model = QStandardItemModel(0, 0)
        self.table.setModel(self.model)

        top = QHBoxLayout()
        top.addWidget(QLabel("Dictionaries"))
        top.addWidget(self.dict_list, 1)

        controls = QHBoxLayout()
        controls.addWidget(self.search)
        controls.addWidget(refresh_btn)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addLayout(controls)
        layout.addWidget(self.table)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh_dicts)
        self.state.data_changed.connect(self.refresh_dicts)

    def refresh_dicts(self) -> None:
        self.dict_list.clear()
        for row in self.state.list_dictionaries():
            self.dict_list.addItem(row["dict_id"])

    def refresh_matrix(self) -> None:
        if not self.state.store:
            return

        dict_ids = [item.text() for item in self.dict_list.selectedItems()]
        if not dict_ids:
            return

        rows = self.state.store.comparison_rows(dict_ids)
        query = self.search.text().strip().lower()
        if query:
            rows = [r for r in rows if query in r["lemma_label"].lower()]

        self.model = QStandardItemModel(0, 2 + len(dict_ids))
        self.model.setHorizontalHeaderLabels(["lemma_group_id", "lemma_label", *dict_ids])

        for row in rows:
            values = row.get("values", {})
            items = [
                QStandardItem(row["lemma_group_id"]),
                QStandardItem(row["lemma_label"]),
                *[QStandardItem(values.get(d, "ABSENT")) for d in dict_ids],
            ]
            for item in items:
                item.setEditable(False)
            self.model.appendRow(items)

        self.table.setModel(self.model)
