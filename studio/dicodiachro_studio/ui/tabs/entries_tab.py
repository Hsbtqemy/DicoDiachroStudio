from __future__ import annotations

import json

from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableView,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ...services.state import AppState


class EntriesTab(QWidget):
    PAGE_SIZE = 100

    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.offset = 0
        self.search_text = ""

        self.table = QTableView()
        self.model = QStandardItemModel(0, 7)
        self.model.setHorizontalHeaderLabels(
            [
                "section",
                "syllables",
                "headword_raw",
                "pos_raw",
                "form_display",
                "form_norm",
                "flags",
            ]
        )
        self.table.setModel(self.model)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self.table.clicked.connect(self.on_row_selected)

        self.details = QTextEdit()
        self.details.setReadOnly(True)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search substring...")
        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self.on_search)

        self.page_label = QLabel("page 1")
        prev_btn = QPushButton("Prev")
        next_btn = QPushButton("Next")
        prev_btn.clicked.connect(self.prev_page)
        next_btn.clicked.connect(self.next_page)

        top = QHBoxLayout()
        top.addWidget(self.search)
        top.addWidget(search_btn)
        top.addStretch(1)
        top.addWidget(prev_btn)
        top.addWidget(next_btn)
        top.addWidget(self.page_label)

        split = QSplitter(Qt.Orientation.Horizontal)
        split.addWidget(self.table)
        split.addWidget(self.details)
        split.setSizes([900, 500])

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(split)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.state.dictionary_changed.connect(lambda _: self.reset_and_refresh())

    def reset_and_refresh(self) -> None:
        self.offset = 0
        self.refresh()

    def on_search(self) -> None:
        self.search_text = self.search.text().strip()
        self.offset = 0
        self.refresh()

    def prev_page(self) -> None:
        self.offset = max(0, self.offset - self.PAGE_SIZE)
        self.refresh()

    def next_page(self) -> None:
        self.offset += self.PAGE_SIZE
        self.refresh()

    def refresh(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            self.model.removeRows(0, self.model.rowCount())
            return

        rows = self.state.store.list_entries(
            dict_id=self.state.active_dict_id,
            limit=self.PAGE_SIZE,
            offset=self.offset,
            search=self.search_text or None,
        )

        self.model.removeRows(0, self.model.rowCount())
        for row in rows:
            _, issues = self.state.store.entry_details(row["entry_id"])
            values = [
                row["section"] or "",
                str(row["syllables"]),
                row["headword_raw"],
                row["pos_raw"],
                row["form_display"] or "",
                row["form_norm"] or "",
                str(len(issues)),
            ]
            items = [QStandardItem(v) for v in values]
            for item in items:
                item.setEditable(False)
            items[0].setData(row["entry_id"], Qt.ItemDataRole.UserRole)
            self.model.appendRow(items)

        page = self.offset // self.PAGE_SIZE + 1
        self.page_label.setText(f"page {page}")

    def on_row_selected(self) -> None:
        index = self.table.currentIndex()
        if not index.isValid() or not self.state.store:
            return

        entry_id = self.model.item(index.row(), 0).data(Qt.ItemDataRole.UserRole)
        if not entry_id:
            return

        entry, issues = self.state.store.entry_details(entry_id)
        if not entry:
            return

        details = {
            "entry_id": entry["entry_id"],
            "source_path": entry["source_path"],
            "line_no": entry["line_no"],
            "headword_raw": entry["headword_raw"],
            "pron_raw": entry["pron_raw"],
            "form_display": entry["form_display"],
            "form_norm": entry["form_norm"],
            "features_json": entry["features_json"],
            "issues": [
                {
                    "code": issue["code"],
                    "kind": issue["kind"],
                    "raw": issue["raw"],
                    "details_json": issue["details_json"],
                }
                for issue in issues
            ],
        }
        self.details.setPlainText(json.dumps(details, ensure_ascii=False, indent=2))
