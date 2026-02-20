from __future__ import annotations

from PySide6.QtWidgets import QHBoxLayout, QLabel, QPushButton, QTextEdit, QVBoxLayout, QWidget

from ...services.state import AppState


class DiagnosticsTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.summary = QLabel("No diagnostics yet")
        self.top_issues = QTextEdit()
        self.top_issues.setReadOnly(True)

        refresh_btn = QPushButton("Refresh diagnostics")
        refresh_btn.clicked.connect(self.refresh)

        layout = QVBoxLayout()
        row = QHBoxLayout()
        row.addWidget(refresh_btn)
        row.addStretch(1)
        layout.addLayout(row)
        layout.addWidget(self.summary)
        layout.addWidget(self.top_issues)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)

    def refresh(self) -> None:
        if not self.state.store:
            self.summary.setText("No project opened")
            self.top_issues.clear()
            return

        dict_id = self.state.active_dict_id
        entries = self.state.store.count_entries(dict_id) if dict_id else 0
        issues = self.state.store.count_issues(dict_id)
        tops = self.state.store.top_issues(dict_id=dict_id, limit=30)

        unparsed = sum(int(row["n"]) for row in tops if row["code"] == "UNPARSED_LINE")
        unknown_symbols = sum(
            int(row["n"]) for row in tops if row["code"] in {"UNKNOWN_SYMBOL", "S_VS_F_CHECK"}
        )

        self.summary.setText(
            f"entries={entries} | issues={issues} | unknown_symbols={unknown_symbols} | unparsed_lines={unparsed}"
        )
        lines = [f"{row['kind']} {row['code']} -> {row['n']}" for row in tops]
        self.top_issues.setPlainText("\n".join(lines))
