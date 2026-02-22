from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QPlainTextEdit,
    QVBoxLayout,
)


class SaveConventionDialog(QDialog):
    def __init__(self, default_name: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Enregistrer une convention")

        self.name_edit = QLineEdit(default_name)
        self.name_edit.setPlaceholderText("Nom de la convention")

        self.description_edit = QPlainTextEdit()
        self.description_edit.setPlaceholderText("Description (optionnelle)")

        form = QFormLayout()
        form.addRow("Nom", self.name_edit)
        form.addRow("Description", self.description_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)

    @property
    def convention_name(self) -> str:
        return self.name_edit.text().strip()

    @property
    def description(self) -> str:
        return self.description_edit.toPlainText().strip()
