from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QMessageBox,
    QVBoxLayout,
)


class AddEntryDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Ajouter une entrée")

        self.headword_edit = QLineEdit()
        self.headword_edit.setPlaceholderText("Entrée diplomatique")

        self.pron_edit = QLineEdit()
        self.pron_edit.setPlaceholderText("Prononciation (optionnelle)")

        self.entry_is_pron_check = QCheckBox("Entrée = prononciation")
        self.entry_is_pron_check.toggled.connect(self._on_entry_is_pron_toggled)
        self.headword_edit.textChanged.connect(self._sync_pron_with_headword)

        self.definition_edit = QLineEdit()
        self.definition_edit.setPlaceholderText("Définition (optionnelle)")

        self.note_edit = QLineEdit()
        self.note_edit.setPlaceholderText("Note d'audit (optionnelle)")

        form = QFormLayout()
        form.addRow("Headword", self.headword_edit)
        form.addRow("Prononciation", self.pron_edit)
        form.addRow("", self.entry_is_pron_check)
        form.addRow("Définition", self.definition_edit)
        form.addRow("Note", self.note_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("Ajouter")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("Annuler")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)

    def _on_entry_is_pron_toggled(self, checked: bool) -> None:
        self.pron_edit.setEnabled(not checked)
        self._sync_pron_with_headword()

    def _sync_pron_with_headword(self) -> None:
        if self.entry_is_pron_check.isChecked():
            self.pron_edit.setText(self.headword_edit.text().strip())

    def accept(self) -> None:
        if not self.headword:
            QMessageBox.warning(self, "Ajouter une entrée", "Le champ Headword est obligatoire.")
            return
        super().accept()

    @property
    def headword(self) -> str:
        return self.headword_edit.text().strip()

    @property
    def pron(self) -> str | None:
        value = self.pron_edit.text().strip()
        return value or None

    @property
    def definition(self) -> str | None:
        value = self.definition_edit.text().strip()
        return value or None

    @property
    def note(self) -> str | None:
        value = self.note_edit.text().strip()
        return value or None

    @property
    def entry_is_pron(self) -> bool:
        return self.entry_is_pron_check.isChecked()
