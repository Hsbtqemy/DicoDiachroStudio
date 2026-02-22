from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QVBoxLayout,
)


class CreateCorpusDialog(QDialog):
    def __init__(self, suggested_name: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Créer un corpus")

        self.name_edit = QLineEdit(suggested_name.strip() or "Corpus")
        self.name_edit.setPlaceholderText("Nom du corpus")

        self.use_next_checkbox = QCheckBox("Utiliser ce corpus pour les prochains imports")
        self.use_next_checkbox.setChecked(True)

        form = QFormLayout()
        form.addRow("Nom du corpus", self.name_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Aucun corpus n'existe encore. Créons-en un pour cette source."))
        layout.addLayout(form)
        layout.addWidget(self.use_next_checkbox)
        layout.addWidget(buttons)
        self.setLayout(layout)

    @property
    def corpus_name(self) -> str:
        return self.name_edit.text().strip()

    @property
    def use_for_next_imports(self) -> bool:
        return self.use_next_checkbox.isChecked()
