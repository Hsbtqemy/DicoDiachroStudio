"""Dialog to edit the list of extra (configurable) fields per corpus."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

if TYPE_CHECKING:
    from dicodiachro.core.storage.sqlite import SQLiteStore


class FieldSchemaDialog(QDialog):
    """Edit corpus field schema: add/remove/reorder extra fields shown in Entries tab."""

    def __init__(
        self,
        store: SQLiteStore,
        corpus_id: str,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.store = store
        self.corpus_id = corpus_id
        self.setWindowTitle("Champs personnalisés — " + (corpus_id or "?"))

        self.list_widget = QListWidget()
        self.list_widget.setMinimumWidth(320)
        self._reload_list()

        add_btn = QPushButton("Ajouter…")
        add_btn.clicked.connect(self._add_field)
        remove_btn = QPushButton("Retirer")
        remove_btn.clicked.connect(self._remove_selected)
        up_btn = QPushButton("Monter")
        up_btn.clicked.connect(self._move_up)
        down_btn = QPushButton("Descendre")
        down_btn.clicked.connect(self._move_down)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(remove_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(up_btn)
        btn_layout.addWidget(down_btn)

        info = QLabel(
            "Ces champs apparaîtront comme colonnes supplémentaires dans l'onglet Entrées. "
            "Les valeurs sont stockées par entrée (extra_json)."
        )
        info.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("Enregistrer")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("Annuler")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(info)
        layout.addWidget(self.list_widget)
        layout.addLayout(btn_layout)
        layout.addWidget(buttons)

    def _reload_list(self) -> None:
        self.list_widget.clear()
        self._schema = self.store.get_field_schema(self.corpus_id)
        for item in self._schema:
            label = item.get("label") or item.get("field_id") or "?"
            list_item = QListWidgetItem(f"{item['field_id']} — {label}")
            list_item.setData(Qt.ItemDataRole.UserRole, item)
            self.list_widget.addItem(list_item)

    def _add_field(self) -> None:
        field_id, ok = QInputDialog.getText(
            self,
            "Nouveau champ",
            "Identifiant du champ (ex: etymology, gender, number):",
            text="",
        )
        if not ok or not field_id.strip():
            return
        field_id = field_id.strip().lower().replace(" ", "_")
        label, ok = QInputDialog.getText(
            self,
            "Nouveau champ",
            "Libellé affiché:",
            text=field_id.replace("_", " ").title(),
        )
        if not ok:
            return
        label = label.strip() or field_id
        self._schema.append({
            "field_id": field_id,
            "label": label,
            "field_type": "text",
            "sort_order": len(self._schema),
            "optional": True,
        })
        list_item = QListWidgetItem(f"{field_id} — {label}")
        list_item.setData(Qt.ItemDataRole.UserRole, self._schema[-1])
        self.list_widget.addItem(list_item)

    def _remove_selected(self) -> None:
        row = self.list_widget.currentRow()
        if row < 0:
            return
        self.list_widget.takeItem(row)
        self._sync_schema_from_list()

    def _move_up(self) -> None:
        row = self.list_widget.currentRow()
        if row <= 0:
            return
        item = self.list_widget.takeItem(row)
        self.list_widget.insertItem(row - 1, item)
        self.list_widget.setCurrentRow(row - 1)
        self._sync_schema_from_list()

    def _move_down(self) -> None:
        row = self.list_widget.currentRow()
        if row < 0 or row >= self.list_widget.count() - 1:
            return
        item = self.list_widget.takeItem(row)
        self.list_widget.insertItem(row + 1, item)
        self.list_widget.setCurrentRow(row + 1)
        self._sync_schema_from_list()

    def _sync_schema_from_list(self) -> None:
        self._schema = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i).data(Qt.ItemDataRole.UserRole)
            if item:
                item["sort_order"] = i
                self._schema.append(item)

    def _save(self) -> None:
        self._sync_schema_from_list()
        self.store.set_field_schema(self.corpus_id, self._schema)
        QMessageBox.information(self, "Champs personnalisés", "Schéma enregistré.")
        self.accept()
