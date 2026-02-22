from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QAction, QBrush, QPalette, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QFileDialog,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QSizePolicy,
    QSplitter,
    QStyle,
    QTableView,
    QTextEdit,
    QToolBar,
    QToolButton,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.overrides import (
    OverrideError,
    create_entry,
    delete_override,
    fill_pron_raw_from_headword,
    list_overrides,
    merge_entries,
    record_entry_edit,
    restore_entries,
    set_entry_status,
    soft_delete_entries,
    split_entry,
)

from ...services.state import AppState
from ...services.theme import apply_theme_safe_styles
from ..dialogs.add_entry_dialog import AddEntryDialog
from ..widgets.alphabet_bar import AlphabetBar


class EntriesTab(QWidget):
    PAGE_SIZE = 100

    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.offset = 0
        self.search_text = ""
        self.only_manual = False
        self.flags_only = False
        self.show_deleted = False
        self.alpha_bucket_filter: str | None = None
        self.status_filters: set[str] = {"auto", "reviewed", "validated"}
        self.row_cache: list[dict[str, Any]] = []
        self.dirty_by_entry: dict[str, dict[str, Any]] = {}
        self._loading_model = False
        self._updating_filters = False

        self.table = QTableView()
        self.model = QStandardItemModel(0, 9)
        self.model.setHorizontalHeaderLabels(
            [
                "section",
                "syllables",
                "headword",
                "pron",
                "definition",
                "display",
                "status",
                "flags",
                "manual",
            ]
        )
        self.table.setModel(self.model)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        self.table.clicked.connect(self.on_row_selected)
        self.model.itemChanged.connect(self.on_item_changed)

        self.details = QTextEdit()
        self.details.setReadOnly(True)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Rechercher...")
        self.search.returnPressed.connect(self.on_search)

        self.only_manual_check = QCheckBox("Modifiées à la main")
        self.only_manual_check.toggled.connect(self.on_filter_manual)
        self.show_deleted_check = QCheckBox("Afficher supprimées")
        self.show_deleted_check.toggled.connect(self.on_toggle_show_deleted)
        self.alpha_bar = AlphabetBar(self)
        self.alpha_bar.bucket_changed.connect(self.on_alpha_bucket_changed)
        self.alpha_filter_label = QLabel("Lettre: Tout")

        self.page_label = QLabel("page 1")

        self._build_actions()
        (
            self.toolbar_search,
            self.toolbar_edit_structure,
            self.toolbar_status_nav,
        ) = self._build_toolbars()
        self.toolbar_rows = [
            self.toolbar_search,
            self.toolbar_edit_structure,
            self.toolbar_status_nav,
        ]

        self.left_panel = QWidget()
        left_layout = QVBoxLayout(self.left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(2)
        left_layout.addWidget(self.toolbar_search, 0)
        left_layout.addWidget(self.toolbar_edit_structure, 0)
        left_layout.addWidget(self.toolbar_status_nav, 0)
        left_layout.addWidget(self.table, 1)

        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.addWidget(self.left_panel)
        self.splitter.addWidget(self.details)
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 2)
        self.splitter.setSizes([980, 420])

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.splitter)
        self.setLayout(layout)

        apply_theme_safe_styles(self)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.state.dictionary_changed.connect(lambda _: self.reset_and_refresh())

    def _build_actions(self) -> None:
        self.search_action = QAction("Chercher", self)
        self.search_action.setToolTip("Lancer la recherche sur les entrées")
        self.search_action.triggered.connect(self.on_search)

        self.filters_menu = QMenu(self)
        self.filter_auto_action = self.filters_menu.addAction("Statut auto")
        self.filter_auto_action.setCheckable(True)
        self.filter_auto_action.setChecked(True)
        self.filter_reviewed_action = self.filters_menu.addAction("Statut reviewed")
        self.filter_reviewed_action.setCheckable(True)
        self.filter_reviewed_action.setChecked(True)
        self.filter_validated_action = self.filters_menu.addAction("Statut validated")
        self.filter_validated_action.setCheckable(True)
        self.filter_validated_action.setChecked(True)
        self.filters_menu.addSeparator()
        self.filter_flags_action = self.filters_menu.addAction("Avec flags seulement")
        self.filter_flags_action.setCheckable(True)
        self.filter_flags_action.setChecked(False)
        self.filters_menu.addSeparator()
        self.filter_reset_action = self.filters_menu.addAction("Réinitialiser filtres")

        self.filter_auto_action.toggled.connect(self._on_status_filters_changed)
        self.filter_reviewed_action.toggled.connect(self._on_status_filters_changed)
        self.filter_validated_action.toggled.connect(self._on_status_filters_changed)
        self.filter_flags_action.toggled.connect(self._on_flags_filter_changed)
        self.filter_reset_action.triggered.connect(self._reset_status_filters)

        self.save_action = QAction("Save edits", self)
        self.save_action.setToolTip("Enregistrer les champs édités")
        self.save_action.triggered.connect(self.save_edits)

        self.revert_action = QAction("Revert row", self)
        self.revert_action.setToolTip("Réinitialiser la ligne sélectionnée")
        self.revert_action.triggered.connect(self.revert_selected_row)

        self.undo_action = QAction("Undo override", self)
        self.undo_action.setToolTip("Annuler le dernier override de la ligne")
        self.undo_action.triggered.connect(self.undo_last_override)

        self.split_action = QAction("Split entry", self)
        self.split_action.setToolTip("Scinder l'entrée sélectionnée")
        self.split_action.triggered.connect(self.split_selected_entry)

        self.merge_prev_action = QAction("Merge prev", self)
        self.merge_prev_action.setToolTip("Fusionner avec l'entrée précédente")
        self.merge_prev_action.triggered.connect(lambda: self.merge_selected_entry(direction=-1))

        self.merge_next_action = QAction("Merge next", self)
        self.merge_next_action.setToolTip("Fusionner avec l'entrée suivante")
        self.merge_next_action.triggered.connect(lambda: self.merge_selected_entry(direction=1))

        self.reviewed_action = QAction("Mark reviewed", self)
        self.reviewed_action.setToolTip("Marquer l'entrée comme relue")
        self.reviewed_action.triggered.connect(lambda: self.mark_selected_status("reviewed"))

        self.validated_action = QAction("Mark validated", self)
        self.validated_action.setToolTip("Marquer l'entrée comme validée")
        self.validated_action.triggered.connect(lambda: self.mark_selected_status("validated"))

        self.history_action = QAction("Historique", self)
        self.history_action.setToolTip("Afficher l'historique des corrections")
        self.history_action.triggered.connect(self.show_entry_history)

        self.fill_pron_action = QAction("Remplir pron_raw", self)
        self.fill_pron_action.setToolTip("Remplir pron_raw depuis l'entrée si vide")
        self.fill_pron_action.triggered.connect(self.fill_pron_from_entry)

        self.delete_entry_action = QAction("Supprimer (corbeille)", self)
        self.delete_entry_action.setToolTip("Déplacer la sélection vers la corbeille")
        self.delete_entry_action.triggered.connect(self.delete_selected_entries)

        self.restore_entry_action = QAction("Restaurer", self)
        self.restore_entry_action.setToolTip("Restaurer la sélection depuis la corbeille")
        self.restore_entry_action.triggered.connect(self.restore_selected_entries)

        self.add_entry_action = QAction("Ajouter une entrée…", self)
        self.add_entry_action.setToolTip("Créer une entrée manuelle dans le corpus actif")
        self.add_entry_action.triggered.connect(self.add_entry)

        self.compare_action = QAction("Comparer…", self)
        self.compare_action.setToolTip("Ouvrir l'atelier Comparer")
        self.compare_action.triggered.connect(self.open_compare)

        self.export_selection_action = QAction("Export sélection…", self)
        self.export_selection_action.setToolTip("Exporter les entrées sélectionnées en CSV")
        self.export_selection_action.triggered.connect(self.export_selected_entries)

        self.reset_layout_action = QAction("Réinitialiser disposition", self)
        self.reset_layout_action.setToolTip("Restaurer la disposition 70/30")
        self.reset_layout_action.triggered.connect(self.reset_layout)

        self.prev_action = QAction("Prev", self)
        self.prev_action.triggered.connect(self.prev_page)
        self.next_action = QAction("Next", self)
        self.next_action.triggered.connect(self.next_page)

    def _new_toolbar(self, title: str, object_name: str) -> QToolBar:
        toolbar = QToolBar(title, self)
        toolbar.setObjectName(object_name)
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        toolbar.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextOnly)
        return toolbar

    def _build_toolbars(self) -> tuple[QToolBar, QToolBar, QToolBar]:
        toolbar_search = self._new_toolbar("Curation search", "curation_toolbar_search")
        self._add_toolbar_group(toolbar_search, "Recherche")
        toolbar_search.addWidget(self.search)
        toolbar_search.addAction(self.search_action)
        toolbar_search.addWidget(self.only_manual_check)
        toolbar_search.addWidget(self.show_deleted_check)

        filters_btn = QToolButton(self)
        filters_btn.setText("Filtres…")
        filters_btn.setMenu(self.filters_menu)
        filters_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        toolbar_search.addWidget(filters_btn)
        toolbar_search.addWidget(self.alpha_bar)
        toolbar_search.addWidget(self.alpha_filter_label)

        toolbar_edit_structure = self._new_toolbar(
            "Curation edit structure", "curation_toolbar_edit_structure"
        )
        self._add_toolbar_group(toolbar_edit_structure, "Édition")
        toolbar_edit_structure.addAction(self.save_action)
        toolbar_edit_structure.addAction(self.revert_action)
        toolbar_edit_structure.addAction(self.undo_action)
        toolbar_edit_structure.addSeparator()
        self._add_toolbar_group(toolbar_edit_structure, "Structure")
        toolbar_edit_structure.addAction(self.split_action)
        toolbar_edit_structure.addAction(self.merge_prev_action)
        toolbar_edit_structure.addAction(self.merge_next_action)

        toolbar_status_nav = self._new_toolbar(
            "Curation status nav",
            "curation_toolbar_status_nav",
        )
        self._add_toolbar_group(toolbar_status_nav, "Statut")
        toolbar_status_nav.addAction(self.reviewed_action)
        toolbar_status_nav.addAction(self.validated_action)
        toolbar_status_nav.addSeparator()
        self._add_toolbar_group(toolbar_status_nav, "Actions")
        plus_menu = QMenu(self)
        plus_menu.addAction(self.add_entry_action)
        plus_menu.addAction(self.delete_entry_action)
        plus_menu.addAction(self.restore_entry_action)
        plus_menu.addAction(self.fill_pron_action)
        plus_menu.addAction(self.compare_action)
        plus_menu.addAction(self.export_selection_action)
        plus_menu.addAction(self.history_action)
        plus_menu.addAction(self.reset_layout_action)

        plus_btn = QToolButton(self)
        plus_btn.setText("⋯ Plus")
        plus_btn.setMenu(plus_menu)
        plus_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        toolbar_status_nav.addWidget(plus_btn)

        spacer = QWidget(self)
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar_status_nav.addWidget(spacer)

        toolbar_status_nav.addAction(self.prev_action)
        toolbar_status_nav.addAction(self.next_action)
        toolbar_status_nav.addWidget(self.page_label)

        return toolbar_search, toolbar_edit_structure, toolbar_status_nav

    def _add_toolbar_group(self, toolbar: QToolBar, label: str) -> None:
        group_label = QLabel(f"{label}:")
        group_label.setObjectName(f"curation_group_{label.lower()}")
        toolbar.addWidget(group_label)

    @staticmethod
    def _effective(row: dict[str, Any], stem: str) -> str:
        edit = row.get(f"{stem}_edit")
        if edit is not None and str(edit).strip():
            return str(edit)
        raw = row.get(f"{stem}_raw")
        return str(raw or "")

    @staticmethod
    def _is_manual(row: dict[str, Any]) -> bool:
        return any(
            [
                bool(str(row.get("headword_edit") or "").strip()),
                bool(str(row.get("pron_edit") or "").strip()),
                bool(str(row.get("definition_edit") or "").strip()),
                bool(int(row.get("manual_created") or 0)),
                str(row.get("status") or "auto") != "auto",
            ]
        )

    @staticmethod
    def _parsed_entry_from_snapshot(snapshot: dict[str, Any], fallback_dict_id: str) -> ParsedEntry:
        def _text(key: str) -> str | None:
            value = snapshot.get(key)
            if value is None:
                return None
            return str(value)

        def _int(key: str, default: int = 0) -> int:
            value = snapshot.get(key)
            if value is None or value == "":
                return default
            return int(value)

        parser_version = _int("parser_version", 0)
        template_version = _int("template_version", 0)
        return ParsedEntry(
            dict_id=str(snapshot.get("dict_id") or fallback_dict_id),
            section=str(snapshot.get("section") or ""),
            syllables=_int("syllables", 1),
            headword_raw=str(snapshot.get("headword_raw") or ""),
            pos_raw=str(snapshot.get("pos_raw") or "v"),
            pron_raw=_text("pron_raw"),
            source_path=str(snapshot.get("source_path") or ""),
            line_no=_int("line_no", 0),
            raw_line=str(snapshot.get("raw_line") or ""),
            origin_raw=_text("origin_raw"),
            origin_norm=_text("origin_norm"),
            pos_norm=_text("pos_norm"),
            parser_id=_text("parser_id"),
            parser_version=parser_version or None,
            parser_sha256=_text("parser_sha256"),
            definition_raw=_text("definition_raw"),
            source_record=_text("source_record"),
            template_id=_text("template_id"),
            template_version=template_version or None,
            template_sha256=_text("template_sha256"),
            source_id=_text("source_id"),
            record_key=_text("record_key"),
        )

    def _selected_status_filters(self) -> set[str]:
        selected: set[str] = set()
        if self.filter_auto_action.isChecked():
            selected.add("auto")
        if self.filter_reviewed_action.isChecked():
            selected.add("reviewed")
        if self.filter_validated_action.isChecked():
            selected.add("validated")
        return selected

    def _on_status_filters_changed(self) -> None:
        if self._updating_filters:
            return

        selected = self._selected_status_filters()
        if not selected:
            self._updating_filters = True
            self.filter_auto_action.setChecked(True)
            self._updating_filters = False
            selected = {"auto"}

        self.status_filters = selected
        self.offset = 0
        self.refresh()

    def _on_flags_filter_changed(self, checked: bool) -> None:
        if self._updating_filters:
            return
        self.flags_only = checked
        self.offset = 0
        self.refresh()

    def _reset_status_filters(self) -> None:
        self._updating_filters = True
        self.filter_auto_action.setChecked(True)
        self.filter_reviewed_action.setChecked(True)
        self.filter_validated_action.setChecked(True)
        self.filter_flags_action.setChecked(False)
        self._updating_filters = False

        self.status_filters = {"auto", "reviewed", "validated"}
        self.flags_only = False
        self.offset = 0
        self.refresh()

    def reset_and_refresh(self) -> None:
        self.offset = 0
        self.refresh()

    def on_search(self) -> None:
        self.search_text = self.search.text().strip()
        self.offset = 0
        self.refresh()

    def on_filter_manual(self, checked: bool) -> None:
        self.only_manual = checked
        self.offset = 0
        self.refresh()

    def on_toggle_show_deleted(self, checked: bool) -> None:
        self.show_deleted = checked
        self.offset = 0
        self.refresh()

    def on_alpha_bucket_changed(self, bucket: str) -> None:
        cleaned = str(bucket or "").strip().upper()
        self.alpha_bucket_filter = cleaned or None
        self.alpha_filter_label.setText(f"Lettre: {cleaned or 'Tout'}")
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
            self.row_cache = []
            self.alpha_bar.set_counts({})
            self.alpha_bar.set_active_bucket(None)
            self.alpha_filter_label.setText("Lettre: Tout")
            return

        alpha_counts = self.state.store.alpha_counts(
            self.state.active_dict_id,
            include_deleted=self.show_deleted,
        )
        self.alpha_bar.set_active_bucket(self.alpha_bucket_filter)
        self.alpha_bar.set_counts(alpha_counts)
        self.alpha_filter_label.setText(f"Lettre: {self.alpha_bucket_filter or 'Tout'}")

        rows = self.state.store.list_entries(
            dict_id=self.state.active_dict_id,
            limit=self.PAGE_SIZE,
            offset=self.offset,
            search=self.search_text or None,
            include_deleted=self.show_deleted,
            alpha_bucket=self.alpha_bucket_filter,
        )
        rows_dict = [dict(row) for row in rows]

        issues_count_by_entry: dict[str, int] = {}
        for row in rows_dict:
            entry_id = str(row["entry_id"])
            _, issues = self.state.store.entry_details(entry_id)
            issues_count_by_entry[entry_id] = len(issues)

        filtered_rows: list[dict[str, Any]] = []
        for row in rows_dict:
            if self.only_manual and not self._is_manual(row):
                continue
            status = str(row.get("status") or "auto")
            if status not in self.status_filters:
                continue
            if self.flags_only and issues_count_by_entry.get(str(row["entry_id"]), 0) == 0:
                continue
            filtered_rows.append(row)

        self.row_cache = filtered_rows
        self.dirty_by_entry.clear()

        self._loading_model = True
        self.model.removeRows(0, self.model.rowCount())
        trash_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_TrashIcon)
        deleted_foreground = QBrush(self.palette().color(QPalette.ColorRole.Mid))
        for row in filtered_rows:
            entry_id = str(row["entry_id"])
            issues_count = issues_count_by_entry.get(entry_id, 0)
            headword = self._effective(row, "headword")
            pron = self._effective(row, "pron")
            definition = self._effective(row, "definition")
            display = str(row.get("pron_render") or row.get("form_display") or pron or headword)
            is_deleted = bool(int(row.get("is_deleted") or 0))
            base_status = str(row.get("status") or "auto")
            status = f"deleted ({base_status})" if is_deleted else base_status
            manual = "yes" if self._is_manual(row) else ""
            deleted_at = str(row.get("deleted_at") or "").strip()
            deleted_reason = str(row.get("deleted_reason") or "").strip()
            deleted_tooltip = ""
            if is_deleted:
                tooltip_parts = ["Entrée supprimée (corbeille)"]
                if deleted_at:
                    tooltip_parts.append(f"Date: {deleted_at}")
                if deleted_reason:
                    tooltip_parts.append(f"Raison: {deleted_reason}")
                deleted_tooltip = "\n".join(tooltip_parts)

            values = [
                str(row.get("section") or ""),
                str(row.get("syllables") or ""),
                headword,
                pron,
                definition,
                display,
                status,
                str(issues_count),
                manual,
            ]
            items = [QStandardItem(value) for value in values]
            for idx, item in enumerate(items):
                item.setEditable(idx in {2, 3, 4} and not is_deleted)
            items[0].setData(entry_id, Qt.ItemDataRole.UserRole)
            if is_deleted:
                items[6].setIcon(trash_icon)
                for item in items:
                    item.setData(deleted_foreground, Qt.ItemDataRole.ForegroundRole)
                    if deleted_tooltip:
                        item.setToolTip(deleted_tooltip)
            self.model.appendRow(items)
        self._loading_model = False

        page = self.offset // self.PAGE_SIZE + 1
        self.page_label.setText(f"page {page}")

    def _selected_entry_id(self) -> str | None:
        index = self.table.currentIndex()
        if not index.isValid():
            return None
        item = self.model.item(index.row(), 0)
        if not item:
            return None
        value = item.data(Qt.ItemDataRole.UserRole)
        return str(value) if value else None

    def _selected_entry_ids(self) -> list[str]:
        selection_model = self.table.selectionModel()
        if selection_model is None:
            return []
        ids: list[str] = []
        seen: set[str] = set()
        for index in selection_model.selectedRows():
            item = self.model.item(index.row(), 0)
            if not item:
                continue
            value = item.data(Qt.ItemDataRole.UserRole)
            entry_id = str(value or "").strip()
            if not entry_id or entry_id in seen:
                continue
            seen.add(entry_id)
            ids.append(entry_id)
        return ids

    def _current_row_payload(self) -> dict[str, Any] | None:
        entry_id = self._selected_entry_id()
        if not entry_id:
            return None
        for row in self.row_cache:
            if str(row.get("entry_id")) == entry_id:
                return row
        return None

    def on_item_changed(self, item: QStandardItem) -> None:
        if self._loading_model:
            return
        row_idx = item.row()
        if row_idx < 0 or row_idx >= len(self.row_cache):
            return

        row = self.row_cache[row_idx]
        entry_id = str(row["entry_id"])

        column_to_field = {
            2: "headword_edit",
            3: "pron_edit",
            4: "definition_edit",
        }
        field_name = column_to_field.get(item.column())
        if not field_name:
            return

        raw_stem = field_name.replace("_edit", "_raw")
        raw_value = str(row.get(raw_stem) or "")
        current_value = item.text().strip()

        if current_value == raw_value:
            if entry_id in self.dirty_by_entry:
                self.dirty_by_entry[entry_id].pop(field_name, None)
                if not self.dirty_by_entry[entry_id]:
                    self.dirty_by_entry.pop(entry_id, None)
            return

        self.dirty_by_entry.setdefault(entry_id, {})[field_name] = current_value

    def save_edits(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        if not self.dirty_by_entry:
            QMessageBox.information(self, "Curation", "Aucune modification à sauvegarder.")
            return

        try:
            for entry_id, field_changes in self.dirty_by_entry.items():
                record_entry_edit(
                    store=self.state.store,
                    corpus_id=self.state.active_dict_id,
                    entry_id=entry_id,
                    field_changes=field_changes,
                )
        except OverrideError as exc:
            QMessageBox.warning(self, "Curation", str(exc))
            return

        QMessageBox.information(self, "Curation", "Modifications enregistrées.")
        self.refresh()

    def add_entry(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return

        dialog = AddEntryDialog(parent=self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        try:
            entry_id = create_entry(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                headword_raw=dialog.headword,
                pron_raw=dialog.pron,
                definition_raw=dialog.definition,
                note=dialog.note,
                entry_is_pron=dialog.entry_is_pron,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Ajouter une entrée", str(exc))
            return

        self.offset = 0
        self.refresh()
        self._focus_entry(entry_id)
        self.state.notify_data_changed()
        QToolTip.showText(
            self.mapToGlobal(self.rect().topLeft()),
            "Entrée ajoutée",
            self,
            self.rect(),
            1200,
        )

    def _focus_entry(self, entry_id: str) -> bool:
        for row_idx in range(self.model.rowCount()):
            row_item = self.model.item(row_idx, 0)
            if not row_item:
                continue
            current_id = str(row_item.data(Qt.ItemDataRole.UserRole) or "")
            if current_id != entry_id:
                continue
            self.table.selectRow(row_idx)
            self.table.scrollTo(row_item.index(), QTableView.ScrollHint.PositionAtCenter)
            self.on_row_selected()
            self._flash_row(row_idx)
            return True
        return False

    def _flash_row(self, row_idx: int) -> None:
        if row_idx < 0 or row_idx >= self.model.rowCount():
            return
        flash_color = self.table.palette().alternateBase()
        original_backgrounds: list[tuple[QStandardItem, Any]] = []
        for col_idx in range(self.model.columnCount()):
            item = self.model.item(row_idx, col_idx)
            if item is None:
                continue
            original_backgrounds.append((item, item.background()))
            item.setBackground(flash_color)

        def _restore() -> None:
            for item, brush in original_backgrounds:
                item.setBackground(brush)

        QTimer.singleShot(900, _restore)

    def _select_row_by_index(self, row_idx: int) -> None:
        if self.model.rowCount() <= 0:
            return
        target = max(0, min(row_idx, self.model.rowCount() - 1))
        self.table.selectRow(target)
        self.table.scrollTo(
            self.model.index(target, 0),
            QTableView.ScrollHint.PositionAtCenter,
        )
        self.on_row_selected()

    def _focus_after_mutation(self, preferred_entry_id: str | None, previous_row_idx: int) -> None:
        if preferred_entry_id and self._focus_entry(preferred_entry_id):
            return
        self._select_row_by_index(previous_row_idx)

    def fill_pron_from_entry(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return

        selected_ids = self._selected_entry_ids()
        if not selected_ids:
            answer = QMessageBox.question(
                self,
                "Remplir pron_raw",
                "Aucune sélection active. Appliquer à tout le corpus ?",
            )
            if answer != QMessageBox.StandardButton.Yes:
                return

        summary = fill_pron_raw_from_headword(
            store=self.state.store,
            corpus_id=self.state.active_dict_id,
            entry_ids=selected_ids or None,
        )
        self.refresh()
        QMessageBox.information(
            self,
            "Remplir pron_raw",
            (
                f"Mises à jour: {summary['updated']}\n"
                f"Déjà rempli: {summary['skipped_non_empty']}\n"
                f"Headword manquant: {summary['skipped_no_headword']}"
            ),
        )

    def delete_selected_entries(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        selected_ids = self._selected_entry_ids()
        if not selected_ids:
            QMessageBox.information(
                self,
                "Supprimer (corbeille)",
                "Sélectionnez au moins une entrée à supprimer.",
            )
            return

        reason, ok = QInputDialog.getText(
            self,
            "Supprimer (corbeille)",
            "Raison (optionnelle):",
        )
        if not ok:
            return

        current_row = self.table.currentIndex().row()
        preferred_entry_id = selected_ids[0] if self.show_deleted else None
        try:
            deleted_count = soft_delete_entries(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                entry_ids=selected_ids,
                reason=reason.strip() or None,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Supprimer (corbeille)", str(exc))
            return

        if deleted_count <= 0:
            QMessageBox.information(
                self,
                "Supprimer (corbeille)",
                "Aucune entrée supprimée (sélection déjà en corbeille ?).",
            )
            return

        self.refresh()
        self._focus_after_mutation(preferred_entry_id, current_row)
        self.state.notify_data_changed()
        QToolTip.showText(
            self.mapToGlobal(self.rect().topLeft()),
            f"{deleted_count} entrée(s) supprimée(s)",
            self,
            self.rect(),
            1500,
        )

    def restore_selected_entries(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        selected_ids = self._selected_entry_ids()
        if not selected_ids:
            QMessageBox.information(
                self,
                "Restaurer",
                "Sélectionnez au moins une entrée à restaurer.",
            )
            return

        current_row = self.table.currentIndex().row()
        preferred_entry_id = selected_ids[0]
        try:
            restored_count = restore_entries(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                entry_ids=selected_ids,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Restaurer", str(exc))
            return

        if restored_count <= 0:
            QMessageBox.information(
                self,
                "Restaurer",
                "Aucune entrée restaurée (sélection non supprimée ?).",
            )
            return

        self.refresh()
        self._focus_after_mutation(preferred_entry_id, current_row)
        self.state.notify_data_changed()
        QToolTip.showText(
            self.mapToGlobal(self.rect().topLeft()),
            f"{restored_count} entrée(s) restaurée(s)",
            self,
            self.rect(),
            1500,
        )

    def export_selected_entries(self) -> None:
        selected_ids = self._selected_entry_ids()
        if not selected_ids:
            QMessageBox.information(self, "Export sélection", "Sélectionnez au moins une entrée.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Exporter la sélection",
            "curation_selection.csv",
            "CSV (*.csv)",
        )
        if not file_path:
            return

        selected_set = set(selected_ids)
        rows = [row for row in self.row_cache if str(row.get("entry_id")) in selected_set]
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "entry_id",
                    "headword_effective",
                    "pron_effective",
                    "definition_effective",
                    "status",
                    "flags",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "entry_id": str(row.get("entry_id") or ""),
                        "headword_effective": self._effective(row, "headword"),
                        "pron_effective": self._effective(row, "pron"),
                        "definition_effective": self._effective(row, "definition"),
                        "status": str(row.get("status") or "auto"),
                        "flags": str(
                            len(self.state.store.entry_details(str(row.get("entry_id")))[1])
                            if self.state.store
                            else 0
                        ),
                    }
                )

        QMessageBox.information(self, "Export sélection", f"Export créé: {target}")

    def revert_selected_row(self) -> None:
        index = self.table.currentIndex()
        if not index.isValid():
            return

        row_idx = index.row()
        if row_idx < 0 or row_idx >= len(self.row_cache):
            return
        row = self.row_cache[row_idx]
        entry_id = str(row["entry_id"])

        self._loading_model = True
        self.model.item(row_idx, 2).setText(str(row.get("headword_raw") or ""))
        self.model.item(row_idx, 3).setText(str(row.get("pron_raw") or ""))
        self.model.item(row_idx, 4).setText(str(row.get("definition_raw") or ""))
        self._loading_model = False

        self.dirty_by_entry.pop(entry_id, None)

    def split_selected_entry(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return

        payload = self._current_row_payload()
        if payload is None:
            return

        entry_id = str(payload["entry_id"])
        seed = self._effective(payload, "headword")
        parts_text, ok = QInputDialog.getText(
            self,
            "Split entry",
            "Nouvelles entrées (séparées par espace, virgule ou ;)",
            text=seed,
        )
        if not ok:
            return

        raw = parts_text.strip()
        if not raw:
            return
        if ";" in raw:
            parts = [part.strip() for part in raw.split(";") if part.strip()]
        elif "," in raw:
            parts = [part.strip() for part in raw.split(",") if part.strip()]
        else:
            parts = [part.strip() for part in raw.split() if part.strip()]

        try:
            split_entry(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                entry_id=entry_id,
                parts=parts,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Split entry", str(exc))
            return

        self.refresh()

    def merge_selected_entry(self, direction: int) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return

        index = self.table.currentIndex()
        if not index.isValid():
            return

        row_idx = index.row()
        target_idx = row_idx + direction
        if target_idx < 0 or target_idx >= len(self.row_cache):
            QMessageBox.information(self, "Merge", "Aucune entrée voisine disponible.")
            return

        entry_id_a = str(self.row_cache[row_idx]["entry_id"])
        entry_id_b = str(self.row_cache[target_idx]["entry_id"])

        try:
            merge_entries(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                entry_id_a=entry_id_a,
                entry_id_b=entry_id_b,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Merge entry", str(exc))
            return

        self.refresh()

    def mark_selected_status(self, status: str) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return

        entry_id = self._selected_entry_id()
        if not entry_id:
            return

        try:
            set_entry_status(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                entry_id=entry_id,
                status=status,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Entry status", str(exc))
            return

        self.refresh()

    def on_row_selected(self) -> None:
        entry_id = self._selected_entry_id()
        if not entry_id or not self.state.store or not self.state.active_dict_id:
            return

        entry, issues = self.state.store.entry_details(entry_id)
        if not entry:
            return

        overrides = list_overrides(
            store=self.state.store,
            corpus_id=self.state.active_dict_id,
            scope="entry",
            entry_id=entry_id,
        )

        details = {
            "entry_id": entry["entry_id"],
            "source_path": entry["source_path"],
            "line_no": entry["line_no"],
            "headword_raw": entry["headword_raw"],
            "headword_edit": entry["headword_edit"],
            "pron_raw": entry["pron_raw"],
            "pron_edit": entry["pron_edit"],
            "definition_raw": entry["definition_raw"],
            "definition_edit": entry["definition_edit"],
            "status": entry["status"],
            "is_deleted": entry["is_deleted"],
            "deleted_at": entry["deleted_at"],
            "deleted_reason": entry["deleted_reason"],
            "manual_created": entry["manual_created"],
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
            "overrides": [
                {
                    "override_id": item.override_id,
                    "op": item.op,
                    "before": item.before,
                    "after": item.after,
                    "created_at": item.created_at,
                }
                for item in overrides
            ],
        }
        self.details.setPlainText(json.dumps(details, ensure_ascii=False, indent=2))

    def show_entry_history(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        entry_id = self._selected_entry_id()
        if not entry_id:
            QMessageBox.information(self, "Historique", "Sélectionnez d'abord une entrée.")
            return

        overrides = list_overrides(
            store=self.state.store,
            corpus_id=self.state.active_dict_id,
            scope="entry",
            entry_id=entry_id,
        )
        payload = [
            {
                "override_id": item.override_id,
                "op": item.op,
                "before": item.before,
                "after": item.after,
                "created_at": item.created_at,
            }
            for item in overrides
        ]
        QMessageBox.information(
            self,
            "Historique des corrections",
            json.dumps(payload, ensure_ascii=False, indent=2),
        )

    def undo_last_override(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        entry_id = self._selected_entry_id()
        if not entry_id:
            QMessageBox.information(self, "Undo", "Sélectionnez d'abord une entrée.")
            return

        overrides = list_overrides(
            store=self.state.store,
            corpus_id=self.state.active_dict_id,
            scope="entry",
            entry_id=entry_id,
        )
        if not overrides:
            QMessageBox.information(self, "Undo", "Aucun override à annuler.")
            return

        latest = overrides[0]
        try:
            if latest.op == "SPLIT_ENTRY":
                new_ids = [str(item) for item in latest.after.get("new_entry_ids", [])]
                before_entry = latest.before.get("entry")
                if not isinstance(before_entry, dict) or not new_ids:
                    QMessageBox.information(self, "Undo", "Payload SPLIT_ENTRY incomplet.")
                    return

                restored = self._parsed_entry_from_snapshot(before_entry, self.state.active_dict_id)
                self.state.store.delete_entries(new_ids)
                self.state.store.insert_entries([restored])
                self.state.store.insert_override(
                    corpus_id=self.state.active_dict_id,
                    scope="entry",
                    source_id=latest.source_id,
                    record_key=latest.record_key,
                    entry_id=entry_id,
                    op="UNDO_OVERRIDE",
                    before_json=latest.after,
                    after_json={"restored": "split_entry"},
                    note=f"undo:{latest.override_id}",
                )
            elif latest.op == "MERGE_ENTRY":
                before_entries = latest.before.get("entries")
                merged_id = latest.after.get("entry_id")
                if not isinstance(before_entries, list) or not merged_id:
                    QMessageBox.information(self, "Undo", "Payload MERGE_ENTRY incomplet.")
                    return

                restored_entries = [
                    self._parsed_entry_from_snapshot(item, self.state.active_dict_id)
                    for item in before_entries
                    if isinstance(item, dict)
                ]
                if not restored_entries:
                    QMessageBox.information(self, "Undo", "Aucune entrée à restaurer.")
                    return

                self.state.store.delete_entries([str(merged_id)])
                self.state.store.insert_entries(restored_entries)
                self.state.store.insert_override(
                    corpus_id=self.state.active_dict_id,
                    scope="entry",
                    source_id=latest.source_id,
                    record_key=latest.record_key,
                    entry_id=entry_id,
                    op="UNDO_OVERRIDE",
                    before_json=latest.after,
                    after_json={"restored": "merge_entry"},
                    note=f"undo:{latest.override_id}",
                )
            else:
                before = latest.before
                changes = {
                    key: before.get(key)
                    for key in ["headword_edit", "pron_edit", "definition_edit", "status"]
                    if key in before
                }
                if not changes:
                    QMessageBox.information(
                        self, "Undo", "Cet override ne contient pas de payload annulable."
                    )
                    return

                self.state.store.update_entry_edit_fields(
                    entry_id=entry_id,
                    dict_id=self.state.active_dict_id,
                    field_changes=changes,
                )
                self.state.store.insert_override(
                    corpus_id=self.state.active_dict_id,
                    scope="entry",
                    source_id=latest.source_id,
                    record_key=latest.record_key,
                    entry_id=entry_id,
                    op="EDIT_ENTRY",
                    before_json=latest.after,
                    after_json=changes,
                    note=f"undo:{latest.override_id}",
                )
            delete_override(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                scope="entry",
                override_id=latest.override_id,
            )
        except OverrideError as exc:
            QMessageBox.warning(self, "Undo", str(exc))
            return

        self.refresh()

    def open_compare(self) -> None:
        if self.state.active_dict_id:
            self.state.request_compare([self.state.active_dict_id])

    def reset_layout(self) -> None:
        self.details.show()
        total = max(self.splitter.width(), 1000)
        left = int(total * 0.7)
        right = max(1, total - left)
        self.splitter.setSizes([left, right])
