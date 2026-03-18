from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.overrides import (
    OverrideError,
    create_entry_from_record,
    delete_override,
    list_overrides,
    upsert_override_record,
)
from dicodiachro.core.pipeline import PipelineError
from dicodiachro.core.templates.csv_mapping import available_csv_columns
from dicodiachro.core.templates.engine import (
    TemplateEngineError,
    apply_template_to_records,
    load_source_records,
)
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind, TemplateSpec
from dicodiachro.core.templates.workflow import (
    apply_template_to_corpus,
    list_template_sources,
    preview_template_on_source,
)

from ...services.jobs import JobThread
from ...services.state import AppState
from ...services.theme import apply_theme_safe_styles

TEMPLATE_META: dict[TemplateKind, tuple[str, str]] = {
    TemplateKind.WORDLIST_TOKENS: (
        "Liste de mots",
        "Découpe chaque record texte en tokens et produit 1 entrée par token utile.",
    ),
    TemplateKind.ENTRY_PLUS_DEFINITION: (
        "Entrée + définition",
        "Coupe la ligne en deux (headword + définition) selon un séparateur lisible.",
    ),
    TemplateKind.HEADWORD_PLUS_PRON: (
        "Mot + prononciation",
        "Sépare la ligne en deux champs (headword et prononciation).",
    ),
    TemplateKind.FR_EN_PRON_THREE_COLS: (
        "FR + EN + pron (3 colonnes)",
        "Extrait headword/pos depuis la colonne FR, l'anglais en définition, et la prononciation en colonne 3.",
    ),
    TemplateKind.CSV_MAPPING: (
        "CSV (mapping)",
        "Mappe des colonnes CSV vers headword/pron/definition avec options de split.",
    ),
}


class ExtractedEditDialog(QDialog):
    def __init__(self, headword: str, pron: str, definition: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Éditer extraction")

        self.headword_edit = QLineEdit(headword)
        self.pron_edit = QLineEdit(pron)
        self.definition_edit = QLineEdit(definition)

        form = QFormLayout()
        form.addRow("Headword", self.headword_edit)
        form.addRow("Pron", self.pron_edit)
        form.addRow("Definition", self.definition_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(buttons)
        self.setLayout(layout)


class SplitRecordDialog(QDialog):
    def __init__(self, seed: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Scinder le record")

        self.entries_edit = QPlainTextEdit(seed)
        self.entries_edit.setPlaceholderText(
            "Une entrée par ligne, ou séparées par espace/virgule/point-virgule"
        )

        note = QLabel(
            "Les valeurs saisies deviennent des headword_raw. "
            "Pour mot+pron, utilisez `mot<TAB>pron` ligne par ligne."
        )
        note.setWordWrap(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(note)
        layout.addWidget(self.entries_edit)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def parsed_entries(self) -> list[dict[str, str]]:
        raw = self.entries_edit.toPlainText().strip()
        if not raw:
            return []

        rows = [line.strip() for line in raw.splitlines() if line.strip()]
        if len(rows) == 1:
            compact = rows[0]
            if ";" in compact:
                rows = [part.strip() for part in compact.split(";") if part.strip()]
            elif "," in compact:
                rows = [part.strip() for part in compact.split(",") if part.strip()]
            elif " " in compact:
                rows = [part.strip() for part in compact.split() if part.strip()]

        parsed: list[dict[str, str]] = []
        for row in rows:
            if "\t" in row:
                headword, pron = row.split("\t", 1)
                parsed.append(
                    {
                        "headword_raw": headword.strip(),
                        "pron_raw": pron.strip(),
                    }
                )
                continue
            parsed.append({"headword_raw": row.strip()})

        return [item for item in parsed if item.get("headword_raw")]


class CreateEntryFromRecordDialog(QDialog):
    TOKEN_SPLIT_RE = re.compile(r"[\s;,]+")

    def __init__(
        self,
        source_text: str,
        headword_seed: str = "",
        pron_seed: str = "",
        definition_seed: str = "",
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Créer une entrée depuis ce record")

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Liste de mots", "wordlist")
        self.mode_combo.addItem("Mot + pron", "headword_pron")
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)

        self.tokens_edit = QPlainTextEdit(source_text)
        self.tokens_edit.setPlaceholderText("Tokens séparés par espaces, virgules ou ;")

        self.headword_edit = QLineEdit(headword_seed or source_text.strip())
        self.pron_edit = QLineEdit(pron_seed)
        self.definition_edit = QLineEdit(definition_seed)
        self.entry_is_pron_check = QCheckBox("Entrée = prononciation")
        self.entry_is_pron_check.toggled.connect(self._on_entry_is_pron_toggled)
        self.headword_edit.textChanged.connect(self._sync_pron_with_headword)
        self.note_edit = QLineEdit()
        self.note_edit.setPlaceholderText("Note d'audit (optionnelle)")

        self.wordlist_panel = QWidget()
        wordlist_layout = QVBoxLayout(self.wordlist_panel)
        wordlist_layout.setContentsMargins(0, 0, 0, 0)
        wordlist_layout.addWidget(QLabel("Tokens à créer"))
        wordlist_layout.addWidget(self.tokens_edit)

        self.headpron_panel = QWidget()
        headpron_form = QFormLayout(self.headpron_panel)
        headpron_form.addRow("Headword", self.headword_edit)
        headpron_form.addRow("Prononciation", self.pron_edit)
        headpron_form.addRow("", self.entry_is_pron_check)
        headpron_form.addRow("Définition", self.definition_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("Créer")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("Annuler")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        form = QFormLayout()
        form.addRow("Mode", self.mode_combo)
        form.addRow("Note", self.note_edit)

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Source"))
        layout.addWidget(QLabel(source_text.strip() or ""))
        layout.addLayout(form)
        layout.addWidget(self.wordlist_panel)
        layout.addWidget(self.headpron_panel)
        layout.addWidget(buttons)

        self._on_mode_changed()

    def _on_mode_changed(self) -> None:
        mode = str(self.mode_combo.currentData() or "wordlist")
        self.wordlist_panel.setVisible(mode == "wordlist")
        self.headpron_panel.setVisible(mode == "headword_pron")

    def _on_entry_is_pron_toggled(self, checked: bool) -> None:
        self.pron_edit.setEnabled(not checked)
        self._sync_pron_with_headword()

    def _sync_pron_with_headword(self) -> None:
        if self.entry_is_pron_check.isChecked():
            self.pron_edit.setText(self.headword_edit.text().strip())

    def entries_to_create(self) -> list[dict[str, str | None]]:
        mode = str(self.mode_combo.currentData() or "wordlist")
        if mode == "wordlist":
            raw_tokens = self.tokens_edit.toPlainText()
            tokens = [
                token.strip() for token in self.TOKEN_SPLIT_RE.split(raw_tokens) if token.strip()
            ]
            entries: list[dict[str, str | None]] = []
            for token in tokens:
                entries.append(
                    {
                        "headword_raw": token,
                        "pron_raw": token if self.entry_is_pron_check.isChecked() else None,
                        "definition_raw": None,
                    }
                )
            return entries

        headword = self.headword_edit.text().strip()
        if not headword:
            return []
        pron = self.pron_edit.text().strip()
        definition = self.definition_edit.text().strip()
        return [
            {
                "headword_raw": headword,
                "pron_raw": pron or None,
                "definition_raw": definition or None,
            }
        ]

    @property
    def note(self) -> str | None:
        value = self.note_edit.text().strip()
        return value or None


class TemplatesTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.current_job: JobThread | None = None
        self.preview_payload: dict[str, Any] = {}
        self.visible_rows: list[dict[str, Any]] = []

        self.source_combo = QComboBox()
        self.source_type_label = QLabel("Type source: -")
        self.template_list = QListWidget()
        self.template_summary = QLabel("")
        self.template_summary.setWordWrap(True)
        self.workflow_label = QLabel(
            "Ce que fait cet atelier: transforme des records source en entrées diplomatiques."
        )
        self.workflow_label.setWordWrap(True)
        self.next_step_label = QLabel("Étape suivante recommandée: Appliquer une convention.")
        self.next_step_label.setWordWrap(True)
        self.next_step_btn = QPushButton("Aller à l'étape suivante")
        self.next_step_btn.clicked.connect(self.open_conventions)

        self.template_id_edit = QLineEdit()
        self.version_spin = QSpinBox()
        self.version_spin.setRange(1, 999)
        self.version_spin.setValue(1)

        self.preview_limit = QSpinBox()
        self.preview_limit.setRange(1, 5000)
        self.preview_limit.setValue(200)
        self.diff_only = QCheckBox("Diff view")
        self.diff_only.toggled.connect(lambda _: self._rerender_preview())

        self.records_count_label = QLabel("Records analysés: 0")
        self.entries_count_label = QLabel("Entrées produites: 0")
        self.ignored_count_label = QLabel("Ignorés: 0")
        self.unrecognized_count_label = QLabel("Non reconnus: 0")
        self.overridden_count_label = QLabel("Overridden: 0")
        self.pending_changes_label = QLabel("Changements en attente: 0")
        self.apply_pending_btn = QPushButton("Appliquer maintenant")
        self.apply_pending_btn.clicked.connect(self.apply_template)
        self.apply_pending_btn.setVisible(False)

        self.preview_table = QTableWidget(0, 7)
        self.preview_table.setHorizontalHeaderLabels(
            [
                "Override",
                "Source",
                "headword_raw",
                "pron_raw",
                "definition_raw",
                "Statut",
                "Pourquoi",
            ]
        )
        self.preview_table.horizontalHeader().setStretchLastSection(True)
        self.preview_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.preview_table.customContextMenuRequested.connect(self._show_preview_context_menu)
        self.preview_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.preview_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        self.apply_profile_check = QCheckBox("Appliquer conventions après extraction")
        self.apply_profile_check.setChecked(True)
        self.profile_combo = QComboBox()
        self.profile_combo.setEditable(True)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        self.apply_summary = QTextEdit()
        self.apply_summary.setReadOnly(True)
        self.history = QTextEdit()
        self.history.setReadOnly(True)

        self.wordlist_trim = QCheckBox("Nettoyer la ponctuation terminale des tokens")
        self.wordlist_pron_from_headword = QCheckBox(
            "Entrée = prononciation (copier dans pron_raw)"
        )

        self.entrydef_separator = QComboBox()
        self.entrydef_separator.addItem("Virgule", "comma")
        self.entrydef_separator.addItem("Point-virgule", "semicolon")
        self.entrydef_separator.addItem("Double espace", "double_space")
        self.entrydef_separator.addItem("Tabulation", "tab")
        self.entrydef_separator.addItem("Séparateur custom", "custom")
        self.entrydef_custom = QLineEdit()
        self.entrydef_custom.setPlaceholderText("Séparateur custom")

        self.headpron_separator = QComboBox()
        self.headpron_separator.addItem("Tabulation", "tab")
        self.headpron_separator.addItem("Espaces multiples", "multi_spaces")
        self.headpron_separator.addItem("Séparateur custom", "custom")
        self.headpron_custom = QLineEdit()
        self.headpron_custom.setPlaceholderText("Séparateur custom")
        self.headpron_trim = QCheckBox("Nettoyer ponctuation terminale")

        self.fr_en_pron_separator = QComboBox()
        self.fr_en_pron_separator.addItem("Auto (recommandé)", "auto")
        self.fr_en_pron_separator.addItem("3+ espaces", "triple_spaces")
        self.fr_en_pron_separator.addItem("2+ espaces", "double_spaces")
        self.fr_en_pron_separator.addItem("Tabulation", "tab")
        self.fr_en_pron_separator.addItem("Séparateur custom", "custom")
        self.fr_en_pron_custom = QLineEdit()
        self.fr_en_pron_custom.setPlaceholderText("Séparateur custom")
        self.fr_en_pron_trim = QCheckBox("Nettoyer ponctuation terminale")
        self.fr_en_pron_trim.setChecked(True)

        self.csv_headword_col = QComboBox()
        self.csv_pron_col = QComboBox()
        self.csv_definition_col = QComboBox()
        self.csv_split_mode = QComboBox()
        self.csv_split_mode.addItem("Aucun split", "none")
        self.csv_split_mode.addItem("Whitespace", "whitespace")
        self.csv_split_mode.addItem("Point-virgule", "semicolon")
        self.csv_split_mode.addItem("Virgule", "comma")
        self.csv_ignore_empty = QCheckBox("Ignorer les lignes sans headword")
        self.csv_ignore_empty.setChecked(True)

        self.csv_pron_col.addItem("(Aucune)", "")
        self.csv_definition_col.addItem("(Aucune)", "")

        self.params_stack = QGroupBox("Paramètres gabarit")
        self.params_layout = QVBoxLayout()
        self.params_stack.setLayout(self.params_layout)

        self.wordlist_panel = QWidget()
        wordlist_form = QFormLayout()
        wordlist_form.addRow(self.wordlist_trim)
        wordlist_form.addRow(self.wordlist_pron_from_headword)
        self.wordlist_panel.setLayout(wordlist_form)

        self.entrydef_panel = QWidget()
        entrydef_form = QFormLayout()
        entrydef_form.addRow("Couper à", self.entrydef_separator)
        entrydef_form.addRow("Custom", self.entrydef_custom)
        self.entrydef_panel.setLayout(entrydef_form)

        self.headpron_panel = QWidget()
        headpron_form = QFormLayout()
        headpron_form.addRow("Séparateur", self.headpron_separator)
        headpron_form.addRow("Custom", self.headpron_custom)
        headpron_form.addRow(self.headpron_trim)
        self.headpron_panel.setLayout(headpron_form)

        self.fr_en_pron_panel = QWidget()
        fr_en_pron_form = QFormLayout()
        fr_en_pron_form.addRow("Séparateur colonnes", self.fr_en_pron_separator)
        fr_en_pron_form.addRow("Custom", self.fr_en_pron_custom)
        fr_en_pron_form.addRow(self.fr_en_pron_trim)
        self.fr_en_pron_panel.setLayout(fr_en_pron_form)

        self.csv_panel = QWidget()
        csv_form = QFormLayout()
        csv_form.addRow("Colonne Headword", self.csv_headword_col)
        csv_form.addRow("Colonne Pron", self.csv_pron_col)
        csv_form.addRow("Colonne Definition", self.csv_definition_col)
        csv_form.addRow("Split headword", self.csv_split_mode)
        csv_form.addRow(self.csv_ignore_empty)
        self.csv_panel.setLayout(csv_form)

        self.params_layout.addWidget(self.wordlist_panel)
        self.params_layout.addWidget(self.entrydef_panel)
        self.params_layout.addWidget(self.headpron_panel)
        self.params_layout.addWidget(self.fr_en_pron_panel)
        self.params_layout.addWidget(self.csv_panel)

        refresh_btn = QPushButton("Rafraîchir")
        refresh_btn.clicked.connect(self.refresh)

        preview_btn = QPushButton("Prévisualiser")
        preview_btn.clicked.connect(self.preview_template)
        suggest_template_btn = QPushButton("Construire depuis sélection")
        suggest_template_btn.clicked.connect(self.build_template_from_selection)
        skip_selected_btn = QPushButton("Ignorer sélection")
        skip_selected_btn.clicked.connect(self.skip_selected_preview_records)
        clear_selected_btn = QPushButton("Annuler override sélection")
        clear_selected_btn.clicked.connect(self.clear_selected_preview_overrides)

        apply_btn = QPushButton("Appliquer au corpus")
        apply_btn.clicked.connect(self.apply_template)

        history_btn = QPushButton("Historique des corrections")
        history_btn.clicked.connect(self.show_history_dialog)

        self.template_list.currentItemChanged.connect(self._on_kind_changed)
        self.source_combo.currentIndexChanged.connect(lambda _: self._on_source_changed())

        left = QWidget()
        left_layout = QVBoxLayout()
        source_row = QHBoxLayout()
        source_row.addWidget(QLabel("Source"))
        source_row.addWidget(self.source_combo)
        source_row.addWidget(refresh_btn)

        left_layout.addLayout(source_row)
        left_layout.addWidget(self.source_type_label)
        left_layout.addWidget(QLabel("Choisir gabarit"))
        left_layout.addWidget(self.template_list)
        left_layout.addWidget(QLabel("Résumé"))
        left_layout.addWidget(self.template_summary)
        left_layout.addWidget(self.params_stack)
        left_layout.addWidget(
            QLabel(
                "Si plusieurs entrées sont sur une ligne: utilisez 'Liste de mots' ou 'Scinder'."
            )
        )
        left_layout.addStretch(1)
        left.setLayout(left_layout)

        center = QWidget()
        center_layout = QVBoxLayout()
        preview_top = QHBoxLayout()
        preview_top.addWidget(preview_btn)
        preview_top.addWidget(suggest_template_btn)
        preview_top.addWidget(skip_selected_btn)
        preview_top.addWidget(clear_selected_btn)
        preview_top.addWidget(QLabel("Lignes"))
        preview_top.addWidget(self.preview_limit)
        preview_top.addWidget(self.diff_only)
        preview_top.addStretch(1)

        counters = QGridLayout()
        counters.addWidget(self.records_count_label, 0, 0)
        counters.addWidget(self.entries_count_label, 0, 1)
        counters.addWidget(self.ignored_count_label, 1, 0)
        counters.addWidget(self.unrecognized_count_label, 1, 1)
        counters.addWidget(self.overridden_count_label, 2, 0)

        center_layout.addLayout(preview_top)
        center_layout.addLayout(counters)
        center_layout.addWidget(self.preview_table)
        center.setLayout(center_layout)

        right = QWidget()
        right_layout = QVBoxLayout()
        template_meta_form = QFormLayout()
        template_meta_form.addRow("Template ID", self.template_id_edit)
        template_meta_form.addRow("Version", self.version_spin)

        right_layout.addLayout(template_meta_form)
        right_layout.addWidget(self.apply_profile_check)
        right_layout.addWidget(self.profile_combo)
        right_layout.addWidget(apply_btn)
        right_layout.addWidget(self.pending_changes_label)
        right_layout.addWidget(self.apply_pending_btn)
        right_layout.addWidget(history_btn)
        right_layout.addWidget(self.progress)
        right_layout.addWidget(QLabel("Résumé application"))
        right_layout.addWidget(self.apply_summary)
        right_layout.addWidget(QLabel("Historique"))
        right_layout.addWidget(self.history)
        right.setLayout(right_layout)

        split = QSplitter(Qt.Orientation.Horizontal)
        split.addWidget(left)
        split.addWidget(center)
        split.addWidget(right)
        split.setSizes([360, 720, 420])

        root = QVBoxLayout()
        banner = QHBoxLayout()
        banner.addWidget(self.workflow_label, 2)
        banner.addWidget(self.next_step_label, 2)
        banner.addWidget(self.next_step_btn)
        root.addLayout(banner)
        root.addWidget(split)
        self.setLayout(root)

        for kind in TemplateKind:
            label, _ = TEMPLATE_META[kind]
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, kind.value)
            self.template_list.addItem(item)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.state.dictionary_changed.connect(lambda _: self.refresh())

        if self.template_list.count() > 0:
            self.template_list.setCurrentRow(0)

        apply_theme_safe_styles(self)

    def _current_kind(self) -> TemplateKind:
        item = self.template_list.currentItem()
        if not item:
            return TemplateKind.WORDLIST_TOKENS
        return TemplateKind(str(item.data(Qt.ItemDataRole.UserRole)))

    def _current_source(self) -> Path | None:
        data = self.source_combo.currentData()
        if not data:
            return None
        return Path(str(data))

    def _source_suffix(self) -> str:
        source = self._current_source()
        if not source:
            return ""
        return source.suffix.lower()

    def _set_counts(
        self,
        records_count: int,
        entries_count: int,
        ignored_count: int,
        unrecognized_count: int,
        overridden_count: int,
    ) -> None:
        self.records_count_label.setText(f"Records analysés: {records_count}")
        self.entries_count_label.setText(f"Entrées produites: {entries_count}")
        self.ignored_count_label.setText(f"Ignorés: {ignored_count}")
        self.unrecognized_count_label.setText(f"Non reconnus: {unrecognized_count}")
        self.overridden_count_label.setText(f"Overridden: {overridden_count}")
        self.pending_changes_label.setText(f"Changements en attente: {overridden_count}")
        has_pending = overridden_count > 0
        self.apply_pending_btn.setVisible(has_pending)
        self.apply_pending_btn.setEnabled(has_pending)

    def _on_kind_changed(self, *_: object) -> None:
        kind = self._current_kind()
        label, summary = TEMPLATE_META[kind]
        self.template_summary.setText(f"{label}: {summary}")

        self.wordlist_panel.setVisible(kind == TemplateKind.WORDLIST_TOKENS)
        self.entrydef_panel.setVisible(kind == TemplateKind.ENTRY_PLUS_DEFINITION)
        self.headpron_panel.setVisible(kind == TemplateKind.HEADWORD_PLUS_PRON)
        self.fr_en_pron_panel.setVisible(kind == TemplateKind.FR_EN_PRON_THREE_COLS)
        self.csv_panel.setVisible(kind == TemplateKind.CSV_MAPPING)

        self.template_id_edit.setText(kind.value)
        self._sync_template_availability()

    def _refresh_profile_options(self) -> None:
        if not self.state.project_dir:
            self.profile_combo.clear()
            self.profile_combo.addItem("reading_v1")
            return

        rules_dir = self.state.project_dir / "rules"
        profiles = sorted(path.stem for path in rules_dir.glob("*.yml"))
        if "reading_v1" not in profiles:
            profiles.insert(0, "reading_v1")

        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        for profile_name in profiles:
            self.profile_combo.addItem(profile_name)
        active = self.state.active_profile or "reading_v1"
        idx = self.profile_combo.findText(active)
        if idx >= 0:
            self.profile_combo.setCurrentIndex(idx)
        else:
            self.profile_combo.setCurrentText(active)
        self.profile_combo.blockSignals(False)

    def _refresh_sources(self) -> None:
        self.source_combo.clear()
        self.source_type_label.setText("Type source: -")
        if not self.state.project_dir:
            return

        try:
            sources = list_template_sources(self.state.project_dir)
        except Exception:
            sources = []

        if not sources:
            self.source_combo.addItem("Aucune source importée", "")
            return

        for source in sources:
            try:
                display = str(source.relative_to(self.state.project_dir))
            except ValueError:
                display = str(source)
            self.source_combo.addItem(display, str(source))

        self._on_source_changed()

    def _on_source_changed(self) -> None:
        source = self._current_source()
        if not source:
            self.source_type_label.setText("Type source: -")
            self._sync_template_availability()
            return

        suffix = source.suffix.lower()
        source_type = "CSV" if suffix == ".csv" else "Texte"
        self.source_type_label.setText(f"Type source: {source_type}")

        if suffix == ".csv":
            self._refresh_csv_columns(source)
        self._sync_template_availability()

    def _sync_template_availability(self) -> None:
        suffix = self._source_suffix()
        enabled_kind = None

        for idx in range(self.template_list.count()):
            item = self.template_list.item(idx)
            kind = TemplateKind(str(item.data(Qt.ItemDataRole.UserRole)))
            enabled = False
            if suffix == ".csv":
                enabled = kind == TemplateKind.CSV_MAPPING
            elif suffix == ".txt":
                enabled = kind != TemplateKind.CSV_MAPPING

            flags = item.flags()
            if enabled:
                item.setFlags(flags | Qt.ItemFlag.ItemIsEnabled)
                if enabled_kind is None:
                    enabled_kind = idx
            else:
                item.setFlags(flags & ~Qt.ItemFlag.ItemIsEnabled)

        current = self.template_list.currentItem()
        if current and current.flags() & Qt.ItemFlag.ItemIsEnabled:
            return
        if enabled_kind is not None:
            self.template_list.setCurrentRow(enabled_kind)

    def _refresh_csv_columns(self, source: Path) -> None:
        try:
            records = load_source_records(source, limit=100)
        except TemplateEngineError:
            return

        columns = available_csv_columns(records)
        self.csv_headword_col.clear()
        self.csv_pron_col.clear()
        self.csv_definition_col.clear()

        self.csv_pron_col.addItem("(Aucune)", "")
        self.csv_definition_col.addItem("(Aucune)", "")

        for column in columns:
            self.csv_headword_col.addItem(column, column)
            self.csv_pron_col.addItem(column, column)
            self.csv_definition_col.addItem(column, column)

    def _collect_params(self) -> dict[str, object]:
        kind = self._current_kind()
        if kind == TemplateKind.WORDLIST_TOKENS:
            return {
                "trim_token_punctuation": self.wordlist_trim.isChecked(),
                "pron_from_headword": self.wordlist_pron_from_headword.isChecked(),
            }

        if kind == TemplateKind.ENTRY_PLUS_DEFINITION:
            return {
                "separator_mode": str(self.entrydef_separator.currentData() or "comma"),
                "custom_separator": self.entrydef_custom.text().strip(),
            }

        if kind == TemplateKind.HEADWORD_PLUS_PRON:
            return {
                "separator_mode": str(self.headpron_separator.currentData() or "tab"),
                "custom_separator": self.headpron_custom.text().strip(),
                "trim_punctuation": self.headpron_trim.isChecked(),
            }

        if kind == TemplateKind.FR_EN_PRON_THREE_COLS:
            return {
                "separator_mode": str(self.fr_en_pron_separator.currentData() or "auto"),
                "custom_separator": self.fr_en_pron_custom.text().strip(),
                "trim_punctuation": self.fr_en_pron_trim.isChecked(),
            }

        return {
            "headword_column": str(self.csv_headword_col.currentData() or ""),
            "pron_column": str(self.csv_pron_col.currentData() or ""),
            "definition_column": str(self.csv_definition_col.currentData() or ""),
            "split_headword": str(self.csv_split_mode.currentData() or "none"),
            "ignore_empty_headword": self.csv_ignore_empty.isChecked(),
        }

    @staticmethod
    def _set_combo_data(combo: QComboBox, value: str, fallback: str) -> None:
        idx = combo.findData(value)
        if idx < 0:
            idx = combo.findData(fallback)
        if idx >= 0:
            combo.setCurrentIndex(idx)

    def _set_template_kind(self, kind: TemplateKind) -> bool:
        for idx in range(self.template_list.count()):
            item = self.template_list.item(idx)
            if TemplateKind(str(item.data(Qt.ItemDataRole.UserRole))) != kind:
                continue
            if not item.flags() & Qt.ItemFlag.ItemIsEnabled:
                return False
            self.template_list.setCurrentRow(idx)
            return True
        return False

    def _selected_payloads_for_builder(self) -> list[dict[str, Any]]:
        payloads = self._selected_preview_payloads()
        selection_model = self.preview_table.selectionModel()
        selected_count = len(selection_model.selectedRows()) if selection_model is not None else 0
        if selected_count > 1:
            return payloads
        if self.visible_rows:
            return list(self.visible_rows[: min(30, len(self.visible_rows))])
        return payloads

    def _build_source_records_from_payloads(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[SourceRecord]:
        records: list[SourceRecord] = []
        seen: set[tuple[str, str]] = set()
        fallback_source_id = str(self._preview_source_id() or "")
        for idx, payload in enumerate(payloads, start=1):
            source_text = str(payload.get("source") or "").strip()
            if not source_text:
                continue

            source_id = str(payload.get("source_id") or fallback_source_id).strip() or "preview"
            record_key = str(payload.get("record_key") or f"preview-{idx}").strip()
            pair = (source_id, record_key)
            if pair in seen:
                continue
            seen.add(pair)

            source_path = str(payload.get("source_path") or source_id).strip() or source_id
            record_no = int(payload.get("record_no") or idx)
            records.append(
                SourceRecord(
                    source_id=source_id,
                    source_path=source_path,
                    record_key=record_key,
                    record_no=record_no,
                    source_type="text",
                    raw_text=source_text,
                )
            )
        return records

    @staticmethod
    def _recommend_template_for_records(
        records: list[SourceRecord],
    ) -> tuple[TemplateKind, dict[str, object], dict[str, int]] | None:
        if not records:
            return None

        candidates: list[tuple[TemplateKind, dict[str, object], int]] = [
            (
                TemplateKind.FR_EN_PRON_THREE_COLS,
                {"separator_mode": "auto", "custom_separator": "", "trim_punctuation": True},
                90,
            ),
            (
                TemplateKind.FR_EN_PRON_THREE_COLS,
                {"separator_mode": "triple_spaces", "custom_separator": "", "trim_punctuation": True},
                80,
            ),
            (
                TemplateKind.FR_EN_PRON_THREE_COLS,
                {"separator_mode": "tab", "custom_separator": "", "trim_punctuation": True},
                70,
            ),
            (
                TemplateKind.HEADWORD_PLUS_PRON,
                {"separator_mode": "multi_spaces", "custom_separator": "", "trim_punctuation": True},
                50,
            ),
            (
                TemplateKind.HEADWORD_PLUS_PRON,
                {"separator_mode": "tab", "custom_separator": "", "trim_punctuation": True},
                40,
            ),
            (
                TemplateKind.ENTRY_PLUS_DEFINITION,
                {"separator_mode": "comma", "custom_separator": ""},
                30,
            ),
            (
                TemplateKind.ENTRY_PLUS_DEFINITION,
                {"separator_mode": "double_space", "custom_separator": ""},
                20,
            ),
        ]

        best: tuple[TemplateKind, dict[str, object], dict[str, int]] | None = None
        best_score = -10_000

        for kind, params, priority in candidates:
            applied = apply_template_to_records(kind=kind, params=params, records=records)
            ok_record_keys = {
                row.record_key for row in applied.preview_rows if str(row.status or "").strip() == "OK"
            }
            unrecognized_keys = {
                row.record_key
                for row in applied.preview_rows
                if str(row.status or "").strip() == "Non reconnu"
            }
            ignored_keys = {
                row.record_key
                for row in applied.preview_rows
                if str(row.status or "").strip() == "Ignoré"
            }
            overflow_penalty = max(0, applied.entries_count - len(records) * 3)
            score = (
                len(ok_record_keys) * 20
                + applied.entries_count
                - len(unrecognized_keys) * 10
                - len(ignored_keys) * 4
                - overflow_penalty
                + priority
            )
            if score <= best_score:
                continue
            best_score = score
            best = (
                kind,
                params,
                {
                    "score": score,
                    "records": len(records),
                    "ok_records": len(ok_record_keys),
                    "entries": applied.entries_count,
                    "unrecognized": len(unrecognized_keys),
                },
            )

        return best

    def _apply_template_suggestion(self, kind: TemplateKind, params: dict[str, object]) -> bool:
        if not self._set_template_kind(kind):
            return False

        if kind == TemplateKind.ENTRY_PLUS_DEFINITION:
            self._set_combo_data(
                self.entrydef_separator,
                str(params.get("separator_mode") or "comma"),
                "comma",
            )
            self.entrydef_custom.setText(str(params.get("custom_separator") or ""))
        elif kind == TemplateKind.HEADWORD_PLUS_PRON:
            self._set_combo_data(
                self.headpron_separator,
                str(params.get("separator_mode") or "multi_spaces"),
                "multi_spaces",
            )
            self.headpron_custom.setText(str(params.get("custom_separator") or ""))
            self.headpron_trim.setChecked(bool(params.get("trim_punctuation", True)))
        elif kind == TemplateKind.FR_EN_PRON_THREE_COLS:
            self._set_combo_data(
                self.fr_en_pron_separator,
                str(params.get("separator_mode") or "auto"),
                "auto",
            )
            self.fr_en_pron_custom.setText(str(params.get("custom_separator") or ""))
            self.fr_en_pron_trim.setChecked(bool(params.get("trim_punctuation", True)))

        current_template_id = self.template_id_edit.text().strip()
        if not current_template_id or current_template_id == kind.value:
            self.template_id_edit.setText(f"custom_{kind.value}")
        return True

    def build_template_from_selection(self) -> None:
        payloads = self._selected_payloads_for_builder()
        if not payloads:
            QMessageBox.information(
                self,
                "Assistant gabarit",
                "Prévisualisez puis sélectionnez des lignes (ou laissez la vue active).",
            )
            return

        records = self._build_source_records_from_payloads(payloads)
        if not records:
            QMessageBox.information(
                self,
                "Assistant gabarit",
                "Impossible d'extraire des records exploitables depuis la sélection.",
            )
            return

        recommendation = self._recommend_template_for_records(records)
        if recommendation is None:
            QMessageBox.warning(
                self,
                "Assistant gabarit",
                "Aucune recommandation fiable n'a été trouvée.",
            )
            return

        kind, params, metrics = recommendation
        if not self._apply_template_suggestion(kind, params):
            QMessageBox.warning(
                self,
                "Assistant gabarit",
                "Le gabarit recommandé n'est pas disponible pour la source actuelle.",
            )
            return

        self.preview_template()
        label, _ = TEMPLATE_META[kind]
        QMessageBox.information(
            self,
            "Assistant gabarit",
            (
                f"Gabarit recommandé: {label}\n"
                f"Records OK: {metrics['ok_records']}/{metrics['records']}\n"
                f"Entrées: {metrics['entries']} | Non reconnus: {metrics['unrecognized']}"
            ),
        )

    def _require_project(self) -> bool:
        if not self.state.project_dir:
            QMessageBox.warning(self, "Projet requis", "Ouvrez d'abord un projet.")
            return False
        return True

    def refresh(self) -> None:
        self._refresh_profile_options()
        self._refresh_sources()
        self.refresh_history()
        self._update_next_step_state()

    def _preview_source_id(self) -> str | None:
        source = self._current_source()
        if not source:
            return None
        return str(source.expanduser().resolve())

    def preview_template(self) -> None:
        if not self._require_project():
            return

        source = self._current_source()
        if not source:
            QMessageBox.information(self, "Atelier", "Importez d'abord un fichier TXT/CSV.")
            return
        if not self.state.active_dict_id:
            QMessageBox.warning(self, "Corpus requis", "Sélectionnez d'abord un corpus actif.")
            return

        kind = self._current_kind()
        params = self._collect_params()

        try:
            preview = preview_template_on_source(
                project_dir=self.state.project_dir,
                source_path=source,
                kind=kind,
                params=params,
                corpus_id=self.state.active_dict_id,
                limit=self.preview_limit.value(),
            )
        except (TemplateEngineError, PipelineError, ValueError) as exc:
            QMessageBox.warning(self, "Prévisualisation impossible", str(exc))
            return

        self.preview_payload = preview
        self._render_preview(preview)

    def _rerender_preview(self) -> None:
        if self.preview_payload:
            self._render_preview(self.preview_payload)

    def _render_preview(self, payload: dict[str, object]) -> None:
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            rows = []

        self._set_counts(
            int(payload.get("records_count", 0)),
            int(payload.get("entries_count", 0)),
            int(payload.get("ignored_count", 0)),
            int(payload.get("unrecognized_count", 0)),
            int(payload.get("overridden_count", 0)),
        )

        self.preview_table.setRowCount(0)
        self.visible_rows = []
        diff_only = self.diff_only.isChecked()

        for row_payload in rows:
            if not isinstance(row_payload, dict):
                continue
            source = str(row_payload.get("source", ""))
            headword = str(row_payload.get("headword_raw", ""))
            pron = str(row_payload.get("pron_raw", ""))
            definition = str(row_payload.get("definition_raw", ""))
            status = str(row_payload.get("status", ""))
            reason = str(row_payload.get("reason", ""))
            override_op = str(row_payload.get("override_op") or "")

            if diff_only and status == "OK" and source.strip() == headword.strip() and not pron:
                continue

            self.visible_rows.append(row_payload)
            table_row = self.preview_table.rowCount()
            self.preview_table.insertRow(table_row)

            override_item = QTableWidgetItem("OVR" if override_op else "")
            override_item.setToolTip(override_op or "")
            source_item = QTableWidgetItem(source)
            headword_item = QTableWidgetItem(headword)
            pron_item = QTableWidgetItem(pron)
            definition_item = QTableWidgetItem(definition)
            status_item = QTableWidgetItem(status)
            reason_item = QTableWidgetItem(reason)

            self.preview_table.setItem(table_row, 0, override_item)
            self.preview_table.setItem(table_row, 1, source_item)
            self.preview_table.setItem(table_row, 2, headword_item)
            self.preview_table.setItem(table_row, 3, pron_item)
            self.preview_table.setItem(table_row, 4, definition_item)
            self.preview_table.setItem(table_row, 5, status_item)
            self.preview_table.setItem(table_row, 6, reason_item)

    def _selected_preview_payload(self) -> dict[str, Any] | None:
        row_idx = self.preview_table.currentRow()
        if row_idx < 0 or row_idx >= len(self.visible_rows):
            return None
        return self.visible_rows[row_idx]

    def _selected_preview_payloads(self) -> list[dict[str, Any]]:
        selected_rows: list[int] = []
        selection_model = self.preview_table.selectionModel()
        if selection_model is not None:
            selected_rows = sorted({index.row() for index in selection_model.selectedRows()})
        if not selected_rows:
            current_row = self.preview_table.currentRow()
            if current_row >= 0:
                selected_rows = [current_row]

        payloads: list[dict[str, Any]] = []
        for row_idx in selected_rows:
            if row_idx < 0 or row_idx >= len(self.visible_rows):
                continue
            payload = self.visible_rows[row_idx]
            if isinstance(payload, dict):
                payloads.append(payload)
        return payloads

    @staticmethod
    def _selected_record_refs(payloads: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
        refs: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str]] = set()
        for payload in payloads:
            source_id = str(payload.get("source_id") or "").strip()
            record_key = str(payload.get("record_key") or "").strip()
            if not source_id or not record_key:
                continue
            pair = (source_id, record_key)
            if pair in seen:
                continue
            seen.add(pair)
            refs.append((source_id, record_key, str(payload.get("source") or "")))
        return refs

    def _apply_skip_overrides_for_payloads(self, payloads: list[dict[str, Any]]) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        refs = self._selected_record_refs(payloads)
        if not refs:
            QMessageBox.information(self, "Override", "Aucune ligne source valide sélectionnée.")
            return

        for source_id, record_key, source_text in refs:
            upsert_override_record(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                source_id=source_id,
                record_key=record_key,
                op="SKIP_RECORD",
                before_json={"source": source_text},
                after_json={"action": "skip"},
            )

        self.preview_template()
        self.refresh_history()

    def _clear_overrides_for_payloads(self, payloads: list[dict[str, Any]]) -> None:
        if not self.state.store or not self.state.active_dict_id:
            return
        refs = self._selected_record_refs(payloads)
        if not refs:
            QMessageBox.information(self, "Override", "Aucune ligne source valide sélectionnée.")
            return

        for source_id, record_key, _ in refs:
            delete_override(
                store=self.state.store,
                corpus_id=self.state.active_dict_id,
                scope="record",
                source_id=source_id,
                record_key=record_key,
            )

        self.preview_template()
        self.refresh_history()

    def skip_selected_preview_records(self) -> None:
        payloads = self._selected_preview_payloads()
        if not payloads:
            QMessageBox.information(
                self,
                "Override",
                "Sélectionnez une ou plusieurs lignes dans la prévisualisation.",
            )
            return
        try:
            self._apply_skip_overrides_for_payloads(payloads)
        except OverrideError as exc:
            QMessageBox.warning(self, "Override", str(exc))

    def clear_selected_preview_overrides(self) -> None:
        payloads = self._selected_preview_payloads()
        if not payloads:
            QMessageBox.information(
                self,
                "Override",
                "Sélectionnez une ou plusieurs lignes dans la prévisualisation.",
            )
            return
        try:
            self._clear_overrides_for_payloads(payloads)
        except OverrideError as exc:
            QMessageBox.warning(self, "Override", str(exc))

    def _show_preview_context_menu(self, pos: QPoint) -> None:
        index = self.preview_table.indexAt(pos)
        if index.isValid():
            selection_model = self.preview_table.selectionModel()
            if (
                selection_model is None
                or not selection_model.isRowSelected(index.row(), self.preview_table.rootIndex())
            ):
                self.preview_table.selectRow(index.row())
        payloads = self._selected_preview_payloads()
        if not payloads or not self.state.store or not self.state.active_dict_id:
            return
        payload = payloads[0]

        selected_record_count = len(self._selected_record_refs(payloads))
        if selected_record_count <= 0:
            return
        source_id = str(payload.get("source_id") or "")
        record_key = str(payload.get("record_key") or "")
        if selected_record_count == 1 and (not source_id or not record_key):
            return

        menu = QMenu(self)
        if selected_record_count > 1:
            skip_action = menu.addAction(f"Ignorer la sélection ({selected_record_count} lignes)")
        else:
            skip_action = menu.addAction("Ignorer cette ligne")
        split_action = None
        edit_action = None
        create_action = None
        if selected_record_count == 1:
            split_action = menu.addAction("Scinder (tokens...)")
            edit_action = menu.addAction("Éditer les champs extraits")
            if str(payload.get("status") or "").strip() == "Non reconnu":
                menu.addSeparator()
                create_action = menu.addAction("Créer une entrée à partir de cette ligne…")
        menu.addSeparator()
        if selected_record_count > 1:
            undo_action = menu.addAction(
                f"Annuler override sélection ({selected_record_count} lignes)"
            )
        else:
            undo_action = menu.addAction("Annuler l'override")

        chosen = menu.exec(self.preview_table.viewport().mapToGlobal(pos))
        if not chosen:
            return

        try:
            if chosen == skip_action:
                self._apply_skip_overrides_for_payloads(payloads)
                return
            elif split_action is not None and chosen == split_action:
                default_seed = str(payload.get("source") or payload.get("headword_raw") or "")
                dialog = SplitRecordDialog(default_seed, parent=self)
                if dialog.exec() != QDialog.DialogCode.Accepted:
                    return
                entries = dialog.parsed_entries()
                if not entries:
                    QMessageBox.information(self, "Scinder", "Aucune entrée valide fournie.")
                    return
                upsert_override_record(
                    store=self.state.store,
                    corpus_id=self.state.active_dict_id,
                    source_id=source_id,
                    record_key=record_key,
                    op="SPLIT_RECORD",
                    before_json={"source": payload.get("source", "")},
                    after_json={"entries": entries},
                )
            elif edit_action is not None and chosen == edit_action:
                dialog = ExtractedEditDialog(
                    headword=str(payload.get("headword_raw") or ""),
                    pron=str(payload.get("pron_raw") or ""),
                    definition=str(payload.get("definition_raw") or ""),
                    parent=self,
                )
                if dialog.exec() != QDialog.DialogCode.Accepted:
                    return
                upsert_override_record(
                    store=self.state.store,
                    corpus_id=self.state.active_dict_id,
                    source_id=source_id,
                    record_key=record_key,
                    op="EDIT_RECORD",
                    before_json={"source": payload.get("source", "")},
                    after_json={
                        "headword_raw": dialog.headword_edit.text().strip(),
                        "pron_raw": dialog.pron_edit.text().strip(),
                        "definition_raw": dialog.definition_edit.text().strip(),
                    },
                )
            elif create_action is not None and chosen == create_action:
                create_dialog = CreateEntryFromRecordDialog(
                    source_text=str(payload.get("source") or ""),
                    headword_seed=str(payload.get("headword_raw") or ""),
                    pron_seed=str(payload.get("pron_raw") or ""),
                    definition_seed=str(payload.get("definition_raw") or ""),
                    parent=self,
                )
                if create_dialog.exec() != QDialog.DialogCode.Accepted:
                    return

                entries_to_create = create_dialog.entries_to_create()
                if not entries_to_create:
                    QMessageBox.information(
                        self, "Créer une entrée", "Aucune entrée valide à créer."
                    )
                    return

                created_ids: list[str] = []
                for item in entries_to_create:
                    entry_id = create_entry_from_record(
                        store=self.state.store,
                        corpus_id=self.state.active_dict_id,
                        source_id=source_id,
                        record_key=record_key,
                        headword_raw=str(item.get("headword_raw") or ""),
                        pron_raw=(str(item["pron_raw"]) if item.get("pron_raw") else None),
                        definition_raw=(
                            str(item["definition_raw"]) if item.get("definition_raw") else None
                        ),
                        note=create_dialog.note,
                        source_path=str(payload.get("source_path") or source_id),
                        source_record=str(payload.get("source") or ""),
                        line_no=int(payload.get("record_no") or 0),
                    )
                    created_ids.append(entry_id)

                self.state.notify_data_changed()
                self.preview_template()
                self.refresh_history()
                answer = QMessageBox.question(
                    self,
                    "Créer une entrée",
                    f"{len(created_ids)} entrée(s) créée(s). Ouvrir l'onglet Curation ?",
                )
                if answer == QMessageBox.StandardButton.Yes:
                    self._open_curation_tab()
                return
            elif chosen == undo_action:
                self._clear_overrides_for_payloads(payloads)
                return
        except OverrideError as exc:
            QMessageBox.warning(self, "Override", str(exc))
            return

        self.preview_template()
        self.refresh_history()

    def _on_apply_finished(self, summary: dict[str, object]) -> None:
        self.progress.hide()
        if isinstance(summary, dict) and summary.get("profile_summary"):
            profile_summary = summary.get("profile_summary")
            if isinstance(profile_summary, dict) and profile_summary.get("profile"):
                self.state.active_profile = str(profile_summary["profile"])

        self.apply_summary.setPlainText(json.dumps(summary, ensure_ascii=False, indent=2))
        self.state.notify_data_changed()
        self.refresh_history()
        QMessageBox.information(self, "Atelier", "Gabarit appliqué avec succès.")

    def _on_apply_failed(self, trace: str) -> None:
        self.progress.hide()
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        friendly = lines[-1] if lines else "Erreur inconnue"
        QMessageBox.critical(self, "Application gabarit", friendly)

    def apply_template(self) -> None:
        if not self._require_project():
            return
        if not self.state.active_dict_id:
            QMessageBox.warning(self, "Corpus requis", "Sélectionnez d'abord un corpus actif.")
            return

        source = self._current_source()
        if not source:
            QMessageBox.information(self, "Atelier", "Aucune source sélectionnée.")
            return

        existing_count = 0
        if self.state.store and self.state.active_dict_id:
            existing_count = self.state.store.count_entries(self.state.active_dict_id)

        if existing_count > 0:
            answer = QMessageBox.question(
                self,
                "Mode ajout",
                "Ce corpus contient déjà des entrées. L'atelier est en mode Ajouter seulement. Continuer ?",
            )
            if answer != QMessageBox.StandardButton.Yes:
                return

        kind = self._current_kind()
        params = self._collect_params()
        template_id = self.template_id_edit.text().strip() or kind.value
        version = self.version_spin.value()
        apply_profile = None
        if self.apply_profile_check.isChecked():
            apply_profile = self.profile_combo.currentText().strip() or self.state.active_profile

        spec = TemplateSpec(
            template_id=template_id,
            kind=kind,
            version=version,
            params=params,
        )

        self.progress.show()
        self.current_job = JobThread(
            apply_template_to_corpus,
            self.state.project_dir,
            self.state.active_dict_id,
            source,
            spec,
            apply_profile,
        )
        self.current_job.signals.finished.connect(self._on_apply_finished)
        self.current_job.signals.failed.connect(self._on_apply_failed)
        self.current_job.start()

    def refresh_history(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            self.history.clear()
            self.pending_changes_label.setText("Changements en attente: 0")
            self.apply_pending_btn.setVisible(False)
            return

        active = self.state.store.get_active_template(self.state.active_dict_id)
        history = self.state.store.list_template_applications(self.state.active_dict_id, limit=30)
        source_id = self._preview_source_id()
        overrides = list_overrides(
            store=self.state.store,
            corpus_id=self.state.active_dict_id,
            scope="record",
            source_id=source_id,
        )

        payload = {
            "active": dict(active) if active else None,
            "history": [dict(row) for row in history],
            "record_overrides": [
                {
                    "override_id": item.override_id,
                    "record_key": item.record_key,
                    "op": item.op,
                    "created_at": item.created_at,
                }
                for item in overrides
            ],
        }
        self.history.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))
        pending_count = len(overrides)
        self.pending_changes_label.setText(f"Changements en attente: {pending_count}")
        self.apply_pending_btn.setVisible(pending_count > 0)
        self.apply_pending_btn.setEnabled(pending_count > 0)

    def _update_next_step_state(self) -> None:
        if not self.state.store or not self.state.active_dict_id:
            self.next_step_label.setText(
                "Étape suivante recommandée: Ouvrir un projet puis importer."
            )
            self.next_step_btn.setEnabled(False)
            return

        entries_count = self.state.store.count_entries(self.state.active_dict_id)
        if entries_count > 0:
            self.next_step_label.setText("Étape suivante recommandée: Atelier Conventions.")
            self.next_step_btn.setEnabled(True)
            return

        self.next_step_label.setText("Étape suivante recommandée: Appliquer le gabarit au corpus.")
        self.next_step_btn.setEnabled(False)

    def open_conventions(self) -> None:
        self.state.request_conventions()

    def _open_curation_tab(self) -> None:
        main_window = self.window()
        tabs = getattr(main_window, "tabs", None)
        entries_tab = getattr(main_window, "entries_tab", None)
        if tabs is None or entries_tab is None:
            return
        index = tabs.indexOf(entries_tab)
        if index >= 0:
            tabs.setCurrentIndex(index)

    def show_history_dialog(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Historique des corrections")
        text = QTextEdit()
        text.setReadOnly(True)
        text.setPlainText(self.history.toPlainText())

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(dialog.reject)
        buttons.accepted.connect(dialog.accept)

        layout = QVBoxLayout()
        layout.addWidget(text)
        layout.addWidget(buttons)
        dialog.setLayout(layout)
        dialog.resize(900, 500)
        dialog.exec()
