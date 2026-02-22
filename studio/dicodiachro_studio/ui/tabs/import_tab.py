from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import unquote, urlparse

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QDragEnterEvent, QDropEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.importers.csv_import import import_csv_batch
from dicodiachro.core.importers.pdf_text_import import PDFTextImportError, import_pdf_text
from dicodiachro.core.importers.text_import import import_text_batch
from dicodiachro.core.importers.url_import import import_from_share_link
from dicodiachro.core.parsers.presets import discover_presets, load_parser_preset
from dicodiachro.core.pipeline import register_import_event, run_pipeline
from dicodiachro.core.storage.sqlite import init_project, project_paths

from ...services.drop_utils import classify_drop_paths
from ...services.jobs import JobThread
from ...services.os_open import open_directory, open_path, reveal_in_file_manager
from ...services.state import AppState
from ..dialogs.create_corpus_dialog import CreateCorpusDialog


class DropZoneFrame(QFrame):
    paths_dropped = Signal(list)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet(
            "QFrame {border: 2px dashed palette(mid); border-radius: 8px; background: palette(base);}"
        )
        layout = QVBoxLayout()
        label = QLabel("Déposez TXT / CSV / dossier / PDF / TIFF ici")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(label)
        self.setLayout(layout)

    def dragEnterEvent(self, event: QDragEnterEvent) -> None:
        mime = event.mimeData()
        if mime.hasUrls() and any(url.isLocalFile() for url in mime.urls()):
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent) -> None:
        paths = [Path(url.toLocalFile()) for url in event.mimeData().urls() if url.isLocalFile()]
        if paths:
            self.paths_dropped.emit(paths)
            event.acceptProposedAction()
        else:
            event.ignore()


class ImportTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.current_job: JobThread | None = None

        self.selected_text_file: Path | None = None
        self.selected_text_folder: Path | None = None
        self.selected_csv_file: Path | None = None
        self.selected_pdf_file: Path | None = None

        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("https://... (share link)")

        self.dict_combo = QComboBox()
        self.profile_combo = QComboBox()
        self.profile_combo.setEditable(True)
        self.parser_combo = QComboBox()

        self.project_label = QLabel("Projet: -")
        self.db_label = QLabel("Base: -")
        self.active_corpus_label = QLabel("Corpus actif: -")

        self.dict_combo.currentTextChanged.connect(self._on_dict_selected)
        self.profile_combo.currentTextChanged.connect(self._on_profile_selected)
        self.parser_combo.currentTextChanged.connect(self._on_parser_selected)

        self.text_file_edit = self._build_readonly_path_edit("Aucun fichier texte sélectionné")
        self.text_folder_edit = self._build_readonly_path_edit("Aucun dossier texte sélectionné")
        self.csv_file_edit = self._build_readonly_path_edit("Aucun fichier CSV sélectionné")
        self.pdf_file_edit = self._build_readonly_path_edit("Aucun PDF sélectionné")

        self.two_columns_check = QCheckBox("Double colonne")

        self.log = QTextEdit()
        self.log.setReadOnly(True)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        self.advanced_toggle_btn = QPushButton("Options avancées")
        self.advanced_toggle_btn.setCheckable(True)
        self.advanced_toggle_btn.setChecked(False)

        self.advanced_panel = QWidget()
        self.advanced_panel.setVisible(False)
        self.advanced_toggle_btn.toggled.connect(self.advanced_panel.setVisible)

        download_url_btn = QPushButton("Télécharger…")
        run_btn = QPushButton("Run pipeline")
        cancel_btn = QPushButton("Cancel")

        browse_text_file_btn = QPushButton("Parcourir…")
        import_text_file_btn = QPushButton("Importer TXT")
        reveal_text_file_btn = QPushButton("Révéler")
        open_text_file_btn = QPushButton("Ouvrir dossier")

        browse_text_folder_btn = QPushButton("Parcourir…")
        import_text_folder_btn = QPushButton("Importer dossier TXT")
        reveal_text_folder_btn = QPushButton("Révéler")
        open_text_folder_btn = QPushButton("Ouvrir")

        browse_csv_btn = QPushButton("Parcourir…")
        import_csv_btn = QPushButton("Importer CSV")
        reveal_csv_btn = QPushButton("Révéler")
        open_csv_btn = QPushButton("Ouvrir dossier")

        browse_pdf_btn = QPushButton("Parcourir…")
        import_pdf_btn = QPushButton("Importer PDF texte")
        reveal_pdf_btn = QPushButton("Révéler")
        open_pdf_btn = QPushButton("Ouvrir dossier")

        manage_corpus_btn = QPushButton("Gérer les corpus…")
        manage_corpus_adv_btn = QPushButton("Gérer les corpus…")
        rename_corpus_btn = QPushButton("Renommer corpus…")
        open_project_folder_btn = QPushButton("Ouvrir dossier projet")

        download_url_btn.clicked.connect(self.import_url)
        run_btn.clicked.connect(self.run_pipeline)
        cancel_btn.clicked.connect(self.cancel_job)

        browse_text_file_btn.clicked.connect(self.browse_text_file)
        import_text_file_btn.clicked.connect(self.import_selected_text_file)
        reveal_text_file_btn.clicked.connect(
            lambda: self._reveal_selected_path(self.selected_text_file)
        )
        open_text_file_btn.clicked.connect(
            lambda: self._open_selected_directory(self.selected_text_file)
        )

        browse_text_folder_btn.clicked.connect(self.browse_text_folder)
        import_text_folder_btn.clicked.connect(self.import_selected_text_folder)
        reveal_text_folder_btn.clicked.connect(
            lambda: self._reveal_selected_path(self.selected_text_folder)
        )
        open_text_folder_btn.clicked.connect(
            lambda: self._open_selected_path(self.selected_text_folder)
        )

        browse_csv_btn.clicked.connect(self.browse_csv_file)
        import_csv_btn.clicked.connect(self.import_selected_csv)
        reveal_csv_btn.clicked.connect(lambda: self._reveal_selected_path(self.selected_csv_file))
        open_csv_btn.clicked.connect(lambda: self._open_selected_directory(self.selected_csv_file))

        browse_pdf_btn.clicked.connect(self.browse_pdf_file)
        import_pdf_btn.clicked.connect(self.import_selected_pdf)
        reveal_pdf_btn.clicked.connect(lambda: self._reveal_selected_path(self.selected_pdf_file))
        open_pdf_btn.clicked.connect(lambda: self._open_selected_directory(self.selected_pdf_file))

        manage_corpus_btn.clicked.connect(self.manage_corpora)
        manage_corpus_adv_btn.clicked.connect(self.manage_corpora)
        rename_corpus_btn.clicked.connect(self.rename_active_corpus)
        open_project_folder_btn.clicked.connect(self.open_project_folder)

        project_bar = QHBoxLayout()
        project_bar.addWidget(self.project_label, 2)
        project_bar.addWidget(self.db_label, 2)
        project_bar.addWidget(self.active_corpus_label, 2)
        project_bar.addStretch(1)
        project_bar.addWidget(manage_corpus_btn)
        project_bar.addWidget(rename_corpus_btn)
        project_bar.addWidget(open_project_folder_btn)

        url_row = QHBoxLayout()
        url_row.addWidget(QLabel("Importer depuis URL"))
        url_row.addWidget(self.url_edit)
        url_row.addWidget(download_url_btn)

        source_grid = QGridLayout()
        source_grid.addWidget(QLabel("Importer TXT (.txt)"), 0, 0)
        source_grid.addWidget(self.text_file_edit, 0, 1)
        source_grid.addWidget(browse_text_file_btn, 0, 2)
        source_grid.addWidget(import_text_file_btn, 0, 3)
        source_grid.addWidget(reveal_text_file_btn, 0, 4)
        source_grid.addWidget(open_text_file_btn, 0, 5)

        source_grid.addWidget(QLabel("Importer dossier TXT"), 1, 0)
        source_grid.addWidget(self.text_folder_edit, 1, 1)
        source_grid.addWidget(browse_text_folder_btn, 1, 2)
        source_grid.addWidget(import_text_folder_btn, 1, 3)
        source_grid.addWidget(reveal_text_folder_btn, 1, 4)
        source_grid.addWidget(open_text_folder_btn, 1, 5)

        source_grid.addWidget(QLabel("Importer CSV"), 2, 0)
        source_grid.addWidget(self.csv_file_edit, 2, 1)
        source_grid.addWidget(browse_csv_btn, 2, 2)
        source_grid.addWidget(import_csv_btn, 2, 3)
        source_grid.addWidget(reveal_csv_btn, 2, 4)
        source_grid.addWidget(open_csv_btn, 2, 5)

        source_grid.addWidget(QLabel("Importer PDF (ABBYY texte)"), 3, 0)
        source_grid.addWidget(self.pdf_file_edit, 3, 1)
        source_grid.addWidget(browse_pdf_btn, 3, 2)
        source_grid.addWidget(import_pdf_btn, 3, 3)
        source_grid.addWidget(reveal_pdf_btn, 3, 4)
        source_grid.addWidget(open_pdf_btn, 3, 5)
        source_grid.addWidget(self.two_columns_check, 3, 6)
        source_grid.setColumnStretch(1, 1)

        advanced_grid = QGridLayout()
        advanced_grid.addWidget(QLabel("Corpus actif"), 0, 0)
        advanced_grid.addWidget(self.dict_combo, 0, 1)
        advanced_grid.addWidget(manage_corpus_adv_btn, 0, 2)

        advanced_grid.addWidget(QLabel("Convention"), 1, 0)
        advanced_grid.addWidget(self.profile_combo, 1, 1, 1, 2)

        advanced_grid.addWidget(QLabel("Preset parser"), 2, 0)
        advanced_grid.addWidget(self.parser_combo, 2, 1, 1, 2)

        self.advanced_panel.setLayout(advanced_grid)

        self.drop_zone = DropZoneFrame(self)
        self.drop_zone.paths_dropped.connect(self._handle_drop_paths)

        actions = QHBoxLayout()
        actions.addWidget(run_btn)
        actions.addWidget(cancel_btn)

        layout = QVBoxLayout()
        layout.addLayout(project_bar)
        layout.addWidget(
            QLabel("Import local/URL sans saisie de chemin: utilisez Parcourir ou glisser-déposer")
        )
        layout.addLayout(url_row)
        layout.addLayout(source_grid)
        layout.addWidget(self.drop_zone)
        layout.addWidget(self.advanced_toggle_btn)
        layout.addWidget(self.advanced_panel)
        layout.addLayout(actions)
        layout.addWidget(self.progress)
        layout.addWidget(self.log)
        self.setLayout(layout)

        self.state.project_changed.connect(self._refresh_selectors)
        self.state.data_changed.connect(self._refresh_selectors)
        self.state.dictionary_changed.connect(lambda _: self._refresh_selectors())
        self._refresh_project_bar()

    @staticmethod
    def _build_readonly_path_edit(placeholder: str) -> QLineEdit:
        edit = QLineEdit()
        edit.setReadOnly(True)
        edit.setPlaceholderText(placeholder)
        return edit

    def _require_project(self) -> bool:
        if not self.state.project_dir:
            QMessageBox.warning(self, "Projet requis", "Ouvrez d'abord un projet.")
            return False
        return True

    def _append(self, text: str) -> None:
        self.log.append(text)

    def _refresh_project_bar(self) -> None:
        if not self.state.project_dir:
            self.project_label.setText("Projet: -")
            self.db_label.setText("Base: -")
            self.active_corpus_label.setText("Corpus actif: -")
            return

        db_path = project_paths(self.state.project_dir).db_path
        self.project_label.setText(f"Projet: {self.state.project_dir}")
        self.db_label.setText(f"Base: {db_path.name}")

        active = self.state.active_dict_id or ""
        if self.state.store and active:
            rows = self.state.list_dictionaries()
            label = active
            for row in rows:
                if str(row["dict_id"]) == active:
                    maybe_label = str(row["label"] or "").strip()
                    if maybe_label and maybe_label != active:
                        label = f"{maybe_label} [{active}]"
                    break
            self.active_corpus_label.setText(f"Corpus actif: {label}")
        else:
            self.active_corpus_label.setText("Corpus actif: -")

    def _selected_dict_id(self) -> str:
        active = self.state.active_dict_id or ""
        if active:
            return active
        data = self.dict_combo.currentData()
        return str(data or "").strip()

    def _selected_profile(self) -> str:
        text = self.profile_combo.currentText().strip()
        if text:
            return text
        return self.state.active_profile or "reading_v1"

    def _selected_parser(self) -> str | None:
        current_data = self.parser_combo.currentData()
        parser_name = str(current_data or self.state.active_parser or "").strip()
        if not parser_name or parser_name.lower() == "auto":
            return None
        return parser_name

    def _on_dict_selected(self, _: str) -> None:
        dict_id = str(self.dict_combo.currentData() or "").strip()
        if dict_id:
            self.state.set_active_dict(dict_id)

    def _on_profile_selected(self, profile_name: str) -> None:
        cleaned = profile_name.strip()
        if cleaned:
            self.state.active_profile = cleaned

    def _on_parser_selected(self, _: str) -> None:
        current_data = self.parser_combo.currentData()
        self.state.active_parser = str(current_data or "auto")

    @staticmethod
    def _corpus_display_label(row) -> str:
        label = str(row["label"] or "").strip()
        dict_id = str(row["dict_id"])
        if label and label != dict_id:
            return f"{label} [{dict_id}]"
        return label or dict_id

    def _refresh_selectors(self) -> None:
        self._refresh_project_bar()
        if not self.state.project_dir:
            return

        self.state.refresh_active_dict()
        dictionaries = self.state.list_dictionaries()
        active_dict = self.state.active_dict_id or ""

        self.dict_combo.blockSignals(True)
        self.dict_combo.clear()
        if dictionaries:
            for row in dictionaries:
                self.dict_combo.addItem(self._corpus_display_label(row), row["dict_id"])
            index = self.dict_combo.findData(active_dict)
            if index >= 0:
                self.dict_combo.setCurrentIndex(index)
        else:
            self.dict_combo.addItem("Aucun corpus", "")
        self.dict_combo.blockSignals(False)

        rules_dir = project_paths(self.state.project_dir).rules_dir
        profiles = sorted(path.stem for path in rules_dir.glob("*.yml"))
        if "reading_v1" not in profiles:
            profiles.insert(0, "reading_v1")

        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        for profile_name in profiles:
            self.profile_combo.addItem(profile_name)
        active_profile = self.state.active_profile or "reading_v1"
        index = self.profile_combo.findText(active_profile)
        if index >= 0:
            self.profile_combo.setCurrentIndex(index)
        else:
            self.profile_combo.setCurrentText(active_profile)
        self.profile_combo.blockSignals(False)

        parser_options: list[tuple[str, str]] = [("Auto (par corpus)", "auto")]
        for preset_path in discover_presets(rules_dir, dict_id=active_dict or None):
            try:
                spec = load_parser_preset(preset_path)
            except Exception:  # pragma: no cover - GUI convenience guard
                continue
            parser_options.append((f"{spec.parser_id} (v{spec.version})", str(preset_path)))

        self.parser_combo.blockSignals(True)
        self.parser_combo.clear()
        for label, value in parser_options:
            self.parser_combo.addItem(label, value)

        active_parser = self.state.active_parser or "auto"
        parser_index = 0
        for idx in range(self.parser_combo.count()):
            if str(self.parser_combo.itemData(idx) or "") == active_parser:
                parser_index = idx
                break
        self.parser_combo.setCurrentIndex(parser_index)
        self.parser_combo.blockSignals(False)
        self._refresh_project_bar()

    def _start_job(self, fn, *args):
        if self.current_job and self.current_job.isRunning():
            QMessageBox.information(self, "Busy", "A job is already running.")
            return
        self.current_job = JobThread(fn, *args)
        self.current_job.signals.finished.connect(self._on_job_finished)
        self.current_job.signals.failed.connect(self._on_job_failed)
        self.progress.show()
        self.current_job.start()

    def _on_job_finished(self, result):
        self.progress.hide()
        if isinstance(result, dict):
            self._append(json.dumps(result, ensure_ascii=False, indent=2))
            if result.get("dict_id"):
                self.state.set_active_dict(str(result["dict_id"]))
            if result.get("profile"):
                self.state.active_profile = str(result["profile"])
            parser_value = result.get("parser")
            if isinstance(parser_value, str):
                self.state.active_parser = parser_value
            elif isinstance(parser_value, dict):
                self.state.active_parser = str(parser_value.get("parser_path") or "auto")
        else:
            self._append(f"Done: {result}")
        self.state.notify_data_changed()

    def _on_job_failed(self, error_text: str):
        self.progress.hide()
        if "PDF_NO_TEXT_LAYER" in error_text:
            friendly = "PDF sans couche texte. Passez-le par ABBYY (ou un OCR) puis réessayez."
        else:
            lines = [line.strip() for line in error_text.splitlines() if line.strip()]
            friendly = lines[-1] if lines else "Unknown import error"
        self._append(friendly)
        QMessageBox.critical(self, "Job failed", friendly)

    def cancel_job(self):
        if self.current_job and self.current_job.isRunning():
            self.current_job.cancel()
            self.current_job.terminate()
            self.progress.hide()
            self._append("Job cancelled (best effort).")

    def _open_selected_path(self, path: Path | None) -> None:
        if not path:
            QMessageBox.information(self, "Path", "Aucun chemin sélectionné.")
            return
        if not open_path(path):
            QMessageBox.warning(self, "Open", f"Impossible d'ouvrir: {path}")

    def _open_selected_directory(self, path: Path | None) -> None:
        if not path:
            QMessageBox.information(self, "Path", "Aucun chemin sélectionné.")
            return
        if not open_directory(path):
            QMessageBox.warning(self, "Open", f"Impossible d'ouvrir le dossier: {path}")

    def _reveal_selected_path(self, path: Path | None) -> None:
        if not path:
            QMessageBox.information(self, "Path", "Aucun chemin sélectionné.")
            return
        if not reveal_in_file_manager(path):
            QMessageBox.warning(self, "Reveal", f"Impossible de révéler: {path}")

    def browse_text_file(self) -> None:
        if not self._require_project():
            return
        selected_file, _ = QFileDialog.getOpenFileName(
            self,
            "Sélectionner un fichier texte",
            str(self.state.project_dir),
            "Text (*.txt)",
        )
        if not selected_file:
            return
        self.selected_text_file = Path(selected_file)
        self.text_file_edit.setText(str(self.selected_text_file))

    def browse_text_folder(self) -> None:
        if not self._require_project():
            return
        selected_dir = QFileDialog.getExistingDirectory(
            self,
            "Sélectionner un dossier de textes",
            str(self.state.project_dir),
        )
        if not selected_dir:
            return
        self.selected_text_folder = Path(selected_dir)
        self.text_folder_edit.setText(str(self.selected_text_folder))

    def browse_csv_file(self) -> None:
        if not self._require_project():
            return
        selected_file, _ = QFileDialog.getOpenFileName(
            self,
            "Sélectionner un fichier CSV",
            str(self.state.project_dir),
            "CSV (*.csv)",
        )
        if not selected_file:
            return
        self.selected_csv_file = Path(selected_file)
        self.csv_file_edit.setText(str(self.selected_csv_file))

    def browse_pdf_file(self) -> None:
        if not self._require_project():
            return
        selected_file, _ = QFileDialog.getOpenFileName(
            self,
            "Sélectionner un PDF texte (ABBYY)",
            str(self.state.project_dir),
            "PDF (*.pdf)",
        )
        if not selected_file:
            return
        self.selected_pdf_file = Path(selected_file)
        self.pdf_file_edit.setText(str(self.selected_pdf_file))

    @staticmethod
    def _suggested_corpus_name(source: Path | str | None) -> str:
        if isinstance(source, Path):
            if source.is_dir():
                return source.name or "Corpus"
            return source.stem or source.name or "Corpus"

        text = str(source or "").strip()
        if not text:
            return "Corpus"

        if text.startswith(("http://", "https://")):
            parsed = urlparse(text)
            path_name = Path(unquote(parsed.path or "")).stem
            return (path_name or parsed.netloc or "Corpus").strip() or "Corpus"

        path = Path(text)
        return (path.stem or path.name or "Corpus").strip() or "Corpus"

    def _prompt_create_corpus(self, suggested_name: str) -> tuple[str, bool] | None:
        dialog = CreateCorpusDialog(suggested_name, parent=self)
        if dialog.exec() != dialog.DialogCode.Accepted:
            return None
        name = dialog.corpus_name or suggested_name or "Corpus"
        return name, dialog.use_for_next_imports

    def _ensure_corpus_or_prompt(self, suggested_name: str) -> bool:
        if not self._require_project():
            return False
        if not self.state.store:
            QMessageBox.warning(self, "Projet requis", "Impossible d'accéder à la base projet.")
            return False

        dictionaries = self.state.list_dictionaries()
        active = self.state.refresh_active_dict()
        if active and any(row["dict_id"] == active for row in dictionaries):
            return True

        self._append("Aucun corpus n'existe encore. Créons-en un pour cette source.")
        created = self._prompt_create_corpus(suggested_name)
        if created is None:
            self._append("Import annulé: création de corpus annulée.")
            return False

        corpus_name, use_for_next = created
        dict_id = self.state.store.create_dictionary(corpus_name)
        if use_for_next or not self.state.active_dict_id:
            self.state.set_active_dict(dict_id)
        self._append("Le corpus a été créé et sélectionné automatiquement.")
        self.state.notify_data_changed()
        return True

    def _create_new_corpus_interactive(self, suggested_name: str = "Corpus") -> str | None:
        if not self._require_project() or not self.state.store:
            return None
        created = self._prompt_create_corpus(suggested_name)
        if created is None:
            return None

        corpus_name, use_for_next = created
        dict_id = self.state.store.create_dictionary(corpus_name)
        if use_for_next or not self.state.active_dict_id:
            self.state.set_active_dict(dict_id)
        self._append("Le corpus a été créé et sélectionné automatiquement.")
        self.state.notify_data_changed()
        return dict_id

    def manage_corpora(self) -> None:
        if not self._require_project() or not self.state.store:
            return

        rows = self.state.list_dictionaries()
        if not rows:
            self._create_new_corpus_interactive("Corpus")
            return

        labels = [self._corpus_display_label(row) for row in rows]
        options = [*labels, "Créer un nouveau corpus…"]

        current_idx = 0
        active = self.state.active_dict_id or ""
        for idx, row in enumerate(rows):
            if row["dict_id"] == active:
                current_idx = idx
                break

        selected, ok = QInputDialog.getItem(
            self,
            "Gérer les corpus",
            "Sélectionnez le corpus actif",
            options,
            current_idx,
            False,
        )
        if not ok or not selected:
            return

        if selected == "Créer un nouveau corpus…":
            self._create_new_corpus_interactive("Corpus")
            return

        for idx, label in enumerate(labels):
            if selected == label:
                self.state.set_active_dict(str(rows[idx]["dict_id"]))
                return

    def rename_active_corpus(self) -> None:
        if not self._require_project() or not self.state.store:
            return
        active = self._selected_dict_id()
        if not active:
            QMessageBox.information(self, "Corpus", "Aucun corpus actif à renommer.")
            return

        rows = self.state.list_dictionaries()
        current_label = active
        for row in rows:
            if str(row["dict_id"]) == active:
                current_label = str(row["label"] or active)
                break

        new_label, ok = QInputDialog.getText(
            self,
            "Renommer corpus",
            "Nouveau nom du corpus",
            text=current_label,
        )
        if not ok:
            return
        cleaned = new_label.strip()
        if not cleaned:
            QMessageBox.warning(self, "Renommer corpus", "Le nom ne peut pas être vide.")
            return
        self.state.store.rename_dictionary_label(active, cleaned)
        self.state.notify_data_changed()
        self._append(f"Corpus renommé: {cleaned} [{active}]")

    def open_project_folder(self) -> None:
        if not self._require_project():
            return
        assert self.state.project_dir is not None
        if not open_directory(self.state.project_dir):
            QMessageBox.warning(self, "Projet", "Impossible d'ouvrir le dossier projet.")

    def _import_text_path(self, input_path: Path) -> None:
        if not self._require_project():
            return
        if not self._ensure_corpus_or_prompt(self._suggested_corpus_name(input_path)):
            return

        project_dir = self.state.project_dir
        assert project_dir is not None

        def _job() -> dict[str, object]:
            paths = init_project(project_dir)
            imported = import_text_batch(paths.raw_dir / "imports", input_path)
            register_import_event(
                project_dir,
                {
                    "type": "text",
                    "input_path": str(input_path),
                    "dict_id": self._selected_dict_id(),
                    "imported": [str(path) for path in imported],
                },
            )
            return {
                "action": "import_text",
                "dict_id": self._selected_dict_id(),
                "input_path": str(input_path),
                "imported": len(imported),
            }

        self._start_job(_job)

    def _import_pdf_path(self, pdf_path: Path) -> None:
        if not self._require_project():
            return
        if not self._ensure_corpus_or_prompt(self._suggested_corpus_name(pdf_path)):
            return

        project_dir = self.state.project_dir
        assert project_dir is not None
        two_columns = self.two_columns_check.isChecked()

        def _job() -> dict[str, object]:
            try:
                imported = import_pdf_text(
                    project_dir=project_dir,
                    pdf_path=pdf_path,
                    two_columns=two_columns,
                )
            except PDFTextImportError as exc:
                raise RuntimeError(f"{exc.code}: {exc}") from exc

            register_import_event(
                project_dir,
                {
                    "type": "pdf_text",
                    "dict_id": self._selected_dict_id(),
                    "pdf_path": str(pdf_path),
                    "two_columns": two_columns,
                    **imported.as_dict(),
                },
            )
            return {
                "action": "import_pdf_text",
                "dict_id": self._selected_dict_id(),
                "pdf_path": str(pdf_path),
                "two_columns": two_columns,
                "output_text_paths": [str(path) for path in imported.output_text_paths],
                "pages_total": imported.pages_total,
                "pages_with_text": imported.pages_with_text,
            }

        self._start_job(_job)

    def _import_csv_path(self, input_path: Path) -> None:
        if not self._require_project():
            return
        if not self._ensure_corpus_or_prompt(self._suggested_corpus_name(input_path)):
            return

        project_dir = self.state.project_dir
        assert project_dir is not None

        def _job() -> dict[str, object]:
            paths = init_project(project_dir)
            imported = import_csv_batch(paths.raw_dir / "imports", input_path)
            register_import_event(
                project_dir,
                {
                    "type": "csv",
                    "input_path": str(input_path),
                    "dict_id": self._selected_dict_id(),
                    "imported": [str(path) for path in imported],
                },
            )
            return {
                "action": "import_csv",
                "dict_id": self._selected_dict_id(),
                "input_path": str(input_path),
                "imported": len(imported),
            }

        self._start_job(_job)

    def import_selected_text_file(self) -> None:
        if not self.selected_text_file:
            QMessageBox.information(self, "Text", "Sélectionnez un fichier .txt.")
            return
        self._import_text_path(self.selected_text_file)

    def import_selected_text_folder(self) -> None:
        if not self.selected_text_folder:
            QMessageBox.information(self, "Text", "Sélectionnez un dossier texte.")
            return
        self._import_text_path(self.selected_text_folder)

    def import_selected_pdf(self) -> None:
        if not self.selected_pdf_file:
            QMessageBox.information(self, "PDF", "Sélectionnez un PDF texte.")
            return
        self._import_pdf_path(self.selected_pdf_file)

    def import_selected_csv(self) -> None:
        if not self.selected_csv_file:
            QMessageBox.information(self, "CSV", "Sélectionnez un fichier CSV.")
            return
        self._import_csv_path(self.selected_csv_file)

    def _ask_drop_action(self, title: str, message: str, action_label: str) -> bool:
        dialog = QMessageBox(self)
        dialog.setWindowTitle(title)
        dialog.setText(message)
        action_btn = dialog.addButton(action_label, QMessageBox.ButtonRole.AcceptRole)
        dialog.addButton(QMessageBox.StandardButton.Cancel)
        dialog.exec()
        return dialog.clickedButton() == action_btn

    def _handle_drop_paths(self, dropped_paths: list[Path]) -> None:
        if not self._require_project():
            return

        classified = classify_drop_paths(dropped_paths)
        if not classified.has_supported and not classified.zip_files:
            QMessageBox.information(
                self,
                "Glisser-déposer",
                "Aucun type supporté détecté (TXT/CSV/dossier/PDF/TIFF).",
            )
            return

        if classified.pdf_files:
            pdf_path = classified.pdf_files[0]
            self.selected_pdf_file = pdf_path
            self.pdf_file_edit.setText(str(pdf_path))
            if self._ask_drop_action(
                "PDF détecté",
                f"Importer {pdf_path.name} comme PDF texte ABBYY ?",
                "Importer PDF texte",
            ):
                self.import_selected_pdf()
            return

        if classified.image_files:
            QMessageBox.information(
                self,
                "OCR externe requis",
                "Ce format nécessite OCR externe (ABBYY) - exportez en PDF texte puis importez.",
            )
            return

        if classified.text_files:
            text_path = classified.text_files[0]
            self.selected_text_file = text_path
            self.text_file_edit.setText(str(text_path))
            if self._ask_drop_action(
                "Fichier texte détecté",
                f"Importer {text_path.name} dans le projet ?",
                "Importer texte",
            ):
                self.import_selected_text_file()
            return

        if classified.csv_files:
            csv_path = classified.csv_files[0]
            self.selected_csv_file = csv_path
            self.csv_file_edit.setText(str(csv_path))
            if self._ask_drop_action(
                "Fichier CSV détecté",
                f"Importer {csv_path.name} dans le projet ?",
                "Importer CSV",
            ):
                self.import_selected_csv()
            return

        if classified.directories:
            folder_path = classified.directories[0]
            self.selected_text_folder = folder_path
            self.text_folder_edit.setText(str(folder_path))
            if self._ask_drop_action(
                "Dossier détecté",
                f"Importer les fichiers texte de {folder_path.name} ?",
                "Importer dossier",
            ):
                self.import_selected_text_folder()
            return

        if classified.zip_files:
            QMessageBox.information(
                self,
                "ZIP détecté",
                "Les ZIP OCR ne sont plus importés directement. Utilisez un PDF texte ABBYY.",
            )
            return

        QMessageBox.information(
            self,
            "Glisser-déposer",
            "Types détectés non pris en charge pour une action automatique.",
        )

    def import_url(self) -> None:
        if not self._require_project():
            return
        url = self.url_edit.text().strip()
        if not url:
            QMessageBox.information(self, "URL", "Entrez une URL de partage.")
            return
        if not self._ensure_corpus_or_prompt(self._suggested_corpus_name(url)):
            return

        project_dir = self.state.project_dir
        assert project_dir is not None
        paths = project_paths(project_dir)
        imports_dir = paths.raw_dir / "imports"
        extract_dir = imports_dir / "unzipped"

        self._append(f"Téléchargement URL: {url}")
        self._append(f"Stockage local: {imports_dir} (extraction ZIP: {extract_dir})")

        def _job() -> dict[str, object]:
            imported, metadata = import_from_share_link(
                url=url,
                imports_dir=imports_dir,
                extract_dir=extract_dir,
            )
            register_import_event(
                project_dir,
                {
                    "type": "url",
                    "dict_id": self._selected_dict_id(),
                    **metadata,
                    "imported_files": [str(path) for path in imported],
                },
            )
            return {
                "action": "import_url",
                "dict_id": self._selected_dict_id(),
                "imported": len(imported),
                "imports_dir": str(imports_dir),
                "extract_dir": str(extract_dir),
            }

        self._start_job(_job)

    def run_pipeline(self) -> None:
        if not self._require_project():
            return
        if not self._ensure_corpus_or_prompt("Corpus"):
            return

        dict_id = self._selected_dict_id()
        if not dict_id:
            QMessageBox.warning(self, "Corpus requis", "Sélectionnez un corpus actif.")
            return

        profile = self._selected_profile() or "reading_v1"
        parser_name = self._selected_parser()
        project_dir = self.state.project_dir
        assert project_dir is not None

        def _job() -> dict[str, object]:
            return run_pipeline(
                project_dir=project_dir,
                dict_id=dict_id,
                profile_name=profile,
                source_paths=None,
                clear_existing=True,
                parser_name=parser_name,
            )

        self._start_job(_job)
