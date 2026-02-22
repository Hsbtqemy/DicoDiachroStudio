from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QMainWindow,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ..services.os_open import open_directory
from ..services.state import AppState
from .tabs.compare_tab import CompareTab
from .tabs.conventions_tab import ConventionsTab
from .tabs.diagnostics_tab import DiagnosticsTab
from .tabs.entries_tab import EntriesTab
from .tabs.export_tab import ExportTab
from .tabs.import_tab import ImportTab
from .tabs.templates_tab import TemplatesTab


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("DicoDiachro Studio")
        self.resize(1400, 900)

        self.state = AppState()

        self.import_tab = ImportTab(self.state)
        self.templates_tab = TemplatesTab(self.state)
        self.entries_tab = EntriesTab(self.state)
        self.conventions_tab = ConventionsTab(self.state)
        self.compare_tab = CompareTab(self.state)
        self.export_tab = ExportTab(self.state)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.import_tab, "Import")
        self.tabs.addTab(self.templates_tab, "Atelier gabarits")
        self.tabs.addTab(self.entries_tab, "Curation")
        self.tabs.addTab(self.conventions_tab, "Conventions")
        self.tabs.addTab(self.compare_tab, "Comparer")
        self.tabs.addTab(self.export_tab, "Export")

        self.import_tab_index = self.tabs.indexOf(self.import_tab)
        self.templates_tab_index = self.tabs.indexOf(self.templates_tab)
        self.conventions_tab_index = self.tabs.indexOf(self.conventions_tab)
        self.compare_tab_index = self.tabs.indexOf(self.compare_tab)
        self.export_tab_index = self.tabs.indexOf(self.export_tab)

        self.state.compare_requested.connect(self._open_compare_tab)
        self.state.conventions_requested.connect(self._open_conventions_tab)
        self.state.export_requested.connect(self._open_export_tab)

        self.setCentralWidget(self.tabs)
        self._build_menus()

    def _build_menus(self) -> None:
        menu_bar = self.menuBar()

        file_menu = menu_bar.addMenu("Fichier")
        file_menu.addAction("Nouveau projet", self.new_project_dialog)
        file_menu.addAction("Ouvrir projet", self.open_project_dialog)
        file_menu.addAction("Fermer projet", self.close_project_dialog)
        file_menu.addSeparator()
        file_menu.addAction("Ouvrir dossier projet", self.open_project_folder_dialog)

        project_menu = menu_bar.addMenu("Projet")
        project_menu.addAction("Gérer corpus", self.import_tab.manage_corpora)
        project_menu.addAction("Renommer corpus", self.import_tab.rename_active_corpus)

        tools_menu = menu_bar.addMenu("Outils")
        tools_menu.addAction("Align avancé…", self._open_advanced_align)
        tools_menu.addAction("Diagnostics", self._show_diagnostics_dialog)

        view_menu = menu_bar.addMenu("View")
        view_menu.addAction("Réinitialiser la disposition", self._reset_layout)

    def _choose_project_dir(self, title: str) -> Path | None:
        selected = QFileDialog.getExistingDirectory(self, title)
        if not selected:
            return None
        return Path(selected)

    def _new_project(self) -> None:
        project_dir = self._choose_project_dir("Sélectionner un dossier pour le projet")
        if project_dir is None:
            return
        self.state.open_project(project_dir)
        self.tabs.setCurrentIndex(self.import_tab_index)

    def _open_project(self) -> None:
        project_dir = self._choose_project_dir("Ouvrir un projet")
        if project_dir is None:
            return
        self.state.open_project(project_dir)
        self.tabs.setCurrentIndex(self.import_tab_index)

    def _close_project(self) -> None:
        self.state.close_project()

    def _open_project_folder(self) -> None:
        if not self.state.project_dir:
            return
        open_directory(self.state.project_dir)

    def new_project_dialog(self) -> None:
        self._new_project()

    def open_project_dialog(self) -> None:
        self._open_project()

    def close_project_dialog(self) -> None:
        self._close_project()

    def open_project_folder_dialog(self) -> None:
        self._open_project_folder()

    def _show_diagnostics_dialog(self) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Diagnostics")
        dialog.resize(860, 520)

        tab = DiagnosticsTab(self.state)
        tab.refresh()

        container = QWidget(dialog)
        layout = QVBoxLayout(container)
        layout.addWidget(tab)
        dialog.setLayout(layout)
        dialog.exec()

    def _reset_layout(self) -> None:
        current = self.tabs.currentWidget()
        if current is None:
            return
        reset = getattr(current, "reset_layout", None)
        if callable(reset):
            reset()

    def _open_compare_tab(self, corpus_ids: object) -> None:
        ids: list[str] = []
        settings: dict[str, object] = {}
        if isinstance(corpus_ids, list):
            ids = [str(item) for item in corpus_ids]
        elif isinstance(corpus_ids, dict):
            raw_ids = corpus_ids.get("corpus_ids")
            if isinstance(raw_ids, list):
                ids = [str(item) for item in raw_ids]
            settings = {
                key: value
                for key, value in corpus_ids.items()
                if key in {"mode", "fuzzy_threshold", "key_field", "algorithm"}
            }
        if ids or settings:
            self.compare_tab.configure_alignment_options(corpus_ids=ids, **settings)
        self.tabs.setCurrentIndex(self.compare_tab_index)

    def _open_advanced_align(self) -> None:
        corpora = [self.state.active_dict_id] if self.state.active_dict_id else []
        self.compare_tab.configure_alignment_options(
            corpus_ids=corpora,
            mode="exact+fuzzy",
            fuzzy_threshold=90,
            key_field="headword_norm_effective",
            algorithm="greedy",
            open_alignment_tab=True,
        )
        self.tabs.setCurrentIndex(self.compare_tab_index)

    def _open_conventions_tab(self) -> None:
        self.tabs.setCurrentIndex(self.conventions_tab_index)

    def _open_export_tab(self) -> None:
        self.tabs.setCurrentIndex(self.export_tab_index)
