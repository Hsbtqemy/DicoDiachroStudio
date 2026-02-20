from __future__ import annotations

from PySide6.QtWidgets import QMainWindow, QTabWidget

from ..services.state import AppState
from .tabs.align_tab import AlignTab
from .tabs.compare_tab import CompareTab
from .tabs.diagnostics_tab import DiagnosticsTab
from .tabs.entries_tab import EntriesTab
from .tabs.export_tab import ExportTab
from .tabs.import_tab import ImportTab
from .tabs.profiles_tab import ProfilesTab
from .tabs.project_tab import ProjectTab


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("DicoDiachro Studio")
        self.resize(1400, 900)

        self.state = AppState()

        tabs = QTabWidget()
        tabs.addTab(ProjectTab(self.state), "Project")
        tabs.addTab(ImportTab(self.state), "Import")
        tabs.addTab(EntriesTab(self.state), "Entries")
        tabs.addTab(ProfilesTab(self.state), "Profiles")
        tabs.addTab(AlignTab(self.state), "Align")
        tabs.addTab(CompareTab(self.state), "Compare")
        tabs.addTab(ExportTab(self.state), "Export")
        tabs.addTab(DiagnosticsTab(self.state), "Diagnostics")

        self.setCentralWidget(tabs)
