import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from dicodiachro_studio.ui.main_window import MainWindow  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402


def test_gui_smoke_offscreen():
    """
    Smoke test : vérifie que l'app GUI s'instancie en headless.
    Le but n'est pas de tester l'UI en profondeur, juste éviter les crashs Qt/imports.
    """
    app = QApplication.instance() or QApplication([])

    w = MainWindow()
    w.show()
    app.processEvents()

    w.close()
    app.processEvents()
