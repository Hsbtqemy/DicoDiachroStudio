from PySide6.QtWidgets import QApplication

from dicodiachro_studio.ui.main_window import MainWindow


def test_gui_smoke_offscreen():
    app = QApplication.instance() or QApplication([])
    w = MainWindow()
    w.show()
    app.processEvents()
    w.close()
    app.processEvents()
