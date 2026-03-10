from PySide6.QtWidgets import QApplication

from dicodiachro_studio.ui.main_window import MainWindow


def test_main_window_exposes_additional_tabs() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window.show()
    app.processEvents()

    labels = [window.tabs.tabText(idx) for idx in range(window.tabs.count())]

    assert "Align avance" in labels
    assert "Profils" in labels
    assert "Projet" in labels

    window.close()
    app.processEvents()


def test_main_window_reset_layout_feedback_when_unavailable() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window.show()
    app.processEvents()

    window.tabs.setCurrentWidget(window.import_tab)
    window._reset_layout()
    app.processEvents()

    message = window.statusBar().currentMessage()
    assert "Aucune disposition a reinitialiser" in message

    window.close()
    app.processEvents()
