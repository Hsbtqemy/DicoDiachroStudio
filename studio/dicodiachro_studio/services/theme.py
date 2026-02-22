from __future__ import annotations

from PySide6.QtWidgets import (
    QListView,
    QListWidget,
    QTableView,
    QTableWidget,
    QTreeView,
    QTreeWidget,
    QWidget,
)

_ITEM_VIEW_QSS = """
QListView, QListWidget, QTreeView, QTreeWidget, QTableView, QTableWidget {
  background-color: palette(base);
  color: palette(text);
  selection-background-color: palette(highlight);
  selection-color: palette(highlighted-text);
  alternate-background-color: palette(alternate-base);
  border: 1px solid palette(mid);
}
QListView::item:selected,
QListWidget::item:selected,
QTreeView::item:selected,
QTreeWidget::item:selected,
QTableView::item:selected,
QTableWidget::item:selected {
  background-color: palette(highlight);
  color: palette(highlighted-text);
}
"""


def apply_theme_safe_styles(widget: QWidget) -> None:
    item_view_types = (QListView, QListWidget, QTreeView, QTreeWidget, QTableView, QTableWidget)
    for view_type in item_view_types:
        for child in widget.findChildren(view_type):
            child.setStyleSheet(_ITEM_VIEW_QSS)
            if hasattr(child, "setAlternatingRowColors"):
                child.setAlternatingRowColors(True)
