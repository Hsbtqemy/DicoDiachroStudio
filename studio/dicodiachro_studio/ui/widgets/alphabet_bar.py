from __future__ import annotations

from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QHBoxLayout, QSizePolicy, QToolButton, QWidget


class AlphabetBar(QWidget):
    bucket_changed = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._active_bucket: str | None = None
        self._buttons: dict[str, QToolButton] = {}

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        for bucket, label in self._bucket_labels():
            button = QToolButton(self)
            button.setObjectName(f"alphabet_bucket_{bucket or 'all'}")
            button.setText(label)
            button.setCheckable(True)
            button.setAutoRaise(False)
            button.clicked.connect(lambda checked=False, b=bucket: self._on_bucket_clicked(b))
            button.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
            self._buttons[bucket] = button
            layout.addWidget(button)

        layout.addStretch(1)
        self.set_counts({})
        self.set_active_bucket(None)

    @staticmethod
    def _bucket_labels() -> list[tuple[str, str]]:
        labels = [("", "Tout")]
        labels.extend((chr(code), chr(code)) for code in range(ord("A"), ord("Z") + 1))
        labels.append(("#", "#"))
        return labels

    def set_counts(self, counts: dict[str, Any]) -> None:
        normalized: dict[str, int] = {}
        for key, value in counts.items():
            bucket = str(key or "").strip().upper()
            if not bucket:
                continue
            try:
                normalized[bucket] = int(value)
            except (TypeError, ValueError):
                normalized[bucket] = 0

        for bucket, button in self._buttons.items():
            if bucket == "":
                button.setToolTip("Tout")
                button.setEnabled(True)
                continue
            count = normalized.get(bucket, 0)
            button.setToolTip(f"{bucket} : {count}")
            button.setEnabled(count > 0 or bucket == self._active_bucket)

    def set_active_bucket(self, bucket: str | None) -> None:
        normalized = str(bucket or "").strip().upper()
        self._active_bucket = normalized or None

        for key, button in self._buttons.items():
            is_active = key == (self._active_bucket or "")
            button.blockSignals(True)
            button.setChecked(is_active)
            button.blockSignals(False)

    def active_bucket(self) -> str | None:
        return self._active_bucket

    def button_for_bucket(self, bucket: str | None) -> QToolButton | None:
        normalized = str(bucket or "").strip().upper()
        if not normalized:
            normalized = ""
        return self._buttons.get(normalized)

    def _on_bucket_clicked(self, bucket: str) -> None:
        normalized = bucket.strip().upper()
        self.set_active_bucket(normalized or None)
        self.bucket_changed.emit(normalized)
