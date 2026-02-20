from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import Any

from PySide6.QtCore import QObject, QThread, Signal


class JobSignals(QObject):
    progress = Signal(int)
    finished = Signal(object)
    failed = Signal(str)


class JobThread(QThread):
    def __init__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = JobSignals()
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            result = self.fn(*self.args, **self.kwargs)
            if not self._cancelled:
                self.signals.finished.emit(result)
        except Exception:
            self.signals.failed.emit(traceback.format_exc())
