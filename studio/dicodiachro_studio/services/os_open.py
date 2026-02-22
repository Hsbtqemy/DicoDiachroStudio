from __future__ import annotations

import platform
import subprocess
from pathlib import Path

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices


def open_path(path: Path) -> bool:
    """Open a file or directory in the platform file manager."""
    target = path.expanduser().resolve()
    return QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))


def open_directory(path: Path) -> bool:
    """Open a directory, or the parent directory if `path` is a file."""
    target = path.expanduser().resolve()
    directory = target if target.is_dir() else target.parent
    return open_path(directory)


def reveal_in_file_manager(path: Path) -> bool:
    """Reveal a file in Finder/Explorer; on Linux opens containing directory."""
    target = path.expanduser().resolve()
    if not target.exists():
        target = target.parent

    system_name = platform.system()
    if system_name == "Darwin":
        cmd = ["open", "-R", str(target)]
    elif system_name == "Windows":
        cmd = ["explorer", f"/select,{target}"]
    else:
        directory = target if target.is_dir() else target.parent
        cmd = ["xdg-open", str(directory)]

    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return False
    return completed.returncode == 0
