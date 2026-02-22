from __future__ import annotations

import re
from pathlib import Path

HEX_COLOR_RE = re.compile(r"#[0-9a-fA-F]{3,8}")


def test_no_hardcoded_colors_in_workshop_and_entries() -> None:
    targets = [
        Path("studio/dicodiachro_studio/ui/tabs/templates_tab.py"),
        Path("studio/dicodiachro_studio/ui/tabs/entries_tab.py"),
        Path("studio/dicodiachro_studio/ui/tabs/conventions_tab.py"),
        Path("studio/dicodiachro_studio/ui/tabs/compare_tab.py"),
    ]

    for target in targets:
        text = target.read_text(encoding="utf-8")
        assert "QColor(" not in text
        assert not HEX_COLOR_RE.search(text)
        assert "setForeground(" not in text
        assert "color: #" not in text
        assert "background: #" not in text
