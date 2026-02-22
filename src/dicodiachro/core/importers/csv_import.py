from __future__ import annotations

import hashlib
import shutil
from pathlib import Path


def list_csv_files(path: Path, patterns: list[str] | None = None) -> list[Path]:
    patterns = patterns or ["*.csv"]
    if path.is_file():
        return [path]
    found: list[Path] = []
    for pattern in patterns:
        found.extend(path.rglob(pattern))
    return sorted(set(found))


def import_csv_batch(
    project_raw_imports: Path, input_path: Path, patterns: list[str] | None = None
) -> list[Path]:
    project_raw_imports.mkdir(parents=True, exist_ok=True)
    imported: list[Path] = []
    for src in list_csv_files(input_path, patterns=patterns):
        content_hash = hashlib.sha256(src.read_bytes()).hexdigest()[:12]
        dst = project_raw_imports / f"{src.stem}-{content_hash}{src.suffix.lower()}"
        if not dst.exists():
            shutil.copy2(src, dst)
        imported.append(dst)
    return imported
