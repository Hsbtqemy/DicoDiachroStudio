from __future__ import annotations

import hashlib
import shutil
from pathlib import Path


def list_text_files(path: Path, patterns: list[str] | None = None) -> list[Path]:
    patterns = patterns or ["*.txt"]
    if path.is_file():
        return [path]
    found: list[Path] = []
    for pattern in patterns:
        found.extend(path.rglob(pattern))
    return sorted(set(found))


def import_text_batch(
    project_raw_imports: Path, input_path: Path, patterns: list[str] | None = None
) -> list[Path]:
    project_raw_imports.mkdir(parents=True, exist_ok=True)
    imported: list[Path] = []
    for src in list_text_files(input_path, patterns=patterns):
        content_hash = hashlib.sha256(src.read_bytes()).hexdigest()[:12]
        dst = project_raw_imports / f"{src.stem}-{content_hash}{src.suffix.lower()}"
        if not dst.exists():
            shutil.copy2(src, dst)
        imported.append(dst)
    return imported


def merge_text_files(
    files: list[Path],
    output_path: Path,
    deduplicate: bool = True,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()

    with output_path.open("w", encoding="utf-8") as out:
        for file_path in sorted(files):
            with file_path.open("r", encoding="utf-8", errors="replace") as handle:
                for raw_line in handle:
                    line = raw_line.rstrip("\n")
                    if deduplicate and line in seen:
                        continue
                    seen.add(line)
                    out.write(line + "\n")

    return output_path
