from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

IMAGE_SUFFIXES = {".tif", ".tiff", ".png", ".jpg", ".jpeg"}
PDF_SUFFIXES = {".pdf"}
ZIP_SUFFIXES = {".zip"}
TEXT_SUFFIXES = {".txt"}
CSV_SUFFIXES = {".csv"}


@dataclass(slots=True)
class DropClassification:
    zip_files: list[Path] = field(default_factory=list)
    pdf_files: list[Path] = field(default_factory=list)
    image_files: list[Path] = field(default_factory=list)
    text_files: list[Path] = field(default_factory=list)
    csv_files: list[Path] = field(default_factory=list)
    directories: list[Path] = field(default_factory=list)
    other_files: list[Path] = field(default_factory=list)

    @property
    def has_supported(self) -> bool:
        return any(
            [
                self.zip_files,
                self.pdf_files,
                self.image_files,
                self.text_files,
                self.csv_files,
                self.directories,
            ]
        )


def classify_drop_paths(paths: Iterable[Path]) -> DropClassification:
    classification = DropClassification()
    seen: set[Path] = set()

    for raw_path in paths:
        path = raw_path.expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)

        if path.is_dir():
            classification.directories.append(path)
            continue

        suffix = path.suffix.lower()
        if suffix in ZIP_SUFFIXES:
            classification.zip_files.append(path)
        elif suffix in PDF_SUFFIXES:
            classification.pdf_files.append(path)
        elif suffix in IMAGE_SUFFIXES:
            classification.image_files.append(path)
        elif suffix in TEXT_SUFFIXES:
            classification.text_files.append(path)
        elif suffix in CSV_SUFFIXES:
            classification.csv_files.append(path)
        else:
            classification.other_files.append(path)

    return classification
