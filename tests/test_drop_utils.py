from __future__ import annotations

from pathlib import Path

from dicodiachro_studio.services.drop_utils import classify_drop_paths


def test_classify_drop_paths_groups_expected_types(tmp_path: Path) -> None:
    zip_file = tmp_path / "export.ZIP"
    pdf_file = tmp_path / "scan.PDF"
    image_file = tmp_path / "page.TIFF"
    text_file = tmp_path / "input.txt"
    csv_file = tmp_path / "table.csv"
    directory = tmp_path / "folder"
    other_file = tmp_path / "notes.md"

    zip_file.write_text("zip", encoding="utf-8")
    pdf_file.write_text("pdf", encoding="utf-8")
    image_file.write_text("img", encoding="utf-8")
    text_file.write_text("txt", encoding="utf-8")
    csv_file.write_text("h,p\\na,b\\n", encoding="utf-8")
    other_file.write_text("md", encoding="utf-8")
    directory.mkdir()

    classified = classify_drop_paths(
        [zip_file, pdf_file, image_file, text_file, csv_file, directory, other_file]
    )

    assert classified.zip_files == [zip_file.resolve()]
    assert classified.pdf_files == [pdf_file.resolve()]
    assert classified.image_files == [image_file.resolve()]
    assert classified.text_files == [text_file.resolve()]
    assert classified.csv_files == [csv_file.resolve()]
    assert classified.directories == [directory.resolve()]
    assert classified.other_files == [other_file.resolve()]
    assert classified.has_supported is True


def test_classify_drop_paths_deduplicates(tmp_path: Path) -> None:
    text_file = tmp_path / "same.txt"
    text_file.write_text("x", encoding="utf-8")

    classified = classify_drop_paths([text_file, text_file])

    assert classified.text_files == [text_file.resolve()]
