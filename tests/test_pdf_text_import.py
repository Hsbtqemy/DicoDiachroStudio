from __future__ import annotations

from pathlib import Path

import pytest
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from typer.testing import CliRunner

from dicodiachro.cli.app import app
from dicodiachro.core.importers.pdf_text_import import PDFTextImportError, import_pdf_text
from dicodiachro.core.storage.sqlite import init_project


def _write_two_column_pdf(path: Path) -> None:
    writer = canvas.Canvas(str(path), pagesize=letter)
    writer.drawString(72, 760, "left one")
    writer.drawString(72, 740, "left two")
    writer.drawString(340, 760, "right one")
    writer.drawString(340, 740, "right two")
    writer.save()


def _write_three_column_pdf(path: Path) -> None:
    writer = canvas.Canvas(str(path), pagesize=letter)
    writer.drawString(50, 760, "col1 one")
    writer.drawString(50, 740, "col1 two")
    writer.drawString(230, 760, "col2 one")
    writer.drawString(230, 740, "col2 two")
    writer.drawString(410, 760, "col3 one")
    writer.drawString(410, 740, "col3 two")
    writer.save()


def _write_two_column_sparse_x_pdf(path: Path) -> None:
    writer = canvas.Canvas(str(path), pagesize=letter)
    writer.drawString(72, 760, "L1")
    writer.drawString(72, 740, "L2")
    writer.drawString(250, 760, "R1")
    writer.drawString(250, 740, "R2")
    writer.save()


def _write_shape_only_pdf(path: Path) -> None:
    writer = canvas.Canvas(str(path), pagesize=letter)
    writer.rect(72, 640, 220, 80, fill=1, stroke=0)
    writer.save()


def test_pdf_text_import_simple(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    pdf_path = tmp_path / "simple.pdf"
    _write_two_column_pdf(pdf_path)

    result = import_pdf_text(project_dir=project_dir, pdf_path=pdf_path, columns=1)
    extracted = result.output_text_paths[0].read_text(encoding="utf-8")

    assert result.pages_total == 1
    assert result.pages_with_text == 1
    assert "left one" in extracted
    assert "right two" in extracted

    # sidecar .line_pages: one int per line, all 1 for single-page PDF
    sidecar = result.output_text_paths[0].with_name(
        result.output_text_paths[0].name + ".line_pages"
    )
    assert sidecar.exists()
    line_pages = [int(x) for x in sidecar.read_text(encoding="utf-8").strip().splitlines()]
    lines = extracted.strip().splitlines()
    assert len(line_pages) == len(lines)
    assert all(p == 1 for p in line_pages)


def test_pdf_text_import_two_columns_left_then_right(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    pdf_path = tmp_path / "two-columns.pdf"
    _write_two_column_pdf(pdf_path)

    result = import_pdf_text(project_dir=project_dir, pdf_path=pdf_path, columns=2)
    lines = result.output_text_paths[0].read_text(encoding="utf-8").splitlines()

    assert lines[:4] == ["left one", "left two", "right one", "right two"]


def test_pdf_text_import_three_columns_left_to_right(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    pdf_path = tmp_path / "three-columns.pdf"
    _write_three_column_pdf(pdf_path)

    result = import_pdf_text(project_dir=project_dir, pdf_path=pdf_path, columns=3)
    lines = result.output_text_paths[0].read_text(encoding="utf-8").splitlines()

    assert lines[:6] == [
        "col1 one",
        "col1 two",
        "col2 one",
        "col2 two",
        "col3 one",
        "col3 two",
    ]


def test_pdf_text_import_two_columns_with_sparse_x_positions(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    pdf_path = tmp_path / "two-columns-sparse-x.pdf"
    _write_two_column_sparse_x_pdf(pdf_path)

    result = import_pdf_text(project_dir=project_dir, pdf_path=pdf_path, columns=2)
    lines = result.output_text_paths[0].read_text(encoding="utf-8").splitlines()

    assert lines[:4] == ["L1", "L2", "R1", "R2"]


def test_pdf_text_import_no_text_layer_raises(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    pdf_path = tmp_path / "shape-only.pdf"
    _write_shape_only_pdf(pdf_path)

    with pytest.raises(PDFTextImportError) as exc_info:
        import_pdf_text(project_dir=project_dir, pdf_path=pdf_path, columns=2)

    assert exc_info.value.code == "PDF_NO_TEXT_LAYER"
    assert "ABBYY" in str(exc_info.value)


def test_cli_import_pdf_text_smoke(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    pdf_path = tmp_path / "cli.pdf"
    _write_two_column_pdf(pdf_path)

    runner = CliRunner()
    init_result = runner.invoke(app, ["init", str(project_dir)])
    assert init_result.exit_code == 0, init_result.output

    import_result = runner.invoke(
        app,
        ["import", "pdf-text", str(project_dir), str(pdf_path), "--columns", "2"],
    )
    assert import_result.exit_code == 0, import_result.output
    assert "output_text_paths" in import_result.output
