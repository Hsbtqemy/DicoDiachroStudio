from __future__ import annotations

from pathlib import Path

import pytest
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dicodiachro.core.importers.pdf_entry_import import (
    PDFEntryImportError,
    _LineInfo,
    _has_font_signal,
    _has_indent_signal,
    _segment_by_font,
    _segment_by_gap,
    _segment_by_indent,
    _words_to_lines,
    detect_signal,
    extract_pdf_entries,
    import_pdf_entries,
)
from dicodiachro.core.storage.sqlite import init_project


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------


def _write_gapped_pdf(path: Path) -> None:
    """Two entries separated by a large vertical gap."""
    c = canvas.Canvas(str(path), pagesize=letter)
    c.drawString(72, 760, "Abaisser,")
    c.drawString(72, 745, "v.a. Faire descendre.")
    # gap > normal line spacing
    c.drawString(72, 710, "Abaissement,")
    c.drawString(72, 695, "s.m. Action d'abaisser.")
    c.save()


def _write_indent_pdf(path: Path) -> None:
    """Two entries with hanging indent (continuation lines indented)."""
    c = canvas.Canvas(str(path), pagesize=letter)
    # entry 1: starts at x=72, continuation at x=90
    c.drawString(72, 760, "Abaisser,")
    c.drawString(90, 745, "v.a. Faire descendre.")
    c.drawString(90, 730, "quelque chose.")
    # entry 2: starts at x=72
    c.drawString(72, 715, "Abaissement,")
    c.drawString(90, 700, "s.m. Action d'abaisser.")
    c.save()


def _write_no_text_pdf(path: Path) -> None:
    c = canvas.Canvas(str(path), pagesize=letter)
    c.save()


# ---------------------------------------------------------------------------
# Unit tests: signal detection helpers
# ---------------------------------------------------------------------------


def _make_line(text: str, x0: float, top: float, fonts: frozenset[str] | None = None) -> _LineInfo:
    return _LineInfo(text=text, x0=x0, x1=x0 + 50.0, top=top, bottom=top + 12.0, fonts=fonts or frozenset())


class TestHasFontSignal:
    def test_bold_lines_detected(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760, frozenset({"TimesNewRoman,Bold"})),
            _make_line("v.a. Faire descendre.", 72, 745, frozenset({"TimesNewRoman"})),
            _make_line("Abaissement,", 72, 715, frozenset({"TimesNewRoman,Bold"})),
            _make_line("s.m. Action.", 72, 700, frozenset({"TimesNewRoman"})),
        ]
        assert _has_font_signal(lines)

    def test_no_bold_returns_false(self) -> None:
        lines = [_make_line(f"word {i}", 72, float(760 - i * 15)) for i in range(10)]
        assert not _has_font_signal(lines)

    def test_empty_returns_false(self) -> None:
        assert not _has_font_signal([])


class TestHasIndentSignal:
    def test_two_clusters_detected(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760),
            _make_line("v.a. Faire", 90, 745),
            _make_line("descendre.", 90, 730),
            _make_line("Abaissement,", 72, 710),
            _make_line("s.m. Action.", 90, 695),
            _make_line("d'abaisser.", 90, 680),
        ]
        assert _has_indent_signal(lines)

    def test_uniform_x0_no_signal(self) -> None:
        lines = [_make_line(f"word {i}", 72.0, float(760 - i * 15)) for i in range(8)]
        assert not _has_indent_signal(lines)

    def test_too_few_lines_no_signal(self) -> None:
        lines = [_make_line("a", 72, 760), _make_line("b", 90, 745)]
        assert not _has_indent_signal(lines)


class TestDetectSignal:
    def test_prefers_font_over_indent(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760, frozenset({"TimesNewRoman,Bold"})),
            _make_line("v.a. Faire", 90, 745),
            _make_line("Abaissement,", 72, 715, frozenset({"TimesNewRoman,Bold"})),
            _make_line("s.m. Action.", 90, 700),
        ]
        assert detect_signal(lines) == "font"

    def test_indent_when_no_bold(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760),
            _make_line("v.a. Faire", 90, 745),
            _make_line("descendre.", 90, 730),
            _make_line("Abaissement,", 72, 710),
            _make_line("s.m. Action.", 90, 695),
            _make_line("d'abaisser.", 90, 680),
        ]
        assert detect_signal(lines) == "indent"

    def test_gap_fallback(self) -> None:
        lines = [_make_line(f"word {i}", 72.0, float(760 - i * 15)) for i in range(6)]
        assert detect_signal(lines) == "gap"


# ---------------------------------------------------------------------------
# Unit tests: segmentation strategies
# ---------------------------------------------------------------------------


class TestSegmentByFont:
    def test_splits_on_bold(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760, frozenset({"TimesNewRoman,Bold"})),
            _make_line("v.a. Faire descendre.", 72, 745),
            _make_line("Abaissement,", 72, 715, frozenset({"TimesNewRoman,Bold"})),
            _make_line("s.m. Action.", 72, 700),
        ]
        blocks = _segment_by_font(lines)
        assert len(blocks) == 2
        assert blocks[0][0].text == "Abaisser,"
        assert blocks[1][0].text == "Abaissement,"

    def test_no_bold_single_block(self) -> None:
        lines = [_make_line(f"word {i}", 72, float(760 - i * 15)) for i in range(4)]
        blocks = _segment_by_font(lines)
        assert len(blocks) == 1


class TestSegmentByIndent:
    def test_splits_on_left_margin(self) -> None:
        lines = [
            _make_line("Abaisser,", 72, 760),
            _make_line("v.a. Faire", 90, 745),
            _make_line("descendre.", 90, 730),
            _make_line("Abaissement,", 72, 710),
            _make_line("s.m. Action.", 90, 695),
        ]
        blocks = _segment_by_indent(lines)
        assert len(blocks) == 2
        assert blocks[0][0].text == "Abaisser,"
        assert blocks[1][0].text == "Abaissement,"


class TestSegmentByGap:
    def test_splits_on_large_gap(self) -> None:
        # top increases downward (pdfplumber convention): 30, 44 → entry 1; 70, 84 → entry 2
        lines = [
            _make_line("Abaisser,", 72, 30),
            _make_line("v.a. Faire.", 72, 44),   # gap ~2pt (44-42) → same entry
            _make_line("Abaissement,", 72, 70),  # gap ~26pt (70-56) → new entry
            _make_line("s.m. Action.", 72, 84),
        ]
        blocks = _segment_by_gap(lines, gap_factor=1.5)
        assert len(blocks) == 2

    def test_uniform_spacing_single_block(self) -> None:
        lines = [_make_line(f"word {i}", 72, float(760 - i * 14)) for i in range(4)]
        blocks = _segment_by_gap(lines)
        assert len(blocks) == 1


# ---------------------------------------------------------------------------
# Integration tests: extract_pdf_entries
# ---------------------------------------------------------------------------


class TestExtractPdfEntries:
    def test_gap_signal_finds_two_entries(self, tmp_path: Path) -> None:
        pdf = tmp_path / "gapped.pdf"
        _write_gapped_pdf(pdf)
        blocks, pages_total, pages_with_text, signal_used = extract_pdf_entries(
            pdf, columns=1, signal="gap"
        )
        assert pages_total == 1
        assert pages_with_text == 1
        assert len(blocks) == 2
        assert signal_used == "gap"

    def test_indent_signal_finds_two_entries(self, tmp_path: Path) -> None:
        pdf = tmp_path / "indented.pdf"
        _write_indent_pdf(pdf)
        blocks, _, _, signal_used = extract_pdf_entries(pdf, columns=1, signal="indent")
        assert len(blocks) == 2
        assert signal_used == "indent"

    def test_auto_signal_returns_result(self, tmp_path: Path) -> None:
        pdf = tmp_path / "auto.pdf"
        _write_gapped_pdf(pdf)
        blocks, _, _, signal_used = extract_pdf_entries(pdf, columns=1, signal="auto")
        assert len(blocks) >= 1
        assert signal_used in {"font", "indent", "gap"}

    def test_no_text_raises(self, tmp_path: Path) -> None:
        pdf = tmp_path / "empty.pdf"
        _write_no_text_pdf(pdf)
        with pytest.raises(PDFEntryImportError) as exc_info:
            extract_pdf_entries(pdf)
        assert exc_info.value.code == "PDF_NO_ENTRIES"

    def test_invalid_columns_raises(self, tmp_path: Path) -> None:
        pdf = tmp_path / "x.pdf"
        _write_gapped_pdf(pdf)
        with pytest.raises(PDFEntryImportError) as exc_info:
            extract_pdf_entries(pdf, columns=5)
        assert exc_info.value.code == "PDF_INVALID_COLUMNS"


# ---------------------------------------------------------------------------
# Integration tests: import_pdf_entries
# ---------------------------------------------------------------------------


class TestImportPdfEntries:
    def test_writes_txt_and_sidecar(self, tmp_path: Path) -> None:
        pdf = tmp_path / "dict.pdf"
        _write_gapped_pdf(pdf)
        project_dir = tmp_path / "project"
        init_project(project_dir)

        result = import_pdf_entries(project_dir, pdf, signal="gap")

        assert result.output_path.exists()
        assert result.output_path.with_name(result.output_path.name + ".line_pages").exists()
        assert result.entries_found == 2
        assert result.signal_used == "gap"
        assert result.pages_total == 1

    def test_entries_text_non_empty(self, tmp_path: Path) -> None:
        pdf = tmp_path / "dict.pdf"
        _write_gapped_pdf(pdf)
        project_dir = tmp_path / "project"
        init_project(project_dir)

        result = import_pdf_entries(project_dir, pdf, signal="gap")
        lines = result.output_path.read_text(encoding="utf-8").splitlines()
        assert all(line.strip() for line in lines)

    def test_custom_output_path(self, tmp_path: Path) -> None:
        pdf = tmp_path / "dict.pdf"
        _write_gapped_pdf(pdf)
        project_dir = tmp_path / "project"
        init_project(project_dir)
        out = tmp_path / "out" / "entries.txt"

        result = import_pdf_entries(project_dir, pdf, out=out, signal="gap")
        assert result.output_path == out
        assert out.exists()

    def test_missing_pdf_raises(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        init_project(project_dir)
        with pytest.raises(PDFEntryImportError):
            import_pdf_entries(project_dir, tmp_path / "missing.pdf")
