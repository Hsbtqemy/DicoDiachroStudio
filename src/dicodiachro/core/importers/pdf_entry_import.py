from __future__ import annotations

import hashlib
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from ..storage.sqlite import project_paths
from ._pdf_utils import column_breakpoints, split_words_by_column

Signal = Literal["font", "indent", "gap", "auto"]

_BOLD_MARKERS = ("bold", "Bold", "BOLD", "Heavy", "Black")


@dataclass(slots=True)
class _LineInfo:
    text: str
    x0: float
    x1: float
    top: float
    bottom: float
    fonts: frozenset[str]


@dataclass(slots=True)
class EntryBlock:
    lines: list[str]
    page: int
    x0: float
    top: float
    x1: float
    bottom: float
    signal_used: str

    @property
    def text(self) -> str:
        return " ".join(line for line in self.lines if line)

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        return (self.x0, self.top, self.x1, self.bottom)


@dataclass(slots=True)
class PDFEntryImportResult:
    source_pdf: Path
    output_path: Path
    pages_total: int
    pages_with_text: int
    entries_found: int
    signal_used: str
    columns: int

    def as_dict(self) -> dict[str, object]:
        return {
            "source_pdf": str(self.source_pdf),
            "output_path": str(self.output_path),
            "pages_total": self.pages_total,
            "pages_with_text": self.pages_with_text,
            "entries_found": self.entries_found,
            "signal_used": self.signal_used,
            "columns": self.columns,
        }


class PDFEntryImportError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "PDF_ENTRY_IMPORT_ERROR",
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


# ---------------------------------------------------------------------------
# Word → line grouping (with metadata)
# ---------------------------------------------------------------------------


def _words_to_lines(words: list[dict], y_tolerance: float = 2.5) -> list[_LineInfo]:
    if not words:
        return []
    ordered = sorted(words, key=lambda w: (float(w["top"]), float(w["x0"])))
    groups: list[list[dict]] = []
    current: list[dict] = [ordered[0]]
    current_top = float(ordered[0]["top"])
    for word in ordered[1:]:
        top = float(word["top"])
        if abs(top - current_top) <= y_tolerance:
            current.append(word)
        else:
            groups.append(sorted(current, key=lambda w: float(w["x0"])))
            current = [word]
            current_top = top
    groups.append(sorted(current, key=lambda w: float(w["x0"])))

    lines: list[_LineInfo] = []
    for grp in groups:
        text = " ".join(
            str(w.get("text", "")).strip() for w in grp if str(w.get("text", "")).strip()
        )
        if not text:
            continue
        x0 = float(grp[0]["x0"])
        x1 = max(float(w.get("x1", w.get("x0", x0))) for w in grp)
        top = float(grp[0]["top"])
        bottom = max(float(w.get("bottom", top)) for w in grp)
        fonts = frozenset(str(w.get("fontname", "")) for w in grp if w.get("fontname"))
        lines.append(_LineInfo(text=text, x0=x0, x1=x1, top=top, bottom=bottom, fonts=fonts))
    return lines


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------


def _has_font_signal(lines: list[_LineInfo]) -> bool:
    """True if a meaningful subset of lines start with a bold/distinct font."""
    if not lines:
        return False
    bold_starts = sum(
        1
        for ln in lines
        if any(marker in f for f in ln.fonts for marker in _BOLD_MARKERS)
    )
    return bold_starts >= max(2, len(lines) * 0.05)


def _has_indent_signal(lines: list[_LineInfo], *, threshold_pt: float = 6.0) -> bool:
    """True if x0 values form two clusters (hanging indent pattern)."""
    if len(lines) < 4:
        return False
    x0s = sorted(ln.x0 for ln in lines)
    # Find the largest gap in the sorted x0 distribution
    gaps = [(x0s[i + 1] - x0s[i], i) for i in range(len(x0s) - 1)]
    max_gap, split_idx = max(gaps, key=lambda t: t[0])
    if max_gap < threshold_pt:
        return False
    left_cluster = x0s[: split_idx + 1]
    # At least 15% of lines should be in the left (entry-start) cluster
    return len(left_cluster) >= max(2, len(lines) * 0.15)


def detect_signal(lines: list[_LineInfo]) -> str:
    """Return the best segmentation signal for this page sample."""
    if _has_font_signal(lines):
        return "font"
    if _has_indent_signal(lines):
        return "indent"
    return "gap"


# ---------------------------------------------------------------------------
# Segmentation strategies
# ---------------------------------------------------------------------------


def _entry_start_by_font(line: _LineInfo) -> bool:
    return any(marker in f for f in line.fonts for marker in _BOLD_MARKERS)


def _indent_threshold(lines: list[_LineInfo], threshold_pt: float = 6.0) -> float:
    """Return the x0 boundary separating entry-starts from continuation lines."""
    x0s = sorted(ln.x0 for ln in lines)
    gaps = [(x0s[i + 1] - x0s[i], i) for i in range(len(x0s) - 1)]
    max_gap, split_idx = max(gaps, key=lambda t: t[0])
    if max_gap < threshold_pt:
        return x0s[0] + threshold_pt
    return (x0s[split_idx] + x0s[split_idx + 1]) / 2


def _median_line_gap(lines: list[_LineInfo]) -> float:
    if len(lines) < 2:
        return 12.0
    gaps = [lines[i + 1].top - lines[i].bottom for i in range(len(lines) - 1)]
    positive = [g for g in gaps if g > 0]
    return statistics.median(positive) if positive else 12.0


def _segment_by_font(lines: list[_LineInfo]) -> list[list[_LineInfo]]:
    blocks: list[list[_LineInfo]] = []
    current: list[_LineInfo] = []
    for ln in lines:
        if _entry_start_by_font(ln) and current:
            blocks.append(current)
            current = [ln]
        else:
            current.append(ln)
    if current:
        blocks.append(current)
    return blocks


def _segment_by_indent(lines: list[_LineInfo]) -> list[list[_LineInfo]]:
    if not lines:
        return []
    boundary = _indent_threshold(lines)
    blocks: list[list[_LineInfo]] = []
    current: list[_LineInfo] = []
    for ln in lines:
        if ln.x0 < boundary and current:
            blocks.append(current)
            current = [ln]
        else:
            current.append(ln)
    if current:
        blocks.append(current)
    return blocks


def _segment_by_gap(lines: list[_LineInfo], *, gap_factor: float = 1.5) -> list[list[_LineInfo]]:
    if not lines:
        return []
    median_gap = _median_line_gap(lines)
    threshold = median_gap * gap_factor
    blocks: list[list[_LineInfo]] = []
    current: list[_LineInfo] = [lines[0]]
    for prev, ln in zip(lines, lines[1:]):
        gap = ln.top - prev.bottom
        if gap > threshold:
            blocks.append(current)
            current = [ln]
        else:
            current.append(ln)
    blocks.append(current)
    return blocks


def _segment(lines: list[_LineInfo], signal: str) -> list[list[_LineInfo]]:
    if signal == "font":
        return _segment_by_font(lines)
    if signal == "indent":
        return _segment_by_indent(lines)
    return _segment_by_gap(lines)


# ---------------------------------------------------------------------------
# Per-page extraction
# ---------------------------------------------------------------------------


def _extract_page_entries(
    page,
    words: list[dict],
    *,
    columns: int,
    signal: str,
    page_no: int,
) -> tuple[list[EntryBlock], str]:
    if not words:
        return [], signal

    width = max(float(page.width or 1.0), 1.0)
    breakpoints = column_breakpoints(words, width=width, columns=columns)
    col_words = split_words_by_column(words, breakpoints)

    # Detect signal once per page on all lines, not per column
    effective_signal = signal
    if signal == "auto":
        all_lines = [ln for col in col_words for ln in _words_to_lines(col)]
        effective_signal = detect_signal(all_lines)

    blocks: list[EntryBlock] = []
    for col in col_words:
        lines = _words_to_lines(col)
        if not lines:
            continue
        for grp in _segment(lines, effective_signal):
            if grp:
                blocks.append(
                    EntryBlock(
                        lines=[ln.text for ln in grp],
                        page=page_no,
                        x0=min(ln.x0 for ln in grp),
                        top=grp[0].top,
                        x1=max(ln.x1 for ln in grp),
                        bottom=grp[-1].bottom,
                        signal_used=effective_signal,
                    )
                )

    return blocks, effective_signal


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_pdf_entries(
    pdf_path: Path,
    *,
    columns: int = 1,
    signal: Signal = "auto",
    min_chars_per_page: int = 4,
) -> tuple[list[EntryBlock], int, int, str]:
    """
    Extract entry blocks from an OCR'd PDF.

    Returns (blocks, pages_total, pages_with_text, signal_used).
    """
    if columns < 1 or columns > 3:
        raise PDFEntryImportError(
            "columns doit être entre 1 et 3",
            code="PDF_INVALID_COLUMNS",
            details={"columns": columns},
        )
    try:
        import pdfplumber
    except ImportError as exc:
        raise RuntimeError(
            "pdfplumber n'est pas installé. Installez avec `pip install 'dicodiachro[pdf]'`."
        ) from exc

    all_blocks: list[EntryBlock] = []
    pages_total = pages_with_text = 0
    signal_used = signal if signal != "auto" else "gap"

    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            pages_total += 1
            words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
            chars = sum(len(str(w.get("text", "")).replace(" ", "")) for w in words)
            if chars >= min_chars_per_page:
                pages_with_text += 1
            page_blocks, effective = _extract_page_entries(
                page, words, columns=columns, signal=signal, page_no=page_no
            )
            all_blocks.extend(page_blocks)
            if signal == "auto" and page_blocks:
                signal_used = effective

    if pages_total == 0 or pages_with_text == 0 or not all_blocks:
        raise PDFEntryImportError(
            "PDF sans couche texte ou aucune entrée détectée.",
            code="PDF_NO_ENTRIES",
            details={
                "pdf_path": str(pdf_path),
                "pages_total": pages_total,
                "pages_with_text": pages_with_text,
            },
        )

    return all_blocks, pages_total, pages_with_text, signal_used


def _default_target_path(project_dir: Path, pdf_path: Path) -> Path:
    imports_dir = project_paths(project_dir).raw_dir / "imports"
    imports_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()[:12]
    return imports_dir / f"{pdf_path.stem}-entries-{digest}.txt"


def _crop_and_save(
    pdf_path: Path,
    blocks: list[EntryBlock],
    images_dir: Path,
    *,
    margin_pt: float = 4.0,
    resolution: int = 150,
) -> dict[int, Path]:
    """
    Crop each entry block from the PDF and save as JPEG.

    Returns a mapping {block_index: image_path}.
    Only called when pdfplumber + Pillow are available.
    """
    import pdfplumber

    images_dir.mkdir(parents=True, exist_ok=True)
    result: dict[int, Path] = {}

    with pdfplumber.open(pdf_path) as pdf:
        pages = {p + 1: page for p, page in enumerate(pdf.pages)}
        for idx, block in enumerate(blocks):
            page = pages.get(block.page)
            if page is None:
                continue
            bbox = (
                max(0.0, block.x0 - margin_pt),
                max(0.0, block.top - margin_pt),
                min(float(page.width), block.x1 + margin_pt),
                min(float(page.height), block.bottom + margin_pt),
            )
            img_path = images_dir / f"entry_{idx:05d}_p{block.page:04d}.jpg"
            try:
                page.crop(bbox).to_image(resolution=resolution).save(str(img_path))
                result[idx] = img_path
            except Exception:
                pass

    return result


def import_pdf_entries(
    project_dir: Path,
    pdf_path: Path,
    *,
    out: Path | None = None,
    columns: int = 1,
    signal: Signal = "auto",
    save_images: bool = False,
    dict_id: str | None = None,
    store=None,
) -> PDFEntryImportResult:
    """
    Import entries from an OCR'd PDF into a project.

    Each detected entry is written as a single line in the output .txt file,
    compatible with the existing text import pipeline.

    save_images: crop each entry block as JPEG and register in entry_images.
                 Requires dict_id and store to link images to entries.
    """
    project_dir = project_dir.expanduser().resolve()
    source_pdf = pdf_path.expanduser().resolve()
    if not source_pdf.exists() or not source_pdf.is_file():
        raise PDFEntryImportError(f"PDF introuvable : {source_pdf}")

    blocks, pages_total, pages_with_text, signal_used = extract_pdf_entries(
        source_pdf,
        columns=columns,
        signal=signal,
    )

    target: Path
    if out is None:
        target = _default_target_path(project_dir, source_pdf)
    elif out.suffix.lower() == ".txt":
        out.parent.mkdir(parents=True, exist_ok=True)
        target = out
    else:
        out.mkdir(parents=True, exist_ok=True)
        target = out / f"{source_pdf.stem}-entries.txt"

    valid_blocks = [b for b in blocks if b.text.strip()]
    target.write_text("\n".join(b.text for b in valid_blocks) + "\n", encoding="utf-8")

    sidecar = target.with_name(target.name + ".line_pages")
    sidecar.write_text(
        "\n".join(str(b.page) for b in valid_blocks) + "\n", encoding="utf-8"
    )

    if save_images and dict_id and store is not None:
        images_dir = project_paths(project_dir).raw_dir / "images" / dict_id
        crops = _crop_and_save(source_pdf, valid_blocks, images_dir)
        # entry_images are linked after pipeline runs (entry_id not yet known here);
        # store crop paths as a sidecar for later linking
        sidecar_images = target.with_name(target.name + ".image_paths")
        lines = [str(crops.get(i, "")) for i in range(len(valid_blocks))]
        sidecar_images.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return PDFEntryImportResult(
        source_pdf=source_pdf,
        output_path=target,
        pages_total=pages_total,
        pages_with_text=pages_with_text,
        entries_found=len(valid_blocks),
        signal_used=signal_used,
        columns=columns,
    )
