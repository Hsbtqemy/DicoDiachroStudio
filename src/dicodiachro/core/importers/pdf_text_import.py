from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from ..storage.sqlite import project_paths

NON_WS_RE = re.compile(r"\s+")


class PDFTextImportError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "PDF_TEXT_IMPORT_ERROR",
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


@dataclass(slots=True)
class PDFTextImportResult:
    source_pdf: Path
    output_text_paths: list[Path]
    pages_total: int
    pages_with_text: int
    columns: int

    def as_dict(self) -> dict[str, object]:
        return {
            "source_pdf": str(self.source_pdf),
            "output_text_paths": [str(path) for path in self.output_text_paths],
            "pages_total": self.pages_total,
            "pages_with_text": self.pages_with_text,
            "columns": self.columns,
        }


def _group_words_by_line(words: list[dict], y_tolerance: float = 2.5) -> list[str]:
    if not words:
        return []
    ordered = sorted(words, key=lambda word: (float(word["top"]), float(word["x0"])))
    lines: list[list[dict]] = []
    current: list[dict] = [ordered[0]]
    current_top = float(ordered[0]["top"])
    for word in ordered[1:]:
        top = float(word["top"])
        if abs(top - current_top) <= y_tolerance:
            current.append(word)
            continue
        lines.append(sorted(current, key=lambda item: float(item["x0"])))
        current = [word]
        current_top = top
    lines.append(sorted(current, key=lambda item: float(item["x0"])))
    return [
        " ".join(
            str(word.get("text", "")).strip() for word in line if str(word.get("text", "")).strip()
        )
        for line in lines
    ]


def _equal_width_breakpoints(width: float, columns: int) -> list[float]:
    return [width * idx / columns for idx in range(1, columns)]


def _column_breakpoints(words: list[dict], *, width: float, columns: int) -> list[float]:
    if columns <= 1:
        return []

    x_positions = sorted(
        {
            float(word.get("x0", 0.0))
            for word in words
            if str(word.get("text", "")).strip()
        }
    )
    if len(x_positions) < columns:
        return _equal_width_breakpoints(width, columns)

    gaps = [
        (x_positions[idx + 1] - x_positions[idx], idx)
        for idx in range(len(x_positions) - 1)
    ]
    positive_gaps = [item for item in gaps if item[0] > 0.0]
    if len(positive_gaps) < columns - 1:
        return _equal_width_breakpoints(width, columns)

    selected = sorted(positive_gaps, key=lambda item: item[0], reverse=True)[: columns - 1]
    selected_indices = sorted(idx for _, idx in selected)
    breakpoints = [(x_positions[idx] + x_positions[idx + 1]) / 2 for idx in selected_indices]
    if len(breakpoints) != columns - 1:
        return _equal_width_breakpoints(width, columns)
    return breakpoints


def _column_index(x0: float, breakpoints: list[float]) -> int:
    for idx, threshold in enumerate(breakpoints):
        if x0 < threshold:
            return idx
    return len(breakpoints)


def _extract_page_lines(page, *, columns: int) -> list[str]:
    words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    if not words:
        text = page.extract_text(layout=True) or page.extract_text() or ""
        return [line.strip() for line in text.splitlines() if line.strip()]

    width = max(float(page.width or 0.0), 1.0)
    breakpoints = _column_breakpoints(words, width=width, columns=columns)
    column_words: list[list[dict]] = [[] for _ in range(columns)]
    for word in words:
        text = str(word.get("text", "")).strip()
        if not text:
            continue
        x0 = float(word.get("x0", 0.0))
        index = _column_index(x0, breakpoints)
        column_words[index].append(word)

    lines: list[str] = []
    for words_in_column in column_words:
        lines.extend(_group_words_by_line(words_in_column))
    return [line.strip() for line in lines if line.strip()]


def extract_pdf_text_lines(
    pdf_path: Path,
    *,
    columns: int = 1,
    min_chars_per_page: int = 4,
) -> tuple[list[str], list[int], int, int]:
    """Extract text lines and per-line page numbers (1-based)."""
    if columns < 1 or columns > 3:
        raise PDFTextImportError(
            "columns must be between 1 and 3",
            code="PDF_INVALID_COLUMNS",
            details={"columns": columns},
        )

    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "pdfplumber is not installed. Install with `pip install 'dicodiachro[pdf]'`."
        ) from exc

    all_lines: list[str] = []
    all_line_pages: list[int] = []
    pages_total = 0
    pages_with_text = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            pages_total += 1
            page_lines = _extract_page_lines(page, columns=columns)
            text_chars = sum(len(NON_WS_RE.sub("", line)) for line in page_lines)
            if text_chars >= min_chars_per_page:
                pages_with_text += 1
            all_lines.extend(page_lines)
            all_line_pages.extend([page_no] * len(page_lines))

    if pages_total == 0 or pages_with_text == 0 or not all_lines:
        raise PDFTextImportError(
            "PDF sans couche texte. Passez-le par ABBYY (ou un OCR) puis réessayez.",
            code="PDF_NO_TEXT_LAYER",
            details={
                "pdf_path": str(pdf_path),
                "pages_total": pages_total,
                "pages_with_text": pages_with_text,
            },
        )

    return all_lines, all_line_pages, pages_total, pages_with_text


def _default_target_path(project_dir: Path, pdf_path: Path) -> Path:
    imports_dir = project_paths(project_dir).raw_dir / "imports"
    imports_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()[:12]
    return imports_dir / f"{pdf_path.stem}-{digest}.txt"


def _resolve_target_path(project_dir: Path, pdf_path: Path, out: Path | None) -> Path:
    if out is None:
        return _default_target_path(project_dir, pdf_path)
    if out.suffix.lower() == ".txt":
        out.parent.mkdir(parents=True, exist_ok=True)
        return out
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{pdf_path.stem}.txt"


def import_pdf_text(
    project_dir: Path,
    pdf_path: Path,
    *,
    out: Path | None = None,
    columns: int = 1,
) -> PDFTextImportResult:
    project_dir = project_dir.expanduser().resolve()
    source_pdf = pdf_path.expanduser().resolve()
    if not source_pdf.exists() or not source_pdf.is_file():
        raise PDFTextImportError(f"PDF introuvable: {source_pdf}")

    lines, line_pages, pages_total, pages_with_text = extract_pdf_text_lines(
        source_pdf,
        columns=columns,
    )
    target_txt = _resolve_target_path(project_dir, source_pdf, out)
    target_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    sidecar_path = target_txt.with_name(target_txt.name + ".line_pages")
    sidecar_path.write_text(
        "\n".join(str(p) for p in line_pages) + ("\n" if line_pages else ""),
        encoding="utf-8",
    )
    return PDFTextImportResult(
        source_pdf=source_pdf,
        output_text_paths=[target_txt],
        pages_total=pages_total,
        pages_with_text=pages_with_text,
        columns=columns,
    )
