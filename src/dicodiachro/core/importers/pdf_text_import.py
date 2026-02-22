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


def _extract_page_lines(page, *, columns: int) -> list[str]:
    words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    if not words:
        text = page.extract_text(layout=True) or page.extract_text() or ""
        return [line.strip() for line in text.splitlines() if line.strip()]

    width = max(float(page.width or 0.0), 1.0)
    column_words: list[list[dict]] = [[] for _ in range(columns)]
    for word in words:
        text = str(word.get("text", "")).strip()
        if not text:
            continue
        x0 = float(word.get("x0", 0.0))
        ratio = min(max(x0 / width, 0.0), 0.999999)
        index = min(int(ratio * columns), columns - 1)
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
) -> tuple[list[str], int, int]:
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
    pages_total = 0
    pages_with_text = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            pages_total += 1
            page_lines = _extract_page_lines(page, columns=columns)
            text_chars = sum(len(NON_WS_RE.sub("", line)) for line in page_lines)
            if text_chars >= min_chars_per_page:
                pages_with_text += 1
            all_lines.extend(page_lines)

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

    return all_lines, pages_total, pages_with_text


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

    lines, pages_total, pages_with_text = extract_pdf_text_lines(
        source_pdf,
        columns=columns,
    )
    target_txt = _resolve_target_path(project_dir, source_pdf, out)
    target_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return PDFTextImportResult(
        source_pdf=source_pdf,
        output_text_paths=[target_txt],
        pages_total=pages_total,
        pages_with_text=pages_with_text,
        columns=columns,
    )
