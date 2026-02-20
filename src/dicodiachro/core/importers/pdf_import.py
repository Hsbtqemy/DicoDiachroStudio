from __future__ import annotations

from pathlib import Path


def _group_words_by_line(words: list[dict], y_tolerance: float = 2.5) -> list[str]:
    if not words:
        return []
    words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: list[list[dict]] = []
    current: list[dict] = [words[0]]
    current_top = words[0]["top"]
    for word in words[1:]:
        if abs(word["top"] - current_top) <= y_tolerance:
            current.append(word)
        else:
            lines.append(sorted(current, key=lambda w: w["x0"]))
            current = [word]
            current_top = word["top"]
    lines.append(sorted(current, key=lambda w: w["x0"]))
    return [" ".join(w.get("text", "") for w in line if w.get("text")) for line in lines]


def extract_pdf_text(pdf_path: Path, use_coords: bool = False) -> list[str]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise RuntimeError(
            "pdfplumber is not installed. Install with `pip install 'dicodiachro[pdf]'`."
        ) from exc

    lines: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            if use_coords:
                words = page.extract_words(
                    use_text_flow=True,
                    keep_blank_chars=False,
                    extra_attrs=["fontname", "size"],
                )
                lines.extend(_group_words_by_line(words))
            else:
                text = page.extract_text(layout=True) or ""
                lines.extend(text.splitlines())
    return lines


def save_pdf_as_text(pdf_path: Path, target_txt_path: Path, use_coords: bool = False) -> Path:
    lines = extract_pdf_text(pdf_path, use_coords=use_coords)
    target_txt_path.parent.mkdir(parents=True, exist_ok=True)
    target_txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target_txt_path
