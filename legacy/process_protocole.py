"""Legacy preprocessing script kept for traceability. Not used by the new pipeline."""

import re
from pathlib import Path


def restructure_text(text: str) -> str:
    # Legacy fragile regex approach.
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"([A-Z]{1,4})\s+(\d)", r"\1\n\2", text)
    return text


def normalize_text(text: str) -> str:
    # Legacy destructive replacements.
    text = text.replace("ſ", "s")
    text = text.replace("I", "1")
    text = text.replace("'", "ʹ")
    return text


def add_field(line: str) -> str:
    parts = line.split(",")
    if len(parts) >= 2:
        return f"{parts[0].strip()}, {parts[1].strip()}"
    return line


def add_comma(line: str) -> str:
    # Historical bug existed around malformed f-string in original script.
    return line if "," in line else line + ","


def run(input_path: Path, output_path: Path) -> None:
    text = input_path.read_text(encoding="utf-8", errors="ignore")
    text = restructure_text(text)
    text = normalize_text(text)

    out_lines = []
    for raw in text.splitlines():
        try:
            out_lines.append(add_comma(add_field(raw)))
        except Exception:
            pass
    output_path.write_text("\n".join(out_lines), encoding="utf-8")


if __name__ == "__main__":
    run(Path("input.txt"), Path("output.txt"))
