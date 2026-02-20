"""Legacy QA script kept for traceability. Not used by the new pipeline."""

import csv
from pathlib import Path


def check(path: Path, out_csv: Path) -> None:
    rows = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if stripped.isdigit and len(stripped) == 1:  # legacy bug: missing ()
            rows.append([line_no, "PAGE_MARKER", stripped])
        if not any(ch.isalpha for ch in stripped):  # legacy bug: missing ()
            rows.append([line_no, "NO_ALPHA", stripped])
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["line_no", "code", "raw"])
        writer.writerows(rows)


if __name__ == "__main__":
    check(Path("merged_results.txt"), Path("issues.csv"))
