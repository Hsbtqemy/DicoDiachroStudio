"""Legacy merge script kept for traceability. Not used by the new pipeline."""

from pathlib import Path


def merge(folder: Path, output: Path) -> None:
    seen = set()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as out:
        for txt in sorted(folder.glob("*.txt")):
            for line in txt.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line in seen:
                    continue
                seen.add(line)
                out.write(line + "\n")


if __name__ == "__main__":
    merge(Path("."), Path("merged_results.txt"))
