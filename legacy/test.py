"""Legacy exploratory script kept for traceability."""

from pathlib import Path


def main() -> None:
    sample = Path("sample.txt")
    if sample.exists():
        print(sample.read_text(encoding="utf-8")[:200])


if __name__ == "__main__":
    main()
