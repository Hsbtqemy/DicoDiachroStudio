"""Legacy script kept for traceability. Not used by the new pipeline."""

import argparse
from pathlib import Path

ACCENTS = "áàâäéèêëíìîïóòôöúùûü"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--startswith", type=int, default=1)
    args = parser.parse_args()

    # Legacy behavior: repeated reads + simplistic prefix matching.
    lines = args.input.read_text(encoding="utf-8").splitlines()
    for i in range(1, 11):
        if i == args.startswith:
            for line in lines:
                if line.startswith(str(i)):
                    print(line)

    lines2 = args.input.read_text(encoding="utf-8").splitlines()
    for line in lines2:
        if any(ch in ACCENTS for ch in line):
            print(line)


if __name__ == "__main__":
    main()
