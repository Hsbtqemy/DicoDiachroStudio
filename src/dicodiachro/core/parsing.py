from __future__ import annotations

import re
from collections.abc import Iterable

from .models import Issue, ParsedEntry

ENTRY_RE = re.compile(r"^([1-9]|10)\s+(.+?),\s*([avpſs])$")
SECTION_RE = re.compile(r"^[A-ZΑ-ΩΪΫ]{1,4}$")
PAGE_MARKER_RE = re.compile(r"^\d+$")

GREEK_TO_LATIN = {
    "Α": "A",
    "Β": "B",
    "Γ": "G",
    "Δ": "D",
    "Ε": "E",
    "Ζ": "Z",
    "Η": "H",
    "Θ": "TH",
    "Ι": "I",
    "Κ": "K",
    "Λ": "L",
    "Μ": "M",
    "Ν": "N",
    "Ξ": "X",
    "Ο": "O",
    "Π": "P",
    "Ρ": "R",
    "Σ": "S",
    "Τ": "T",
    "Υ": "Y",
    "Φ": "PH",
    "Χ": "CH",
    "Ψ": "PS",
    "Ω": "O",
}


def normalize_section(label: str) -> str:
    out = []
    for ch in label.strip().upper():
        out.append(GREEK_TO_LATIN.get(ch, ch))
    return "".join(out)


def parse_lines(
    lines: Iterable[str],
    dict_id: str,
    source_path: str,
) -> tuple[list[ParsedEntry], list[Issue]]:
    entries: list[ParsedEntry] = []
    issues: list[Issue] = []
    current_section = ""

    for line_no, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line:
            continue

        if SECTION_RE.match(line):
            current_section = normalize_section(line)
            continue

        if PAGE_MARKER_RE.match(line):
            issues.append(
                Issue(
                    dict_id=dict_id,
                    source_path=source_path,
                    line_no=line_no,
                    kind="warning",
                    code="PAGE_MARKER",
                    raw=raw_line.rstrip("\n"),
                )
            )
            continue

        match = ENTRY_RE.match(line)
        if match:
            syllables = int(match.group(1))
            token = match.group(2).strip()
            pos_raw = match.group(3)
            if not current_section:
                issues.append(
                    Issue(
                        dict_id=dict_id,
                        source_path=source_path,
                        line_no=line_no,
                        kind="warning",
                        code="MISSING_SECTION",
                        raw=raw_line.rstrip("\n"),
                    )
                )
            entries.append(
                ParsedEntry(
                    dict_id=dict_id,
                    section=current_section,
                    syllables=syllables,
                    headword_raw=token,
                    pos_raw=pos_raw,
                    pron_raw=token,
                    source_path=source_path,
                    line_no=line_no,
                    raw_line=raw_line.rstrip("\n"),
                )
            )
            continue

        issues.append(
            Issue(
                dict_id=dict_id,
                source_path=source_path,
                line_no=line_no,
                kind="error",
                code="UNPARSED_LINE",
                raw=raw_line.rstrip("\n"),
            )
        )

    return entries, issues
