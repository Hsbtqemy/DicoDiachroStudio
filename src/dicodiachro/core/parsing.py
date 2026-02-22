from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .models import Issue, ParsedEntry
from .parsers.presets import parse_line_with_preset

if TYPE_CHECKING:
    from .parsers.presets import ParserPresetSpec

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
    parser_preset: ParserPresetSpec | None = None,
    parser_sha256: str | None = None,
) -> tuple[list[ParsedEntry], list[Issue]]:
    entries: list[ParsedEntry] = []
    issues: list[Issue] = []
    current_section = ""

    for line_no, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
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

        if parser_preset is not None:
            parsed = parse_line_with_preset(line, parser_preset)
            if parsed.matched:
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
                        syllables=int(parsed.values["syllables"]),
                        headword_raw=str(parsed.values["headword_raw"]).strip(),
                        pos_raw=str(parsed.values["pos_raw"]).strip(),
                        pron_raw=str(
                            parsed.values.get("pron_raw") or parsed.values["headword_raw"]
                        ),
                        source_path=source_path,
                        line_no=line_no,
                        raw_line=raw_line.rstrip("\n"),
                        origin_raw=(
                            str(parsed.values["origin_raw"]).strip()
                            if "origin_raw" in parsed.values
                            else None
                        ),
                        origin_norm=(
                            str(parsed.values["origin_norm"]).strip()
                            if "origin_norm" in parsed.values
                            else None
                        ),
                        pos_norm=(
                            str(parsed.values["pos_norm"]).strip()
                            if "pos_norm" in parsed.values
                            else None
                        ),
                        parser_id=parser_preset.parser_id,
                        parser_version=parser_preset.version,
                        parser_sha256=parser_sha256,
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
                    parser_id=parser_preset.parser_id if parser_preset else None,
                    parser_version=parser_preset.version if parser_preset else None,
                    parser_sha256=parser_sha256 if parser_preset else None,
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
