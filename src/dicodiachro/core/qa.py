from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .models import KNOWN_POS, Issue, ParsedEntry
from .parsing import PAGE_MARKER_RE, SECTION_RE

ENTRY_LOOSE_RE = re.compile(r"^(\d+)\s+(.+?),\s*([^\s,])$")
ENTRY_NO_COMMA_RE = re.compile(r"^([1-9]|10)\s+(.+)\s+([avpſs])$")


def load_s_vs_f(path: Path) -> set[str]:
    if not path.exists():
        return set()
    values: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        token = line.strip()
        if token and not token.startswith("#"):
            values.add(token)
    return values


def validate_entries(entries: list[ParsedEntry]) -> list[Issue]:
    issues: list[Issue] = []
    for entry in entries:
        if entry.syllables < 1 or entry.syllables > 10:
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="error",
                    code="SYLLABLES_OUT_OF_RANGE",
                    raw=entry.raw_line,
                    details={"syllables": entry.syllables},
                )
            )
        if entry.pos_raw not in KNOWN_POS:
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="error",
                    code="INVALID_POS",
                    raw=entry.raw_line,
                    details={"pos": entry.pos_raw},
                )
            )
        if "," not in entry.raw_line:
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="warning",
                    code="MISSING_COMMA",
                    raw=entry.raw_line,
                )
            )
        if not any(ch.isalpha() for ch in entry.headword_raw):
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="warning",
                    code="NON_ALPHA_HEADWORD",
                    raw=entry.raw_line,
                )
            )
    return issues


def lint_lines(lines: list[str], dict_id: str, source_path: str) -> list[Issue]:
    issues: list[Issue] = []
    for line_no, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or SECTION_RE.match(line) or PAGE_MARKER_RE.match(line):
            continue

        if ENTRY_NO_COMMA_RE.match(line):
            issues.append(
                Issue(
                    dict_id=dict_id,
                    source_path=source_path,
                    line_no=line_no,
                    kind="warning",
                    code="MISSING_COMMA",
                    raw=raw,
                )
            )
            continue

        loose = ENTRY_LOOSE_RE.match(line)
        if loose:
            syllables = int(loose.group(1))
            pos = loose.group(3)
            if syllables < 1 or syllables > 10:
                issues.append(
                    Issue(
                        dict_id=dict_id,
                        source_path=source_path,
                        line_no=line_no,
                        kind="error",
                        code="SYLLABLES_OUT_OF_RANGE",
                        raw=raw,
                        details={"syllables": syllables},
                    )
                )
            if pos not in KNOWN_POS:
                issues.append(
                    Issue(
                        dict_id=dict_id,
                        source_path=source_path,
                        line_no=line_no,
                        kind="error",
                        code="INVALID_POS",
                        raw=raw,
                        details={"pos": pos},
                    )
                )
    return issues


def warn_s_vs_f(entries: list[ParsedEntry], dict_id: str, lexicon: set[str]) -> list[Issue]:
    if not lexicon:
        return []
    issues: list[Issue] = []
    for entry in entries:
        lowered = entry.headword_raw.lower()
        if lowered in lexicon and "f" in lowered:
            issues.append(
                Issue(
                    dict_id=dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="warning",
                    code="S_VS_F_CHECK",
                    raw=entry.raw_line,
                    details={"headword": entry.headword_raw},
                )
            )
    return issues


def export_issues_csv(issues: list[Issue], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dict_id",
                "source_path",
                "line_no",
                "kind",
                "code",
                "raw",
                "details_json",
                "created_at",
            ]
        )
        for issue in issues:
            writer.writerow(
                [
                    issue.dict_id,
                    issue.source_path,
                    issue.line_no,
                    issue.kind,
                    issue.code,
                    issue.raw,
                    json.dumps(issue.details, ensure_ascii=False, sort_keys=True),
                    issue.created_at,
                ]
            )
