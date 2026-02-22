from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .models import KNOWN_POS, Issue, ParsedEntry, ProfileApplied, ProfileSpec
from .parsing import PAGE_MARKER_RE, SECTION_RE

ENTRY_LOOSE_RE = re.compile(r"^(\d+)\s+(.+?),\s*([^\s,])$")
ENTRY_NO_COMMA_RE = re.compile(r"^([1-9]|10)\s+(.+)\s+([avpſs])$")
PROFILE_AWARE_CODES = {
    "UNKNOWN_SYMBOL",
    "DETACHED_COMBINING_MARK",
    "MULTIPLE_PRIMARY_STRESS",
    "INCONSISTENT_STRESS",
    "PROFILE_RULE_RUNTIME_ERROR",
    "PROFILE_INVALID",
}


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
        normalized_pos = (entry.pos_norm or entry.pos_raw or "").strip().lower().rstrip(".")
        if normalized_pos == "f":
            normalized_pos = "ſ"
        if normalized_pos not in KNOWN_POS:
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="error",
                    code="INVALID_POS",
                    raw=entry.raw_line,
                    details={
                        "pos_raw": entry.pos_raw,
                        "pos_norm": entry.pos_norm,
                        "validated_pos": normalized_pos,
                    },
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
        if not line or line.startswith("#") or SECTION_RE.match(line) or PAGE_MARKER_RE.match(line):
            continue

        if "," not in line and ENTRY_NO_COMMA_RE.match(line):
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


def validate_profile_applied(
    *,
    entry: ParsedEntry,
    entry_id: str,
    profile: ProfileSpec,
    applied: ProfileApplied,
) -> list[Issue]:
    issues: list[Issue] = []

    detached_count = int(applied.features.get("combining_detached_count", 0) or 0)
    if detached_count > 0:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="DETACHED_COMBINING_MARK",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                    "count": detached_count,
                },
            )
        )

    if applied.unknown_symbols:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="UNKNOWN_SYMBOL",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                    "symbols": applied.unknown_symbols,
                },
            )
        )

    primary_stress_count = int(applied.features.get("primary_stress_count", 0) or 0)
    if primary_stress_count > 1:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="MULTIPLE_PRIMARY_STRESS",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                    "primary_stress_count": primary_stress_count,
                },
            )
        )

    qa_cfg = profile.qa if isinstance(profile.qa, dict) else {}
    enforce_stress_consistency = bool(qa_cfg.get("enforce_stress_consistency", False))
    require_prime = bool(qa_cfg.get("require_prime_for_primary_stress", False))
    require_acute = bool(qa_cfg.get("require_acute_for_primary_stress", False))

    marks = profile.features.get("marks") if isinstance(profile.features, dict) else {}
    if not isinstance(marks, dict):
        marks = {}
    if require_prime and not isinstance(marks.get("prime"), str):
        require_prime = False
    if require_acute and not isinstance(marks.get("acute_vowels"), str):
        require_acute = False

    prime_count = int(applied.features.get("prime_count", 0) or 0)
    accented_vowel_count = int(applied.features.get("accented_vowel_count", 0) or 0)
    inconsistent = False
    if enforce_stress_consistency:
        if require_prime and accented_vowel_count > 0 and prime_count == 0:
            inconsistent = True
        if require_acute and prime_count > 0 and accented_vowel_count == 0:
            inconsistent = True

    if inconsistent:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="INCONSISTENT_STRESS",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                    "prime_count": prime_count,
                    "accented_vowel_count": accented_vowel_count,
                    "enforce_stress_consistency": enforce_stress_consistency,
                    "require_prime_for_primary_stress": require_prime,
                    "require_acute_for_primary_stress": require_acute,
                },
            )
        )

    for warning_code in applied.warnings:
        if warning_code == "PROFILE_RULE_RUNTIME_ERROR":
            issues.append(
                Issue(
                    dict_id=entry.dict_id,
                    source_path=entry.source_path,
                    line_no=entry.line_no,
                    kind="error",
                    code="PROFILE_RULE_RUNTIME_ERROR",
                    raw=entry.raw_line,
                    details={
                        "entry_id": entry_id,
                        "profile_id": profile.profile_id,
                    },
                )
            )

    return issues
