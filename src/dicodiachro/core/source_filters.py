from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ALLOWED_TOP_KEYS = {"enabled", "default", "sources"}
_ALLOWED_RULE_KEYS = {
    "exclude_line_ranges",
    "drop_before_regex",
    "drop_after_regex",
    "drop_line_regexes",
    "case_insensitive",
}


class SourceFilterValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(slots=True)
class SourceFilterRule:
    exclude_line_ranges: list[tuple[int, int]]
    drop_before_regex: str | None
    drop_after_regex: str | None
    drop_line_regexes: list[str]
    case_insensitive: bool = True

    def has_effect(self) -> bool:
        return bool(
            self.exclude_line_ranges
            or self.drop_before_regex
            or self.drop_after_regex
            or self.drop_line_regexes
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "exclude_line_ranges": [f"{start}-{end}" for start, end in self.exclude_line_ranges],
            "drop_before_regex": self.drop_before_regex,
            "drop_after_regex": self.drop_after_regex,
            "drop_line_regexes": list(self.drop_line_regexes),
            "case_insensitive": self.case_insensitive,
        }


@dataclass(slots=True)
class SourceFilterConfig:
    path: Path
    enabled: bool
    default_rule: SourceFilterRule
    source_overrides: list[tuple[str, dict[str, Any]]]
    project_root: Path | None = None


@dataclass(slots=True)
class SourceFilterResult:
    lines: list[str]
    dropped_line_numbers: list[int]
    report: dict[str, Any]


def _parse_range_token(raw: Any, context: str, errors: list[str]) -> tuple[int, int] | None:
    if isinstance(raw, int):
        if raw < 1:
            errors.append(f"{context} must be >= 1")
            return None
        return raw, raw

    text = str(raw).strip()
    if not text:
        errors.append(f"{context} cannot be empty")
        return None

    if "-" in text:
        left, right = [part.strip() for part in text.split("-", 1)]
        if not left.isdigit() or not right.isdigit():
            errors.append(f"{context} must be N or N-M")
            return None
        start = int(left)
        end = int(right)
    else:
        if not text.isdigit():
            errors.append(f"{context} must be N or N-M")
            return None
        start = int(text)
        end = int(text)

    if start < 1 or end < 1:
        errors.append(f"{context} values must be >= 1")
        return None
    if end < start:
        errors.append(f"{context} end must be >= start")
        return None
    return start, end


def _validate_regex(text: str, context: str, errors: list[str]) -> None:
    try:
        re.compile(text)
    except re.error as exc:
        errors.append(f"{context} invalid regex: {exc}")


def _parse_rule_mapping(
    payload: dict[str, Any],
    *,
    context: str,
    partial: bool,
    errors: list[str],
) -> dict[str, Any]:
    unknown = sorted(set(payload.keys()) - _ALLOWED_RULE_KEYS)
    for key in unknown:
        errors.append(f"Unknown key: {context}.{key}")

    parsed: dict[str, Any] = {}

    if "exclude_line_ranges" in payload:
        ranges_raw = payload.get("exclude_line_ranges")
        if ranges_raw is None:
            parsed["exclude_line_ranges"] = []
        elif not isinstance(ranges_raw, list):
            errors.append(f"{context}.exclude_line_ranges must be a list")
        else:
            ranges: list[tuple[int, int]] = []
            for idx, item in enumerate(ranges_raw):
                parsed_range = _parse_range_token(item, f"{context}.exclude_line_ranges[{idx}]", errors)
                if parsed_range is not None:
                    ranges.append(parsed_range)
            parsed["exclude_line_ranges"] = ranges
    elif not partial:
        parsed["exclude_line_ranges"] = []

    if "drop_before_regex" in payload:
        before_raw = payload.get("drop_before_regex")
        if before_raw is None:
            parsed["drop_before_regex"] = None
        elif not isinstance(before_raw, str):
            errors.append(f"{context}.drop_before_regex must be a string or null")
        else:
            _validate_regex(before_raw, f"{context}.drop_before_regex", errors)
            parsed["drop_before_regex"] = before_raw
    elif not partial:
        parsed["drop_before_regex"] = None

    if "drop_after_regex" in payload:
        after_raw = payload.get("drop_after_regex")
        if after_raw is None:
            parsed["drop_after_regex"] = None
        elif not isinstance(after_raw, str):
            errors.append(f"{context}.drop_after_regex must be a string or null")
        else:
            _validate_regex(after_raw, f"{context}.drop_after_regex", errors)
            parsed["drop_after_regex"] = after_raw
    elif not partial:
        parsed["drop_after_regex"] = None

    if "drop_line_regexes" in payload:
        lines_raw = payload.get("drop_line_regexes")
        if lines_raw is None:
            parsed["drop_line_regexes"] = []
        elif not isinstance(lines_raw, list):
            errors.append(f"{context}.drop_line_regexes must be a list")
        else:
            regexes: list[str] = []
            for idx, item in enumerate(lines_raw):
                if not isinstance(item, str):
                    errors.append(f"{context}.drop_line_regexes[{idx}] must be a string")
                    continue
                _validate_regex(item, f"{context}.drop_line_regexes[{idx}]", errors)
                regexes.append(item)
            parsed["drop_line_regexes"] = regexes
    elif not partial:
        parsed["drop_line_regexes"] = []

    if "case_insensitive" in payload:
        case_raw = payload.get("case_insensitive")
        if not isinstance(case_raw, bool):
            errors.append(f"{context}.case_insensitive must be a boolean")
        else:
            parsed["case_insensitive"] = case_raw
    elif not partial:
        parsed["case_insensitive"] = True

    return parsed


def _rule_from_mapping(mapping: dict[str, Any]) -> SourceFilterRule:
    return SourceFilterRule(
        exclude_line_ranges=list(mapping.get("exclude_line_ranges", [])),
        drop_before_regex=mapping.get("drop_before_regex"),
        drop_after_regex=mapping.get("drop_after_regex"),
        drop_line_regexes=list(mapping.get("drop_line_regexes", [])),
        case_insensitive=bool(mapping.get("case_insensitive", True)),
    )


def load_source_filter_config(path: Path, project_root: Path | None = None) -> SourceFilterConfig:
    errors: list[str] = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourceFilterValidationError([f"Unable to read source filters file: {exc}"]) from exc

    try:
        payload = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise SourceFilterValidationError([f"Invalid YAML: {exc}"]) from exc

    if not isinstance(payload, dict):
        raise SourceFilterValidationError(["Source filters YAML root must be a mapping"])

    unknown_top = sorted(set(payload.keys()) - _ALLOWED_TOP_KEYS)
    for key in unknown_top:
        errors.append(f"Unknown key: {key}")

    enabled_raw = payload.get("enabled", True)
    if not isinstance(enabled_raw, bool):
        errors.append("enabled must be a boolean")
        enabled = True
    else:
        enabled = enabled_raw

    default_raw = payload.get("default", {})
    if default_raw is None:
        default_raw = {}
    if not isinstance(default_raw, dict):
        errors.append("default must be a mapping")
        default_raw = {}
    default_rule_mapping = _parse_rule_mapping(
        default_raw,
        context="default",
        partial=False,
        errors=errors,
    )

    source_overrides: list[tuple[str, dict[str, Any]]] = []
    sources_raw = payload.get("sources", {})
    if sources_raw is None:
        sources_raw = {}
    if not isinstance(sources_raw, dict):
        errors.append("sources must be a mapping")
        sources_raw = {}
    for key, item in sources_raw.items():
        pattern = str(key).strip()
        if not pattern:
            errors.append("sources keys must be non-empty strings")
            continue
        if item is None:
            item = {}
        if not isinstance(item, dict):
            errors.append(f"sources.{pattern} must be a mapping")
            continue
        override_mapping = _parse_rule_mapping(
            item,
            context=f"sources.{pattern}",
            partial=True,
            errors=errors,
        )
        source_overrides.append((pattern, override_mapping))

    if errors:
        raise SourceFilterValidationError(errors)

    return SourceFilterConfig(
        path=path.resolve(),
        enabled=enabled,
        default_rule=_rule_from_mapping(default_rule_mapping),
        source_overrides=source_overrides,
        project_root=project_root.resolve() if project_root else None,
    )


def discover_source_filter_config_path(rules_dir: Path, dict_id: str | None = None) -> Path | None:
    rules_root = rules_dir.resolve()
    candidates: list[Path] = []
    if dict_id:
        cleaned = dict_id.strip()
        if cleaned:
            candidates.extend(
                [
                    rules_root / cleaned / "source_filters.yml",
                    rules_root / cleaned / "source_filters.yaml",
                ]
            )
    candidates.extend(
        [
            rules_root / "source_filters" / "source_filters.yml",
            rules_root / "source_filters" / "source_filters.yaml",
            rules_root / "source_filters.yml",
            rules_root / "source_filters.yaml",
        ]
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def load_project_source_filters(
    rules_dir: Path,
    dict_id: str | None = None,
) -> SourceFilterConfig | None:
    config_path = discover_source_filter_config_path(rules_dir, dict_id=dict_id)
    if config_path is None:
        return None
    project_root = rules_dir.resolve().parent
    return load_source_filter_config(config_path, project_root=project_root)


def _merge_rule(
    default_rule: SourceFilterRule,
    overrides: list[dict[str, Any]],
) -> SourceFilterRule:
    merged = default_rule.as_dict()
    merged["exclude_line_ranges"] = list(default_rule.exclude_line_ranges)
    merged["drop_line_regexes"] = list(default_rule.drop_line_regexes)
    for item in overrides:
        for key, value in item.items():
            merged[key] = value
    normalized_ranges = merged.get("exclude_line_ranges", [])
    if isinstance(normalized_ranges, list):
        normalized_ranges = [
            tuple(pair) if isinstance(pair, tuple) else pair for pair in normalized_ranges
        ]
    merged["exclude_line_ranges"] = normalized_ranges
    return _rule_from_mapping(merged)


def _source_match_candidates(source_path: Path, project_root: Path | None = None) -> list[str]:
    resolved = source_path.expanduser().resolve()
    candidates = [resolved.as_posix(), resolved.name]

    if project_root:
        root = project_root.resolve()
        try:
            candidates.append(resolved.relative_to(root).as_posix())
        except ValueError:
            pass
        imports_dir = root / "data" / "raw" / "imports"
        try:
            candidates.append(resolved.relative_to(imports_dir).as_posix())
        except ValueError:
            pass

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.replace("\\", "/")
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _pattern_matches_source(pattern: str, candidates: list[str]) -> bool:
    normalized = pattern.replace("\\", "/").strip().casefold()
    if not normalized:
        return False
    for candidate in candidates:
        if fnmatch.fnmatchcase(candidate.casefold(), normalized):
            return True
    return False


def resolve_source_filter_rule(
    config: SourceFilterConfig,
    source_path: Path,
) -> tuple[SourceFilterRule, list[str]]:
    candidates = _source_match_candidates(source_path, project_root=config.project_root)
    matched_patterns: list[str] = []
    matched_overrides: list[dict[str, Any]] = []
    for pattern, override in config.source_overrides:
        if _pattern_matches_source(pattern, candidates):
            matched_patterns.append(pattern)
            matched_overrides.append(override)
    return _merge_rule(config.default_rule, matched_overrides), matched_patterns


def _drop_range(
    keep_flags: list[bool],
    start: int,
    end: int,
) -> int:
    dropped = 0
    left = max(start - 1, 0)
    right = min(end, len(keep_flags))
    for idx in range(left, right):
        if keep_flags[idx]:
            keep_flags[idx] = False
            dropped += 1
    return dropped


def _drop_span(keep_flags: list[bool], start: int, end: int) -> int:
    dropped = 0
    for idx in range(max(start, 0), min(end, len(keep_flags))):
        if keep_flags[idx]:
            keep_flags[idx] = False
            dropped += 1
    return dropped


def _first_regex_match(lines: list[str], pattern: re.Pattern[str]) -> int | None:
    for idx, line in enumerate(lines):
        if pattern.search(line):
            return idx
    return None


def apply_source_filters(
    lines: list[str],
    source_path: Path,
    config: SourceFilterConfig | None,
) -> SourceFilterResult:
    source = source_path.expanduser().resolve()
    if config is None:
        return SourceFilterResult(
            lines=list(lines),
            dropped_line_numbers=[],
            report={
                "source_path": str(source),
                "config_path": None,
                "enabled": False,
                "applied": False,
                "matched_source_patterns": [],
                "rule": None,
                "total_lines": len(lines),
                "kept_lines": len(lines),
                "dropped_lines": 0,
                "dropped_by_ranges": 0,
                "dropped_before_regex": 0,
                "dropped_after_regex": 0,
                "dropped_by_line_regex": 0,
                "drop_before_matched_line": None,
                "drop_after_matched_line": None,
            },
        )

    rule, matched_patterns = resolve_source_filter_rule(config, source)
    if not config.enabled or not rule.has_effect():
        return SourceFilterResult(
            lines=list(lines),
            dropped_line_numbers=[],
            report={
                "source_path": str(source),
                "config_path": str(config.path),
                "enabled": bool(config.enabled),
                "applied": False,
                "matched_source_patterns": matched_patterns,
                "rule": rule.as_dict(),
                "total_lines": len(lines),
                "kept_lines": len(lines),
                "dropped_lines": 0,
                "dropped_by_ranges": 0,
                "dropped_before_regex": 0,
                "dropped_after_regex": 0,
                "dropped_by_line_regex": 0,
                "drop_before_matched_line": None,
                "drop_after_matched_line": None,
            },
        )

    flags = re.IGNORECASE if rule.case_insensitive else 0
    keep = [True] * len(lines)

    dropped_by_ranges = 0
    dropped_before_regex = 0
    dropped_after_regex = 0
    dropped_by_line_regex = 0
    drop_before_matched_line: int | None = None
    drop_after_matched_line: int | None = None

    if rule.drop_before_regex:
        before_pattern = re.compile(rule.drop_before_regex, flags)
        matched = _first_regex_match(lines, before_pattern)
        if matched is not None:
            drop_before_matched_line = matched + 1
            dropped_before_regex += _drop_span(keep, 0, matched)

    if rule.drop_after_regex:
        after_pattern = re.compile(rule.drop_after_regex, flags)
        matched = _first_regex_match(lines, after_pattern)
        if matched is not None:
            drop_after_matched_line = matched + 1
            dropped_after_regex += _drop_span(keep, matched, len(lines))

    for start, end in rule.exclude_line_ranges:
        dropped_by_ranges += _drop_range(keep, start, end)

    drop_line_patterns = [re.compile(pattern, flags) for pattern in rule.drop_line_regexes]
    for idx, raw in enumerate(lines):
        if not keep[idx]:
            continue
        if any(pattern.search(raw) for pattern in drop_line_patterns):
            keep[idx] = False
            dropped_by_line_regex += 1

    dropped_line_numbers = [idx + 1 for idx, is_kept in enumerate(keep) if not is_kept]
    filtered_lines = [raw if keep[idx] else "" for idx, raw in enumerate(lines)]

    report = {
        "source_path": str(source),
        "config_path": str(config.path),
        "enabled": bool(config.enabled),
        "applied": True,
        "matched_source_patterns": matched_patterns,
        "rule": rule.as_dict(),
        "total_lines": len(lines),
        "kept_lines": len(lines) - len(dropped_line_numbers),
        "dropped_lines": len(dropped_line_numbers),
        "dropped_by_ranges": dropped_by_ranges,
        "dropped_before_regex": dropped_before_regex,
        "dropped_after_regex": dropped_after_regex,
        "dropped_by_line_regex": dropped_by_line_regex,
        "drop_before_matched_line": drop_before_matched_line,
        "drop_after_matched_line": drop_after_matched_line,
    }

    return SourceFilterResult(
        lines=filtered_lines,
        dropped_line_numbers=dropped_line_numbers,
        report=report,
    )


def summarize_source_filter_reports(
    config: SourceFilterConfig | None,
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "config_path": str(config.path) if config else None,
        "enabled": bool(config.enabled) if config else False,
        "sources_count": len(reports),
        "applied_sources": sum(1 for report in reports if bool(report.get("applied"))),
        "total_lines": sum(int(report.get("total_lines", 0) or 0) for report in reports),
        "kept_lines": sum(int(report.get("kept_lines", 0) or 0) for report in reports),
        "dropped_lines": sum(int(report.get("dropped_lines", 0) or 0) for report in reports),
        "dropped_by_ranges": sum(
            int(report.get("dropped_by_ranges", 0) or 0) for report in reports
        ),
        "dropped_before_regex": sum(
            int(report.get("dropped_before_regex", 0) or 0) for report in reports
        ),
        "dropped_after_regex": sum(
            int(report.get("dropped_after_regex", 0) or 0) for report in reports
        ),
        "dropped_by_line_regex": sum(
            int(report.get("dropped_by_line_regex", 0) or 0) for report in reports
        ),
        "sources": reports,
    }
