from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ALLOWED_TOP_LEVEL_KEYS = {
    "parser_id",
    "version",
    "entry_regex",
    "fields",
    "pos_map",
    "origin_map",
    "allow_extra_trailing",
}
ALLOWED_FIELD_KEYS = {"syllables", "headword_raw", "pos_raw", "origin_raw", "pron_raw"}
REQUIRED_FIELD_KEYS = {"syllables", "headword_raw", "pos_raw"}


class ParserPresetValidationError(ValueError):
    def __init__(self, errors: list[str], warnings: list[str] | None = None) -> None:
        self.errors = errors
        self.warnings = warnings or []
        message = "; ".join(errors) if errors else "Parser preset validation error"
        super().__init__(message)


@dataclass(slots=True)
class ParserPresetSpec:
    parser_id: str
    version: int
    entry_regex: str
    fields: dict[str, int]
    pos_map: dict[str, str] = field(default_factory=dict)
    origin_map: dict[str, str] = field(default_factory=dict)
    allow_extra_trailing: bool = False
    validation_warnings: list[str] = field(default_factory=list)

    def compile_regex(self) -> re.Pattern[str]:
        return re.compile(self.entry_regex)


@dataclass(slots=True)
class ParserPresetResult:
    matched: bool
    values: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ResolvedParserPreset:
    path: Path
    spec: ParserPresetSpec
    sha256: str
    auto_selected: bool = False


def _normalize_eol(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def preset_sha256(preset_yaml_text: str) -> str:
    normalized = _normalize_eol(preset_yaml_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def preset_sha256_from_path(path: Path) -> str:
    return preset_sha256(path.read_text(encoding="utf-8"))


def _repo_template_parser_dir() -> Path:
    return Path(__file__).resolve().parents[4] / "rules" / "templates" / "parsers"


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ParserPresetValidationError([f"Invalid YAML syntax: {exc}"]) from exc
    if not isinstance(payload, dict):
        raise ParserPresetValidationError(["Parser preset root must be a mapping"])
    return payload


def _is_string_map(value: Any) -> bool:
    return isinstance(value, dict) and all(
        isinstance(k, str) and isinstance(v, str) for k, v in value.items()
    )


def load_parser_preset(path: Path, strict: bool = False) -> ParserPresetSpec:
    payload = _load_yaml(path)
    errors: list[str] = []
    warnings: list[str] = []

    unknown_keys = sorted(set(payload.keys()) - ALLOWED_TOP_LEVEL_KEYS)
    if unknown_keys:
        message = f"Unknown parser preset keys: {', '.join(unknown_keys)}"
        if strict:
            errors.append(message)
        else:
            warnings.append(message)

    parser_id = payload.get("parser_id")
    if not isinstance(parser_id, str) or not parser_id.strip():
        errors.append("parser_id (str) is required")

    version = payload.get("version")
    if not isinstance(version, int):
        errors.append("version (int) is required")

    entry_regex = payload.get("entry_regex")
    pattern: re.Pattern[str] | None = None
    if not isinstance(entry_regex, str) or not entry_regex.strip():
        errors.append("entry_regex (str) is required")
    else:
        try:
            pattern = re.compile(entry_regex)
        except re.error as exc:
            errors.append(f"entry_regex is invalid: {exc}")

    fields = payload.get("fields")
    if not isinstance(fields, dict):
        errors.append("fields (mapping) is required")
        fields = {}

    normalized_fields: dict[str, int] = {}
    for key, value in fields.items():
        if key not in ALLOWED_FIELD_KEYS:
            message = f"Unknown fields key: {key}"
            if strict:
                errors.append(message)
            else:
                warnings.append(message)
            continue
        if not isinstance(value, int) or value < 1:
            errors.append(f"fields.{key} must be a positive integer capture index")
            continue
        normalized_fields[key] = value

    missing_required = sorted(REQUIRED_FIELD_KEYS - set(normalized_fields.keys()))
    if missing_required:
        errors.append(f"fields missing required keys: {', '.join(missing_required)}")

    if pattern is not None:
        for key, group_index in normalized_fields.items():
            if group_index > pattern.groups:
                errors.append(
                    f"fields.{key} group index {group_index} exceeds regex capture count {pattern.groups}"
                )

    pos_map = payload.get("pos_map") or {}
    if not _is_string_map(pos_map):
        errors.append("pos_map must be a mapping of string to string")
        pos_map = {}

    origin_map = payload.get("origin_map") or {}
    if not _is_string_map(origin_map):
        errors.append("origin_map must be a mapping of string to string")
        origin_map = {}

    allow_extra_trailing = payload.get("allow_extra_trailing", False)
    if not isinstance(allow_extra_trailing, bool):
        errors.append("allow_extra_trailing must be a boolean")
        allow_extra_trailing = False

    if errors:
        raise ParserPresetValidationError(errors=errors, warnings=warnings)

    return ParserPresetSpec(
        parser_id=parser_id.strip(),
        version=version,
        entry_regex=entry_regex,
        fields=normalized_fields,
        pos_map=pos_map,
        origin_map=origin_map,
        allow_extra_trailing=allow_extra_trailing,
        validation_warnings=warnings,
    )


def _map_value(raw: str, mapping: dict[str, str]) -> str | None:
    if raw in mapping:
        return mapping[raw]
    lowered = raw.lower()
    for key, value in mapping.items():
        if key.lower() == lowered:
            return value
    return None


def parse_line_with_preset(line: str, spec: ParserPresetSpec) -> ParserPresetResult:
    pattern = spec.compile_regex()
    match = pattern.match(line)
    candidate = line
    if match is None and spec.allow_extra_trailing:
        candidate = line.rstrip().rstrip(" .;:,")
        match = pattern.match(candidate)

    if match is None:
        return ParserPresetResult(matched=False)

    values: dict[str, Any] = {}
    warnings: list[str] = []
    for field_name, group_index in spec.fields.items():
        value = match.group(group_index).strip()
        values[field_name] = value

    syllables_raw = str(values.get("syllables", "")).strip()
    try:
        values["syllables"] = int(syllables_raw)
    except ValueError:
        warnings.append("INVALID_SYLLABLES")
        return ParserPresetResult(matched=False, values=values, warnings=warnings)

    pos_raw = str(values.get("pos_raw", "")).strip()
    if spec.pos_map:
        mapped_pos = _map_value(pos_raw, spec.pos_map)
        if mapped_pos is not None:
            values["pos_norm"] = mapped_pos

    origin_raw = values.get("origin_raw")
    if isinstance(origin_raw, str) and spec.origin_map:
        mapped_origin = _map_value(origin_raw.strip(), spec.origin_map)
        if mapped_origin is not None:
            values["origin_norm"] = mapped_origin

    if "pron_raw" not in values:
        values["pron_raw"] = values.get("headword_raw", "")

    return ParserPresetResult(matched=True, values=values, warnings=warnings)


def discover_presets(project_rules_dir: Path, dict_id: str | None = None) -> list[Path]:
    candidate_dirs: list[Path] = []
    if dict_id:
        candidate_dirs.append(project_rules_dir / dict_id)
    candidate_dirs.extend(
        [
            project_rules_dir / "parsers",
            project_rules_dir,
            Path("rules") / "templates" / "parsers",
            _repo_template_parser_dir(),
        ]
    )

    seen: set[Path] = set()
    valid_paths: list[Path] = []
    for directory in candidate_dirs:
        if not directory.exists() or not directory.is_dir():
            continue
        for pattern in ("*.yml", "*.yaml"):
            for path in sorted(directory.glob(pattern)):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                try:
                    load_parser_preset(resolved)
                except ParserPresetValidationError:
                    continue
                valid_paths.append(resolved)
    return sorted(valid_paths)


def _auto_resolve_dict_preset(project_rules_dir: Path, dict_id: str) -> ResolvedParserPreset | None:
    dict_dir = project_rules_dir / dict_id
    if not dict_dir.exists() or not dict_dir.is_dir():
        return None

    ordered_candidates: list[Path] = []
    for name in ["parser.yml", "parser.yaml", "parser_v1.yml", "parser_v1.yaml"]:
        candidate = dict_dir / name
        if candidate.exists():
            ordered_candidates.append(candidate)
    for pattern in ("*.yml", "*.yaml"):
        for path in sorted(dict_dir.glob(pattern)):
            if path not in ordered_candidates:
                ordered_candidates.append(path)

    for path in ordered_candidates:
        try:
            spec = load_parser_preset(path)
        except ParserPresetValidationError:
            continue
        return ResolvedParserPreset(
            path=path.resolve(),
            spec=spec,
            sha256=preset_sha256_from_path(path),
            auto_selected=True,
        )
    return None


def resolve_preset(
    project_rules_dir: Path,
    dict_id: str,
    parser_arg: str | None = None,
) -> ResolvedParserPreset | None:
    parser_ref = (parser_arg or "").strip()
    if not parser_ref or parser_ref.lower() == "auto":
        return _auto_resolve_dict_preset(project_rules_dir, dict_id)

    parser_path = Path(parser_ref)
    if parser_path.exists():
        spec = load_parser_preset(parser_path)
        return ResolvedParserPreset(
            path=parser_path.resolve(),
            spec=spec,
            sha256=preset_sha256_from_path(parser_path),
            auto_selected=False,
        )

    for candidate in discover_presets(project_rules_dir, dict_id=dict_id):
        spec = load_parser_preset(candidate)
        if spec.parser_id == parser_ref or candidate.stem == parser_ref:
            return ResolvedParserPreset(
                path=candidate.resolve(),
                spec=spec,
                sha256=preset_sha256_from_path(candidate),
                auto_selected=False,
            )

    raise FileNotFoundError(f"Parser preset not found: {parser_ref}")
