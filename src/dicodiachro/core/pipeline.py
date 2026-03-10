from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .models import Issue, ParsedEntry
from .parsers.presets import (
    ParserPresetValidationError,
    ResolvedParserPreset,
    resolve_preset,
)
from .parsing import parse_lines
from .profiles import (
    ProfileValidationError,
    apply_profile,
    load_profile,
    load_symbols_inventory,
    profile_sha256_from_path,
)
from .qa import (
    PROFILE_AWARE_CODES,
    export_issues_csv,
    lint_lines,
    load_s_vs_f,
    validate_entries,
    validate_profile_applied,
    warn_s_vs_f,
)
from .source_filters import (
    SourceFilterConfig,
    SourceFilterValidationError,
    apply_source_filters,
    load_project_source_filters,
    summarize_source_filter_reports,
)
from .storage.sqlite import SQLiteStore, append_jsonl_log, entry_id_for, init_project, project_paths


class PipelineError(RuntimeError):
    pass


def _resolve_profile_path(project_rules_dir: Path, profile_name: str) -> Path:
    if Path(profile_name).exists():
        return Path(profile_name).resolve()
    repo_templates = Path(__file__).resolve().parents[3] / "rules" / "templates"
    candidates = [
        project_rules_dir / f"{profile_name}.yml",
        project_rules_dir / f"{profile_name}.yaml",
        project_rules_dir / profile_name / "profile.yml",
        project_rules_dir / profile_name / "profile.yaml",
        Path("rules") / "templates" / f"{profile_name}.yml",
        Path("rules") / "templates" / f"{profile_name}.yaml",
        repo_templates / f"{profile_name}.yml",
        repo_templates / f"{profile_name}.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise PipelineError(f"Profile not found: {profile_name}")


def resolve_profile_path(project_rules_dir: Path, profile_name: str) -> Path:
    return _resolve_profile_path(project_rules_dir, profile_name)


def discover_source_texts(raw_dir: Path) -> list[Path]:
    return sorted(p for p in raw_dir.rglob("*.txt") if p.is_file())


def _parse_source(
    path: Path,
    dict_id: str,
    parser: ResolvedParserPreset | None = None,
    source_filters: SourceFilterConfig | None = None,
) -> tuple[list[ParsedEntry], list[Issue], list[str], dict[str, Any]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    filtered = apply_source_filters(lines, source_path=path, config=source_filters)
    entries, parse_issues = parse_lines(
        filtered.lines,
        dict_id=dict_id,
        source_path=str(path),
        parser_preset=parser.spec if parser else None,
        parser_sha256=parser.sha256 if parser else None,
    )
    qa_issues = lint_lines(filtered.lines, dict_id=dict_id, source_path=str(path))
    qa_issues.extend(validate_entries(entries))
    return entries, parse_issues + qa_issues, filtered.lines, filtered.report


def _resolve_s_vs_f(project_dir: Path) -> Path | None:
    candidates = [
        project_dir / "rules" / "s_vs_f.txt",
        Path(__file__).resolve().parents[3] / "sample_data" / "s_vs_f.txt",
        Path("sample_data") / "s_vs_f.txt",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _profile_invalid_issue(
    dict_id: str, profile_path: Path, error: ProfileValidationError
) -> Issue:
    return Issue(
        dict_id=dict_id,
        source_path=str(profile_path),
        line_no=0,
        kind="error",
        code="PROFILE_INVALID",
        raw=profile_path.name,
        details={
            "errors": error.errors,
            "warnings": error.warnings,
        },
    )


def run_pipeline(
    project_dir: Path,
    dict_id: str,
    profile_name: str,
    source_paths: list[Path] | None = None,
    clear_existing: bool = True,
    extra_issues: list[Issue] | None = None,
    parser_name: str | None = None,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary(dict_id=dict_id, label=dict_id)

    profile_path = _resolve_profile_path(paths.rules_dir, profile_name)
    try:
        profile = load_profile(profile_path)
    except ProfileValidationError as exc:
        store.insert_issues([_profile_invalid_issue(dict_id, profile_path, exc)])
        raise PipelineError(f"Invalid profile {profile_path}: {exc}") from exc

    profile_hash = profile_sha256_from_path(profile_path)
    store.upsert_profile(profile, profile_path, profile_hash)

    resolved_parser: ResolvedParserPreset | None = None
    try:
        resolved_parser = resolve_preset(paths.rules_dir, dict_id=dict_id, parser_arg=parser_name)
    except (ParserPresetValidationError, FileNotFoundError) as exc:
        raise PipelineError(str(exc)) from exc

    if source_paths is None:
        source_paths = discover_source_texts(paths.raw_dir)
    if not source_paths:
        raise PipelineError("No .txt sources found in project data/raw.")
    try:
        source_filters = load_project_source_filters(paths.rules_dir, dict_id=dict_id)
    except SourceFilterValidationError as exc:
        raise PipelineError(f"Invalid source filters configuration: {exc}") from exc

    all_entries: list[ParsedEntry] = []
    all_issues: list[Issue] = []
    source_filter_reports: list[dict[str, Any]] = []
    symbols_inventory = load_symbols_inventory(paths.rules_dir, dict_id=dict_id)

    s_vs_f_path = _resolve_s_vs_f(project_dir)
    s_vs_f_lexicon = load_s_vs_f(s_vs_f_path) if s_vs_f_path else set()
    for source in source_paths:
        entries, issues, _, filter_report = _parse_source(
            source,
            dict_id=dict_id,
            parser=resolved_parser,
            source_filters=source_filters,
        )
        all_entries.extend(entries)
        all_issues.extend(issues)
        source_filter_reports.append(filter_report)

    source_filter_summary = summarize_source_filter_reports(source_filters, source_filter_reports)

    all_issues.extend(warn_s_vs_f(all_entries, dict_id=dict_id, lexicon=s_vs_f_lexicon))
    if extra_issues:
        all_issues.extend(extra_issues)

    applied = {}
    for entry in all_entries:
        entry_id = entry_id_for(entry)
        result = apply_profile(
            entry.pron_raw or entry.headword_raw,
            profile,
            symbols_inventory=symbols_inventory,
        )
        applied[entry_id] = result
        all_issues.extend(
            validate_profile_applied(
                entry=entry,
                entry_id=entry_id,
                profile=profile,
                applied=result,
            )
        )

    if clear_existing:
        store.clear_dict_entries(dict_id)

    store.insert_entries(all_entries, applied)
    store.insert_issues(all_issues)

    issues_csv = paths.derived_dir / f"{dict_id}_issues.csv"
    export_issues_csv(all_issues, issues_csv)

    summary = {
        "dict_id": dict_id,
        "profile": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
        "entries": len(all_entries),
        "issues": len(all_issues),
        "sources": [str(path) for path in source_paths],
        "issues_csv": str(issues_csv),
        "profile_warnings": profile.validation_warnings,
        "source_filters": source_filter_summary,
        "parser": (
            {
                "parser_id": resolved_parser.spec.parser_id,
                "parser_version": resolved_parser.spec.version,
                "parser_sha256": resolved_parser.sha256,
                "parser_path": str(resolved_parser.path),
                "auto_selected": resolved_parser.auto_selected,
            }
            if resolved_parser
            else None
        ),
    }
    store.record_profile_application(
        dict_id=dict_id,
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_sha256=profile_hash,
        entries_count=len(all_entries),
        status="success",
        details={
            "action": "run_pipeline",
            "issues": len(all_issues),
            "sources": [str(path) for path in source_paths],
            "source_filters": source_filter_summary,
            "parser": summary["parser"],
        },
    )

    append_jsonl_log(
        paths.logs_dir / "pipeline.jsonl",
        {
            "event": "run_pipeline",
            "summary": summary,
        },
    )
    return summary


def apply_profile_to_entries(project_dir: Path, dict_id: str, profile_name: str) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    profile_path = _resolve_profile_path(paths.rules_dir, profile_name)
    try:
        profile = load_profile(profile_path)
    except ProfileValidationError as exc:
        store.insert_issues([_profile_invalid_issue(dict_id, profile_path, exc)])
        raise PipelineError(f"Invalid profile {profile_path}: {exc}") from exc

    profile_hash = profile_sha256_from_path(profile_path)
    store.upsert_profile(profile, profile_path, profile_hash)

    rows = store.entries_for_dict(dict_id)
    if not rows:
        raise PipelineError(f"No entries found for dictionary {dict_id}")

    symbols_inventory = load_symbols_inventory(paths.rules_dir, dict_id=dict_id)
    applied = {}
    profile_issues: list[Issue] = []
    for row in rows:
        raw_form = row["pron_raw"] or row["headword_raw"]
        result = apply_profile(raw_form, profile, symbols_inventory=symbols_inventory)
        applied[row["entry_id"]] = result
        entry = ParsedEntry(
            dict_id=row["dict_id"],
            section=row["section"] or "",
            syllables=int(row["syllables"]),
            headword_raw=row["headword_raw"],
            pos_raw=row["pos_raw"],
            pron_raw=row["pron_raw"],
            source_path=row["source_path"],
            line_no=int(row["line_no"]),
            raw_line=f"{row['syllables']} {row['headword_raw']}, {row['pos_raw']}",
            origin_raw=row["origin_raw"],
            origin_norm=row["origin_norm"],
            pos_norm=row["pos_norm"],
            parser_id=row["parser_id"],
            parser_version=row["parser_version"],
            parser_sha256=row["parser_sha256"],
        )
        profile_issues.extend(
            validate_profile_applied(
                entry=entry,
                entry_id=row["entry_id"],
                profile=profile,
                applied=result,
            )
        )

    store.update_profile_fields(dict_id=dict_id, applied_by_entry_id=applied)
    store.clear_issues_by_codes(dict_id=dict_id, codes=PROFILE_AWARE_CODES)
    store.insert_issues(profile_issues)
    store.record_profile_application(
        dict_id=dict_id,
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_sha256=profile_hash,
        entries_count=len(applied),
        status="success",
        details={
            "action": "apply_profile",
            "issues": len(profile_issues),
        },
    )

    summary = {
        "dict_id": dict_id,
        "profile": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
        "updated_entries": len(applied),
        "profile_issues": len(profile_issues),
        "profile_warnings": profile.validation_warnings,
    }
    append_jsonl_log(
        paths.logs_dir / "pipeline.jsonl",
        {
            "event": "apply_profile",
            "summary": summary,
        },
    )
    return summary


def preview_profile_entries(
    project_dir: Path,
    dict_id: str,
    profile_name: str,
    limit: int = 50,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    profile_path = _resolve_profile_path(paths.rules_dir, profile_name)
    try:
        profile = load_profile(profile_path)
    except ProfileValidationError as exc:
        raise PipelineError(f"Invalid profile {profile_path}: {exc}") from exc

    profile_hash = profile_sha256_from_path(profile_path)
    symbols_inventory = load_symbols_inventory(paths.rules_dir, dict_id=dict_id)

    rows = store.list_entries(dict_id=dict_id, limit=limit, offset=0)
    preview_rows: list[dict[str, Any]] = []
    for row in rows:
        raw_form = row["pron_raw"] or row["headword_raw"]
        entry = ParsedEntry(
            dict_id=row["dict_id"],
            section=row["section"] or "",
            syllables=int(row["syllables"]),
            headword_raw=row["headword_raw"],
            pos_raw=row["pos_raw"],
            pron_raw=row["pron_raw"],
            source_path=row["source_path"],
            line_no=int(row["line_no"]),
            raw_line=f"{row['syllables']} {row['headword_raw']}, {row['pos_raw']}",
            origin_raw=row["origin_raw"],
            origin_norm=row["origin_norm"],
            pos_norm=row["pos_norm"],
            parser_id=row["parser_id"],
            parser_version=row["parser_version"],
            parser_sha256=row["parser_sha256"],
        )
        applied = apply_profile(raw_form, profile, symbols_inventory=symbols_inventory)
        issues = validate_profile_applied(
            entry=entry,
            entry_id=row["entry_id"],
            profile=profile,
            applied=applied,
        )
        preview_rows.append(
            {
                "entry_id": row["entry_id"],
                "raw": raw_form,
                "display": applied.form_display,
                "norm": applied.form_norm,
                "features": applied.features,
                "unknown_symbols": applied.unknown_symbols,
                "issue_codes": [issue.code for issue in issues],
            }
        )

    return {
        "dict_id": dict_id,
        "profile_id": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
        "profile_warnings": profile.validation_warnings,
        "count": len(preview_rows),
        "rows": preview_rows,
    }


def register_import_event(project_dir: Path, event: dict[str, Any]) -> None:
    paths = project_paths(project_dir)
    event = dict(event)
    event["event_id"] = hashlib.sha256(str(sorted(event.items())).encode("utf-8")).hexdigest()
    append_jsonl_log(paths.logs_dir / "imports.jsonl", event)
