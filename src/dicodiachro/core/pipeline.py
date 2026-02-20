from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .models import Issue, ParsedEntry
from .parsing import parse_lines
from .profiles import apply_profile, load_profile, profile_sha256
from .qa import export_issues_csv, lint_lines, load_s_vs_f, validate_entries, warn_s_vs_f
from .storage.sqlite import SQLiteStore, append_jsonl_log, entry_id_for, init_project, project_paths


class PipelineError(RuntimeError):
    pass


def _resolve_profile_path(project_rules_dir: Path, profile_name: str) -> Path:
    repo_templates = Path(__file__).resolve().parents[3] / "rules" / "templates"
    candidates = [
        project_rules_dir / f"{profile_name}.yml",
        project_rules_dir / f"{profile_name}.yaml",
        Path("rules") / "templates" / f"{profile_name}.yml",
        Path("rules") / "templates" / f"{profile_name}.yaml",
        repo_templates / f"{profile_name}.yml",
        repo_templates / f"{profile_name}.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise PipelineError(f"Profile not found: {profile_name}")


def discover_source_texts(raw_dir: Path) -> list[Path]:
    return sorted(p for p in raw_dir.rglob("*.txt") if p.is_file())


def _parse_source(path: Path, dict_id: str) -> tuple[list[ParsedEntry], list[Issue], list[str]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    entries, parse_issues = parse_lines(lines, dict_id=dict_id, source_path=str(path))
    qa_issues = lint_lines(lines, dict_id=dict_id, source_path=str(path))
    qa_issues.extend(validate_entries(entries))
    return entries, parse_issues + qa_issues, lines


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


def run_pipeline(
    project_dir: Path,
    dict_id: str,
    profile_name: str,
    source_paths: list[Path] | None = None,
    clear_existing: bool = True,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary(dict_id=dict_id, label=dict_id)

    profile_path = _resolve_profile_path(paths.rules_dir, profile_name)
    profile = load_profile(profile_path)
    profile_hash = profile_sha256(profile_path)
    store.upsert_profile(profile, profile_path, profile_hash)

    if source_paths is None:
        source_paths = discover_source_texts(paths.raw_dir)
    if not source_paths:
        raise PipelineError("No .txt sources found in project data/raw.")

    all_entries: list[ParsedEntry] = []
    all_issues: list[Issue] = []

    s_vs_f_path = _resolve_s_vs_f(project_dir)
    s_vs_f_lexicon = load_s_vs_f(s_vs_f_path) if s_vs_f_path else set()
    for source in source_paths:
        entries, issues, _ = _parse_source(source, dict_id=dict_id)
        all_entries.extend(entries)
        all_issues.extend(issues)

    all_issues.extend(warn_s_vs_f(all_entries, dict_id=dict_id, lexicon=s_vs_f_lexicon))

    applied = {}
    for entry in all_entries:
        result = apply_profile(entry.pron_raw or entry.headword_raw, profile)
        applied[entry_id_for(entry)] = result

    if clear_existing:
        store.clear_dict_entries(dict_id)

    store.insert_entries(all_entries, applied)
    store.insert_issues(all_issues)

    issues_csv = paths.derived_dir / f"{dict_id}_issues.csv"
    export_issues_csv(all_issues, issues_csv)

    summary = {
        "dict_id": dict_id,
        "profile": profile.profile_id,
        "profile_sha256": profile_hash,
        "entries": len(all_entries),
        "issues": len(all_issues),
        "sources": [str(path) for path in source_paths],
        "issues_csv": str(issues_csv),
    }

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
    profile = load_profile(profile_path)
    profile_hash = profile_sha256(profile_path)
    store.upsert_profile(profile, profile_path, profile_hash)

    rows = store.entries_for_dict(dict_id)
    if not rows:
        raise PipelineError(f"No entries found for dictionary {dict_id}")

    applied = {}
    for row in rows:
        raw_form = row["pron_raw"] or row["headword_raw"]
        applied[row["entry_id"]] = apply_profile(raw_form, profile)

    store.update_profile_fields(dict_id=dict_id, applied_by_entry_id=applied)

    summary = {
        "dict_id": dict_id,
        "profile": profile.profile_id,
        "profile_sha256": profile_hash,
        "updated_entries": len(applied),
    }
    append_jsonl_log(
        paths.logs_dir / "pipeline.jsonl",
        {
            "event": "apply_profile",
            "summary": summary,
        },
    )
    return summary


def register_import_event(project_dir: Path, event: dict[str, Any]) -> None:
    paths = project_paths(project_dir)
    event = dict(event)
    event["event_id"] = hashlib.sha256(str(sorted(event.items())).encode("utf-8")).hexdigest()
    append_jsonl_log(paths.logs_dir / "imports.jsonl", event)
