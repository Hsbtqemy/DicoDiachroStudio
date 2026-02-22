from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..models import Issue, ParsedEntry, ProfileSpec
from ..pipeline import PipelineError, resolve_profile_path
from ..profiles import (
    ProfileValidationError,
    apply_profile,
    load_profile,
    load_symbols_inventory,
    profile_sha256_from_path,
)
from ..qa import PROFILE_AWARE_CODES, validate_profile_applied
from ..storage.sqlite import SQLiteStore, init_project

CONVENTION_EXTRA_CODES = {"MISSING_PRON", "BAD_SYLL_COUNT"}
CONVENTION_CODES = PROFILE_AWARE_CODES | CONVENTION_EXTRA_CODES

ALERT_LABELS = {
    "UNKNOWN_SYMBOL": "Symboles inconnus",
    "DETACHED_COMBINING_MARK": "Diacritique détaché",
    "MULTIPLE_PRIMARY_STRESS": "Stress primaire multiple",
    "INCONSISTENT_STRESS": "Stress incohérent",
    "PROFILE_RULE_RUNTIME_ERROR": "Règle de profil invalide",
    "PROFILE_INVALID": "Profil invalide",
    "MISSING_PRON": "Prononciation manquante",
    "BAD_SYLL_COUNT": "Compte syllabique invalide",
}


def _effective(row: dict[str, Any], stem: str) -> str:
    edit = row.get(f"{stem}_edit")
    if edit is not None and str(edit).strip():
        return str(edit).strip()
    return str(row.get(f"{stem}_raw") or "").strip()


def _raw_line_from_row(row: dict[str, Any], headword_effective: str) -> str:
    source_record = str(row.get("source_record") or "").strip()
    if source_record:
        return source_record
    syllables = int(row.get("syllables") or 0)
    pos_raw = str(row.get("pos_raw") or "").strip() or "v"
    return f"{syllables} {headword_effective}, {pos_raw}".strip()


def _build_entry_for_qa(
    row: dict[str, Any],
    headword_effective: str,
    pron_effective: str,
) -> ParsedEntry:
    return ParsedEntry(
        dict_id=str(row["dict_id"]),
        section=str(row.get("section") or ""),
        syllables=int(row.get("syllables") or 0),
        headword_raw=headword_effective,
        pos_raw=str(row.get("pos_raw") or "v"),
        pron_raw=pron_effective or None,
        source_path=str(row.get("source_path") or ""),
        line_no=int(row.get("line_no") or 0),
        raw_line=_raw_line_from_row(row, headword_effective),
        origin_raw=(str(row.get("origin_raw")) if row.get("origin_raw") else None),
        origin_norm=(str(row.get("origin_norm")) if row.get("origin_norm") else None),
        pos_norm=(str(row.get("pos_norm")) if row.get("pos_norm") else None),
        parser_id=(str(row.get("parser_id")) if row.get("parser_id") else None),
        parser_version=(int(row.get("parser_version")) if row.get("parser_version") else None),
        parser_sha256=(str(row.get("parser_sha256")) if row.get("parser_sha256") else None),
        definition_raw=(str(row.get("definition_raw")) if row.get("definition_raw") else None),
        source_record=(str(row.get("source_record")) if row.get("source_record") else None),
        template_id=(str(row.get("template_id")) if row.get("template_id") else None),
        template_version=(
            int(row.get("template_version")) if row.get("template_version") else None
        ),
        template_sha256=(str(row.get("template_sha256")) if row.get("template_sha256") else None),
        source_id=(str(row.get("source_id")) if row.get("source_id") else None),
        record_key=(str(row.get("record_key")) if row.get("record_key") else None),
    )


def _extra_qa_issues(
    *,
    profile: ProfileSpec,
    row: dict[str, Any],
    entry: ParsedEntry,
    entry_id: str,
    pron_effective: str,
) -> list[Issue]:
    issues: list[Issue] = []
    qa_cfg = profile.qa if isinstance(profile.qa, dict) else {}

    if bool(qa_cfg.get("require_pronunciation", False)) and not pron_effective:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="MISSING_PRON",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                },
            )
        )

    syllables = int(row.get("syllables") or 0)
    if syllables < 1 or syllables > 10:
        issues.append(
            Issue(
                dict_id=entry.dict_id,
                source_path=entry.source_path,
                line_no=entry.line_no,
                kind="warning",
                code="BAD_SYLL_COUNT",
                raw=entry.raw_line,
                details={
                    "entry_id": entry_id,
                    "profile_id": profile.profile_id,
                    "syllables": syllables,
                },
            )
        )

    return issues


def _compute_entry_convention(
    *,
    row: dict[str, Any],
    profile: ProfileSpec,
    profile_hash: str,
    symbols_inventory: set[str],
) -> tuple[dict[str, Any], list[Issue]]:
    entry_id = str(row["entry_id"])
    headword_raw = str(row.get("headword_raw") or "")
    pron_raw = str(row.get("pron_raw") or "")

    headword_effective = _effective(row, "headword")
    pron_effective = _effective(row, "pron")
    qa_text = pron_effective or headword_effective

    head_applied = apply_profile(headword_effective, profile, symbols_inventory=symbols_inventory)
    pron_applied = apply_profile(qa_text, profile, symbols_inventory=symbols_inventory)

    features = dict(pron_applied.features)
    features["headword_symbols_used"] = head_applied.symbols_used
    features["headword_unknown_symbols"] = head_applied.unknown_symbols

    entry = _build_entry_for_qa(
        row=row,
        headword_effective=headword_effective,
        pron_effective=pron_effective,
    )

    issues = validate_profile_applied(
        entry=entry,
        entry_id=entry_id,
        profile=profile,
        applied=pron_applied,
    )
    issues.extend(
        _extra_qa_issues(
            profile=profile,
            row=row,
            entry=entry,
            entry_id=entry_id,
            pron_effective=pron_effective,
        )
    )

    issue_codes = sorted({issue.code for issue in issues})
    alert_labels = [ALERT_LABELS.get(code, code) for code in issue_codes]

    modified = any(
        [
            head_applied.form_norm != headword_effective,
            pron_applied.form_norm != qa_text,
            pron_applied.form_render != qa_text,
        ]
    )

    payload = {
        "entry_id": entry_id,
        "headword_raw": headword_raw,
        "headword_effective": headword_effective,
        "headword_norm": head_applied.form_norm,
        "pron_raw": pron_raw,
        "pron_effective": pron_effective,
        "pron_norm": pron_applied.form_norm,
        "pron_render": pron_applied.form_render,
        "form_display": pron_applied.form_display,
        "form_norm": pron_applied.form_norm,
        "features": features,
        "features_json": json.dumps(features, ensure_ascii=False, sort_keys=True),
        "issue_codes": issue_codes,
        "alerts": alert_labels,
        "unknown_symbols": pron_applied.unknown_symbols,
        "unknown_symbols_count": len(pron_applied.unknown_symbols),
        "modified": modified,
        "overridden": bool(
            str(row.get("headword_edit") or "").strip()
            or str(row.get("pron_edit") or "").strip()
            or str(row.get("definition_edit") or "").strip()
        ),
        "profile_id": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
    }
    return payload, issues


def _load_profile_from_ref(project_dir: Path, profile_ref: str) -> tuple[ProfileSpec, Path, str]:
    paths = init_project(project_dir)
    profile_path = resolve_profile_path(paths.rules_dir, profile_ref)
    try:
        profile = load_profile(profile_path)
    except ProfileValidationError as exc:
        raise PipelineError(f"Invalid profile {profile_path}: {exc}") from exc
    return profile, profile_path, profile_sha256_from_path(profile_path)


def preview_convention(
    project_dir: Path,
    corpus_id: str,
    profile_ref: str,
    limit: int = 200,
    entry_ids: list[str] | None = None,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    profile, profile_path, profile_hash = _load_profile_from_ref(project_dir, profile_ref)
    symbols_inventory = load_symbols_inventory(paths.rules_dir, dict_id=corpus_id)

    rows: list[dict[str, Any]] = []
    if entry_ids:
        seen: set[str] = set()
        for entry_id in entry_ids:
            cleaned_entry_id = str(entry_id or "").strip()
            if not cleaned_entry_id or cleaned_entry_id in seen:
                continue
            seen.add(cleaned_entry_id)
            row = store.entry_by_id(cleaned_entry_id)
            if row is None or str(row["dict_id"]) != corpus_id:
                continue
            rows.append(dict(row))
    else:
        rows = [dict(row) for row in store.list_entries(corpus_id, limit=limit, offset=0)]

    preview_rows: list[dict[str, Any]] = []
    alerts_count = 0
    unknown_symbols_total = 0
    modified_count = 0

    for row in rows:
        payload, issues = _compute_entry_convention(
            row=row,
            profile=profile,
            profile_hash=profile_hash,
            symbols_inventory=symbols_inventory,
        )
        preview_rows.append(payload)
        alerts_count += len(issues)
        unknown_symbols_total += payload["unknown_symbols_count"]
        modified_count += 1 if payload["modified"] else 0

    return {
        "corpus_id": corpus_id,
        "profile_id": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
        "profile_path": str(profile_path),
        "profile_warnings": profile.validation_warnings,
        "entries_analyzed": len(preview_rows),
        "modified_count": modified_count,
        "alerts_count": alerts_count,
        "unknown_symbols_count": unknown_symbols_total,
        "sample_entry_ids": [str(row["entry_id"]) for row in rows],
        "rows": preview_rows,
    }


def apply_convention(
    project_dir: Path,
    corpus_id: str,
    profile_ref: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    profile, profile_path, profile_hash = _load_profile_from_ref(project_dir, profile_ref)
    symbols_inventory = load_symbols_inventory(paths.rules_dir, dict_id=corpus_id)
    rows = [dict(row) for row in store.entries_for_dict(corpus_id)]

    if not rows:
        raise PipelineError(f"No entries found for dictionary {corpus_id}")

    updates: list[dict[str, Any]] = []
    issues: list[Issue] = []
    unknown_symbols_total = 0

    for row in rows:
        payload, entry_issues = _compute_entry_convention(
            row=row,
            profile=profile,
            profile_hash=profile_hash,
            symbols_inventory=symbols_inventory,
        )
        updates.append(payload)
        issues.extend(entry_issues)
        unknown_symbols_total += payload["unknown_symbols_count"]

    store.update_convention_fields(corpus_id, updates)
    store.clear_issues_by_codes(corpus_id, CONVENTION_CODES)
    store.insert_issues(issues)
    store.record_convention_application(
        corpus_id=corpus_id,
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_sha256=profile_hash,
        entries_count=len(updates),
        issues_count=len(issues),
        status="success",
        details={
            "profile_path": str(profile_path),
            "alerts_by_code": {
                code: sum(1 for issue in issues if issue.code == code)
                for code in sorted({issue.code for issue in issues})
            },
        },
    )

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return {
        "corpus_id": corpus_id,
        "profile_id": profile.profile_id,
        "profile_version": profile.version,
        "profile_sha256": profile_hash,
        "profile_path": str(profile_path),
        "entries_updated": len(updates),
        "alerts_count": len(issues),
        "unknown_symbols_count": unknown_symbols_total,
        "elapsed_ms": elapsed_ms,
        "profile_warnings": profile.validation_warnings,
    }
