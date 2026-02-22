from __future__ import annotations

from pathlib import Path
from typing import Any

from ..models import Issue, ParsedEntry
from ..overrides import (
    apply_record_overrides,
    apply_record_overrides_to_apply,
    list_overrides,
)
from ..pipeline import PipelineError, apply_profile_to_entries
from ..storage.sqlite import SQLiteStore, init_project
from .engine import apply_template_to_records, load_source_records, preview_template
from .spec import TemplateKind, TemplateSpec, template_sha256

SUPPORTED_SOURCE_SUFFIXES = {".txt", ".csv"}


def list_template_sources(project_dir: Path) -> list[Path]:
    paths = init_project(project_dir)
    imports_dir = paths.raw_dir / "imports"
    if not imports_dir.exists():
        return []
    files = [
        path
        for path in sorted(imports_dir.rglob("*"))
        if path.is_file() and path.suffix.lower() in SUPPORTED_SOURCE_SUFFIXES
    ]
    return files


def _default_pos(kind: TemplateKind) -> str:
    if kind == TemplateKind.WORDLIST_TOKENS:
        return "p"
    return "v"


def preview_template_on_source(
    project_dir: Path,
    source_path: Path,
    kind: TemplateKind | str,
    params: dict[str, Any],
    corpus_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    records = load_source_records(source_path=source_path, limit=limit)
    result = preview_template(kind=kind, params=params, records=records)

    rows = [
        {
            "source": row.source,
            "headword_raw": row.headword_raw,
            "pron_raw": row.pron_raw,
            "definition_raw": row.definition_raw,
            "source_id": row.source_id,
            "record_key": row.record_key,
            "status": row.status,
            "reason": row.reason,
            "source_path": row.source_path,
            "record_no": row.record_no,
            "issue_code": row.issue_code,
            "override_op": row.override_op,
        }
        for row in result.rows
    ]

    overridden_count = 0
    if corpus_id:
        overrides = list_overrides(
            store=store,
            corpus_id=corpus_id,
            scope="record",
            source_id=str(source_path.expanduser().resolve()),
        )
        rows = apply_record_overrides(rows, overrides)
        overridden_count = sum(1 for row in rows if row.get("override_op"))

    ignored_count = sum(1 for row in rows if row.get("status") == "Ignoré")
    unrecognized_count = sum(1 for row in rows if row.get("status") == "Non reconnu")
    entries_count = sum(1 for row in rows if row.get("status") == "OK")
    issues_by_code: dict[str, int] = {}
    for row in rows:
        issue_code = row.get("issue_code")
        if issue_code:
            issue_code_text = str(issue_code)
            issues_by_code[issue_code_text] = issues_by_code.get(issue_code_text, 0) + 1

    return {
        "records_count": result.records_count,
        "entries_count": entries_count,
        "ignored_count": ignored_count,
        "unrecognized_count": unrecognized_count,
        "overridden_count": overridden_count,
        "issues_by_code": issues_by_code,
        "rows": rows,
    }


def apply_template_to_corpus(
    project_dir: Path,
    corpus_id: str,
    source_path: Path,
    template_spec: TemplateSpec,
    apply_profile: str | None = None,
) -> dict[str, Any]:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary(dict_id=corpus_id, label=corpus_id)
    entries_before = store.count_entries(corpus_id)

    records = load_source_records(source_path=source_path, limit=None)
    applied = apply_template_to_records(template_spec.kind, template_spec.params, records)
    overrides = list_overrides(
        store=store,
        corpus_id=corpus_id,
        scope="record",
        source_id=str(source_path.expanduser().resolve()),
    )
    overridden_entries, overridden_preview_rows = apply_record_overrides_to_apply(
        entries=applied.entries,
        preview_rows=applied.preview_rows,
        overrides=overrides,
    )
    sha = template_sha256(template_spec)

    parsed_entries: list[ParsedEntry] = []
    for draft in overridden_entries:
        parsed_entries.append(
            ParsedEntry(
                dict_id=corpus_id,
                section="",
                syllables=1,
                headword_raw=draft.headword_raw,
                pos_raw=_default_pos(template_spec.kind),
                pron_raw=draft.pron_raw,
                source_path=draft.source_path,
                line_no=draft.record_no,
                raw_line=draft.source_record,
                definition_raw=draft.definition_raw,
                source_record=draft.source_record,
                template_id=template_spec.template_id,
                template_version=template_spec.version,
                template_sha256=sha,
                source_id=draft.source_id,
                record_key=draft.record_key,
            )
        )

    issues: list[Issue] = []
    for row in overridden_preview_rows:
        if not row.issue_code:
            continue
        issues.append(
            Issue(
                dict_id=corpus_id,
                source_path=row.source_path,
                line_no=row.record_no,
                kind="warning",
                code=row.issue_code,
                raw=row.source,
                details={
                    "template_id": template_spec.template_id,
                    "template_kind": template_spec.kind.value,
                    "template_version": template_spec.version,
                    "template_sha256": sha,
                    "reason": row.reason,
                },
            )
        )

    store.insert_entries(parsed_entries)
    store.insert_issues(issues)
    store.save_active_template(
        corpus_id=corpus_id,
        template_id=template_spec.template_id,
        template_kind=template_spec.kind.value,
        version=template_spec.version,
        params=template_spec.params,
        sha256=sha,
    )
    store.record_template_application(
        corpus_id=corpus_id,
        template_id=template_spec.template_id,
        version=template_spec.version,
        sha256=sha,
        params=template_spec.params,
        source_ids=[str(source_path)],
        records_count=applied.records_count,
        entries_count=len(parsed_entries),
        status="ok",
    )
    entries_after = store.count_entries(corpus_id)

    profile_summary: dict[str, Any] | None = None
    if apply_profile:
        try:
            profile_summary = apply_profile_to_entries(
                project_dir=project_dir,
                dict_id=corpus_id,
                profile_name=apply_profile,
            )
        except PipelineError as exc:
            store.record_template_application(
                corpus_id=corpus_id,
                template_id=template_spec.template_id,
                version=template_spec.version,
                sha256=sha,
                params=template_spec.params,
                source_ids=[str(source_path)],
                records_count=applied.records_count,
                entries_count=applied.entries_count,
                status=f"profile_error: {exc}",
            )
            raise

    return {
        "corpus_id": corpus_id,
        "source_path": str(source_path),
        "template_id": template_spec.template_id,
        "template_kind": template_spec.kind.value,
        "template_version": template_spec.version,
        "template_sha256": sha,
        "records_count": applied.records_count,
        "entries_count": len(parsed_entries),
        "ignored_count": sum(1 for row in overridden_preview_rows if row.status == "Ignoré"),
        "unrecognized_count": sum(
            1 for row in overridden_preview_rows if row.status == "Non reconnu"
        ),
        "overridden_count": sum(
            1 for row in overridden_preview_rows if getattr(row, "override_op", None)
        ),
        "issues_by_code": dict(
            sorted(
                {
                    row.issue_code: sum(
                        1 for item in overridden_preview_rows if item.issue_code == row.issue_code
                    )
                    for row in overridden_preview_rows
                    if row.issue_code
                }.items()
            )
        ),
        "issues_count": len(issues),
        "entries_before": entries_before,
        "entries_after": entries_after,
        "profile_summary": profile_summary,
    }


def resolve_source_for_kind(project_dir: Path, kind: TemplateKind | str) -> Path:
    template_kind = TemplateKind(kind)
    sources = list_template_sources(project_dir)
    if not sources:
        raise FileNotFoundError("Aucune source importée trouvée dans data/raw/imports.")

    if template_kind == TemplateKind.CSV_MAPPING:
        for source in sources:
            if source.suffix.lower() == ".csv":
                return source
        raise FileNotFoundError("Aucune source CSV trouvée pour le gabarit CSV_MAPPING.")

    for source in sources:
        if source.suffix.lower() == ".txt":
            return source
    raise FileNotFoundError("Aucune source texte trouvée pour ce gabarit.")
