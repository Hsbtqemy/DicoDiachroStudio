from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Annotated, Any

import typer

from dicodiachro.core.align.cluster import cluster_matches
from dicodiachro.core.align.match import match_dictionaries
from dicodiachro.core.compare.workflow import (
    CompareWorkflowError,
    apply_compare_run,
    list_compare_runs,
    load_compare_run_data,
    preview_alignment,
    preview_coverage,
    preview_diff,
)
from dicodiachro.core.conventions.workflow import apply_convention, preview_convention
from dicodiachro.core.exporters.compare_exports import (
    export_compare_alignment,
    export_compare_coverage,
    export_compare_diff,
)
from dicodiachro.core.exporters.csv_jsonl import (
    export_entries_csv,
    export_entries_jsonl,
    import_entries_csv,
    import_entries_jsonl,
)
from dicodiachro.core.exporters.excel_xlsx import export_comparison_xlsx
from dicodiachro.core.exporters.word_docx import export_comparison_docx
from dicodiachro.core.filters import filter_by_accents, filter_by_prefix, filter_by_syllables
from dicodiachro.core.importers.csv_import import import_csv_batch
from dicodiachro.core.importers.pdf_text_import import (
    PDFTextImportError,
    import_pdf_text,
)
from dicodiachro.core.importers.text_import import (
    import_text_batch,
    list_text_files,
    merge_text_files,
)
from dicodiachro.core.importers.url_import import import_from_share_link
from dicodiachro.core.models import ParsedEntry, ProfileApplied
from dicodiachro.core.parsers.presets import (
    ParserPresetValidationError,
    discover_presets,
    load_parser_preset,
    preset_sha256_from_path,
)
from dicodiachro.core.pipeline import (
    PipelineError,
    apply_profile_to_entries,
    preview_profile_entries,
    register_import_event,
    run_pipeline,
)
from dicodiachro.core.profiles import ProfileValidationError, load_profile, profile_sha256_from_path
from dicodiachro.core.storage.sqlite import SQLiteStore, entry_id_for, init_project, project_paths
from dicodiachro.core.templates.spec import TemplateKind, TemplateSpec
from dicodiachro.core.templates.workflow import (
    apply_template_to_corpus,
    list_template_sources,
    preview_template_on_source,
    resolve_source_for_kind,
)

app = typer.Typer(help="DicoDiachro CLI", rich_markup_mode=None)
import_app = typer.Typer(help="Import data into a local project", rich_markup_mode=None)
export_app = typer.Typer(help="Export dictionaries and comparisons", rich_markup_mode=None)
filter_app = typer.Typer(help="Filter entries from DB or CSV", rich_markup_mode=None)
parser_app = typer.Typer(help="Parsing preset discovery and validation", rich_markup_mode=None)
profile_app = typer.Typer(
    help="Profile validation, preview, and application", rich_markup_mode=None
)
template_app = typer.Typer(help="Template workshop (preview/apply)", rich_markup_mode=None)
convention_app = typer.Typer(help="Conventions workshop (preview/apply)", rich_markup_mode=None)
compare_app = typer.Typer(
    help="Comparison workshop (coverage/alignment/diff)", rich_markup_mode=None
)

app.add_typer(import_app, name="import")
app.add_typer(export_app, name="export")
app.add_typer(filter_app, name="filter")
app.add_typer(parser_app, name="parser")
app.add_typer(profile_app, name="profile")
app.add_typer(template_app, name="template")
app.add_typer(convention_app, name="convention")
app.add_typer(compare_app, name="compare")


def _ensure_templates(paths: Path) -> None:
    template_dir = Path("rules") / "templates"
    if not template_dir.exists():
        template_dir = Path(__file__).resolve().parents[3] / "rules" / "templates"
    if not template_dir.exists():
        return
    for template in template_dir.rglob("*.yml"):
        dst = paths / template.relative_to(template_dir)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copy2(template, dst)


def _load_rows_from_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def _load_rows_from_db(project_dir: Path, dict_id: str) -> list[dict[str, str]]:
    store = SQLiteStore(project_paths(project_dir).db_path)
    rows = store.entries_for_dict(dict_id)
    return [dict(row) for row in rows]


def _print_json(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


def _parse_json_params(raw: str | None) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON for --params: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("--params must be a JSON object")
    return parsed


def _resolve_template_source(project_dir: Path, source: Path | None, kind: TemplateKind) -> Path:
    if source:
        path = source.expanduser()
        if not path.is_absolute():
            path = (project_paths(project_dir).raw_dir / "imports" / path).resolve()
        else:
            path = path.resolve()
        if not path.exists() or not path.is_file():
            raise typer.BadParameter(f"Source not found: {path}")
        return path
    return resolve_source_for_kind(project_dir, kind)


@app.command()
def init(project_dir: Path) -> None:
    """Create a DicoDiachro local-first project folder."""
    paths = init_project(project_dir)
    _ensure_templates(paths.rules_dir)
    typer.echo(f"Project initialized: {paths.root}")
    typer.echo(f"Database: {paths.db_path}")


@import_app.command("text")
def import_text(
    project_dir: Path,
    input_path: Path,
    patterns: Annotated[
        list[str] | None, typer.Option("--pattern", help="Glob(s) for batch import.")
    ] = None,
) -> None:
    paths = init_project(project_dir)
    imported = import_text_batch(paths.raw_dir / "imports", input_path, patterns=patterns)
    register_import_event(
        project_dir,
        {
            "type": "text",
            "input_path": str(input_path),
            "imported": [str(p) for p in imported],
        },
    )
    typer.echo(f"Imported {len(imported)} text file(s)")


@import_app.command("csv")
def import_csv(
    project_dir: Path,
    input_path: Path,
    patterns: Annotated[
        list[str] | None, typer.Option("--pattern", help="Glob(s) for CSV batch import.")
    ] = None,
) -> None:
    paths = init_project(project_dir)
    imported = import_csv_batch(paths.raw_dir / "imports", input_path, patterns=patterns)
    register_import_event(
        project_dir,
        {
            "type": "csv",
            "input_path": str(input_path),
            "imported": [str(path) for path in imported],
        },
    )
    typer.echo(f"Imported {len(imported)} CSV file(s)")


@import_app.command("merge")
def import_merge(
    project_dir: Path,
    input_path: Path,
    output_name: str = "merged_results.txt",
    deduplicate: bool = True,
) -> None:
    """Merge imported text files (legacy merge_resultats behavior, configurable)."""
    paths = init_project(project_dir)
    files = list_text_files(input_path)
    out_path = merge_text_files(files, paths.interim_dir / output_name, deduplicate=deduplicate)
    register_import_event(
        project_dir,
        {
            "type": "merge",
            "source_dir": str(input_path),
            "output": str(out_path),
            "count": len(files),
            "deduplicate": deduplicate,
        },
    )
    typer.echo(f"Merged {len(files)} file(s) -> {out_path}")


@import_app.command("pdf")
@import_app.command("pdf-text")
def import_pdf_text_cmd(
    project_dir: Path,
    pdf_path: Path,
    out: Path | None = typer.Option(
        None,
        "--out",
        help="Optional output file or directory for extracted text.",
    ),
    two_columns: bool = typer.Option(
        False, "--two-columns/--no-two-columns", help="Read PDF as double-column pages."
    ),
    dict_id: str | None = typer.Option(
        None,
        "--dict-id",
        help="Optional convenience mode: run pipeline after import.",
    ),
    profile: str = typer.Option(
        "reading_v1",
        "--profile",
        help="Profile used only when --dict-id is provided.",
    ),
    parser: str | None = typer.Option(
        None,
        "--parser",
        help="Parser preset path/id used only when --dict-id is provided.",
    ),
) -> None:
    paths = init_project(project_dir)
    try:
        imported = import_pdf_text(
            project_dir=project_dir,
            pdf_path=pdf_path,
            out=out,
            two_columns=two_columns,
        )
    except PDFTextImportError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    register_import_event(
        project_dir,
        {
            "type": "pdf_text",
            "pdf_path": str(pdf_path),
            "two_columns": two_columns,
            **imported.as_dict(),
        },
    )

    payload: dict[str, Any] = {
        "import": imported.as_dict(),
    }

    if dict_id:
        try:
            payload["pipeline"] = run_pipeline(
                project_dir=project_dir,
                dict_id=dict_id,
                profile_name=profile,
                source_paths=imported.output_text_paths,
                parser_name=parser,
            )
        except PipelineError as exc:
            typer.echo(str(exc))
            raise typer.Exit(code=1) from exc
    else:
        payload["next"] = {
            "hint": "Run `dicodiachro run` with --dict-id and --profile when ready.",
            "sources": [str(path) for path in imported.output_text_paths],
        }

    if not out:
        payload["project_raw_imports"] = str(paths.raw_dir / "imports")
    _print_json(payload)


@import_app.command("url")
def import_url(project_dir: Path, url: str) -> None:
    """Import from a share URL (download to local raw/imports, optional unzip)."""
    paths = init_project(project_dir)
    imported, metadata = import_from_share_link(
        url=url,
        imports_dir=paths.raw_dir / "imports",
        extract_dir=paths.raw_dir / "imports" / "unzipped",
    )
    register_import_event(
        project_dir,
        {
            "type": "url",
            **metadata,
            "imported_files": [str(p) for p in imported],
        },
    )
    typer.echo(f"Downloaded {len(imported)} file(s) from URL")


@import_app.command("nomenclature")
def import_nomenclature(
    project_dir: Path, input_file: Path, dict_id: str = typer.Option(..., "--dict-id")
) -> None:
    """Import an entries CSV/JSONL nomenclature into SQLite."""
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary(dict_id=dict_id, label=dict_id)

    if input_file.suffix.lower() == ".jsonl":
        rows = import_entries_jsonl(input_file)
    else:
        rows = import_entries_csv(input_file)

    entries: list[ParsedEntry] = []
    applied: dict[str, ProfileApplied] = {}
    for idx, row in enumerate(rows, start=1):
        source_path = str(row.get("source_path") or input_file)
        line_no = int(row.get("line_no") or idx)
        headword_raw = str(row.get("headword_raw") or "")
        pos_raw = str(row.get("pos_raw") or "v")
        syllables = int(row.get("syllables") or 1)
        pron_raw = row.get("pron_raw") or headword_raw

        entry = ParsedEntry(
            dict_id=dict_id,
            section=str(row.get("section") or ""),
            syllables=syllables,
            headword_raw=headword_raw,
            pos_raw=pos_raw,
            pron_raw=pron_raw,
            source_path=source_path,
            line_no=line_no,
            raw_line=f"{syllables} {headword_raw}, {pos_raw}",
            origin_raw=(str(row.get("origin_raw")) if row.get("origin_raw") else None),
            origin_norm=(str(row.get("origin_norm")) if row.get("origin_norm") else None),
            pos_norm=(str(row.get("pos_norm")) if row.get("pos_norm") else None),
            parser_id=(str(row.get("parser_id")) if row.get("parser_id") else None),
            parser_version=(int(row.get("parser_version")) if row.get("parser_version") else None),
            parser_sha256=(str(row.get("parser_sha256")) if row.get("parser_sha256") else None),
        )
        entries.append(entry)

        features_json = row.get("features_json") or "{}"
        try:
            features = json.loads(features_json)
        except json.JSONDecodeError:
            features = {}
        applied[entry_id_for(entry)] = ProfileApplied(
            form_display=str(row.get("form_display") or pron_raw),
            form_norm=str(row.get("form_norm") or pron_raw),
            features=features if isinstance(features, dict) else {},
        )

    store.insert_entries(entries, applied)
    register_import_event(
        project_dir,
        {
            "type": "nomenclature",
            "input_file": str(input_file),
            "dict_id": dict_id,
            "rows": len(rows),
        },
    )
    typer.echo(f"Imported {len(rows)} nomenclature row(s) into dict {dict_id}")


@app.command()
def run(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id", help="Dictionary identifier."),
    profile: str = typer.Option("reading_v1", "--profile", help="Profile ID/file stem."),
    parser: str | None = typer.Option(
        None,
        "--parser",
        help="Parsing preset path or parser_id. Defaults to auto preset for dict_id.",
    ),
    source: Annotated[
        list[Path] | None,
        typer.Option("--source", help="Optional source txt file(s)."),
    ] = None,
) -> None:
    """Run parse + QA + profile application to SQLite entries."""
    try:
        summary = run_pipeline(
            project_dir=project_dir,
            dict_id=dict_id,
            profile_name=profile,
            source_paths=source,
            parser_name=parser,
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


@parser_app.command("list")
def parser_list(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
) -> None:
    paths = init_project(project_dir)
    presets = discover_presets(paths.rules_dir, dict_id=dict_id)

    rows: list[dict[str, Any]] = []
    for preset_path in presets:
        try:
            spec = load_parser_preset(preset_path)
            rows.append(
                {
                    "path": str(preset_path),
                    "parser_id": spec.parser_id,
                    "version": spec.version,
                    "sha256": preset_sha256_from_path(preset_path),
                    "warnings": spec.validation_warnings,
                }
            )
        except ParserPresetValidationError:
            continue
    _print_json({"dict_id": dict_id, "count": len(rows), "presets": rows})


@parser_app.command("validate")
def parser_validate(
    parser_path: Path,
    strict: bool = typer.Option(
        False, "--strict/--no-strict", help="Treat unknown keys as errors."
    ),
) -> None:
    try:
        preset = load_parser_preset(parser_path, strict=strict)
    except ParserPresetValidationError as exc:
        _print_json(
            {
                "ok": False,
                "parser_path": str(parser_path),
                "errors": exc.errors,
                "warnings": exc.warnings,
            }
        )
        raise typer.Exit(code=1) from exc

    _print_json(
        {
            "ok": True,
            "parser_path": str(parser_path),
            "parser_id": preset.parser_id,
            "version": preset.version,
            "sha256": preset_sha256_from_path(parser_path),
            "warnings": preset.validation_warnings,
        }
    )


@app.command("apply-profile")
def apply_profile_cmd(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
    profile: str = typer.Option(..., "--profile"),
) -> None:
    summary = apply_profile_to_entries(project_dir, dict_id=dict_id, profile_name=profile)
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


@profile_app.command("validate")
def profile_validate(
    profile_path: Path,
    strict: bool = typer.Option(
        False, "--strict/--no-strict", help="Treat unknown keys as errors."
    ),
) -> None:
    try:
        profile = load_profile(profile_path, strict=strict)
    except ProfileValidationError as exc:
        _print_json(
            {
                "ok": False,
                "profile_path": str(profile_path),
                "errors": exc.errors,
                "warnings": exc.warnings,
            }
        )
        raise typer.Exit(code=1) from exc

    _print_json(
        {
            "ok": True,
            "profile_id": profile.profile_id,
            "version": profile.version,
            "sha256": profile_sha256_from_path(profile_path),
            "warnings": profile.validation_warnings,
        }
    )


@profile_app.command("preview")
def profile_preview(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
    profile: str = typer.Option(..., "--profile"),
    limit: int = typer.Option(50, "--limit", min=1, max=5000),
    out: Path | None = typer.Option(None, "--out", help="Optional CSV output path."),
) -> None:
    try:
        preview = preview_profile_entries(
            project_dir=project_dir,
            dict_id=dict_id,
            profile_name=profile,
            limit=limit,
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "entry_id",
                    "raw",
                    "display",
                    "norm",
                    "prime_count",
                    "accented_vowel_count",
                    "unknown_symbols",
                    "issue_codes",
                ]
            )
            for row in preview["rows"]:
                features = row["features"]
                writer.writerow(
                    [
                        row["entry_id"],
                        row["raw"],
                        row["display"],
                        row["norm"],
                        features.get("prime_count", 0),
                        features.get("accented_vowel_count", 0),
                        ",".join(row["unknown_symbols"]),
                        ",".join(row["issue_codes"]),
                    ]
                )
        _print_json(
            {
                "profile_id": preview["profile_id"],
                "profile_version": preview["profile_version"],
                "profile_sha256": preview["profile_sha256"],
                "rows": preview["count"],
                "preview_csv": str(out),
            }
        )
        return

    _print_json(preview)


@profile_app.command("apply")
def profile_apply(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
    profile: str = typer.Option(..., "--profile"),
) -> None:
    try:
        summary = apply_profile_to_entries(project_dir, dict_id=dict_id, profile_name=profile)
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    _print_json(summary)


def _discover_profile_files(project_dir: Path, corpus_id: str | None = None) -> list[Path]:
    paths = init_project(project_dir)
    rules_dir = paths.rules_dir
    candidates: set[Path] = set(rules_dir.glob("*.yml")) | set(rules_dir.glob("*.yaml"))
    if corpus_id:
        corpus_rules_dir = rules_dir / corpus_id
        if corpus_rules_dir.exists():
            candidates |= set(corpus_rules_dir.glob("*.yml"))
            candidates |= set(corpus_rules_dir.glob("*.yaml"))
    return sorted(path.resolve() for path in candidates)


@convention_app.command("list")
def convention_list(
    project_dir: Path,
    corpus: str = typer.Option(..., "--corpus"),
) -> None:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    files = _discover_profile_files(project_dir, corpus_id=corpus)
    latest = store.list_convention_applications(corpus, limit=1)
    active = dict(latest[0]) if latest else None

    profiles: list[dict[str, Any]] = []
    for profile_path in files:
        try:
            profile = load_profile(profile_path)
            profiles.append(
                {
                    "profile_id": profile.profile_id,
                    "version": profile.version,
                    "sha256": profile_sha256_from_path(profile_path),
                    "path": str(profile_path),
                    "warnings": profile.validation_warnings,
                }
            )
        except ProfileValidationError as exc:
            profiles.append(
                {
                    "profile_id": profile_path.stem,
                    "path": str(profile_path),
                    "errors": exc.errors,
                    "warnings": exc.warnings,
                }
            )

    _print_json(
        {
            "project_dir": str(project_dir.resolve()),
            "corpus_id": corpus,
            "count": len(profiles),
            "profiles": profiles,
            "active_convention": active,
        }
    )


@convention_app.command("preview")
def convention_preview(
    project_dir: Path,
    corpus: str = typer.Option(..., "--corpus"),
    profile: str = typer.Option(..., "--profile"),
    limit: int = typer.Option(200, "--limit", min=1, max=5000),
    out: Path | None = typer.Option(None, "--out", help="Optional CSV output."),
) -> None:
    try:
        payload = preview_convention(
            project_dir=project_dir,
            corpus_id=corpus,
            profile_ref=profile,
            limit=limit,
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "entry_id",
                    "headword_raw",
                    "headword_effective",
                    "headword_norm",
                    "pron_raw",
                    "pron_effective",
                    "pron_norm",
                    "pron_render",
                    "features_json",
                    "issue_codes",
                ]
            )
            for row in payload["rows"]:
                writer.writerow(
                    [
                        row["entry_id"],
                        row["headword_raw"],
                        row["headword_effective"],
                        row["headword_norm"],
                        row["pron_raw"],
                        row["pron_effective"],
                        row["pron_norm"],
                        row["pron_render"],
                        json.dumps(row["features"], ensure_ascii=False, sort_keys=True),
                        ",".join(row["issue_codes"]),
                    ]
                )
        payload["preview_csv"] = str(out)
    _print_json(payload)


@convention_app.command("apply")
def convention_apply(
    project_dir: Path,
    corpus: str = typer.Option(..., "--corpus"),
    profile: str = typer.Option(..., "--profile"),
) -> None:
    try:
        payload = apply_convention(
            project_dir=project_dir,
            corpus_id=corpus,
            profile_ref=profile,
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    _print_json(payload)


def _resolve_compare_pair(
    corpus_ids: list[str], corpus_a: str | None, corpus_b: str | None
) -> tuple[str, str]:
    unique = [corpus_id.strip() for corpus_id in corpus_ids if corpus_id.strip()]
    if len(unique) < 2:
        raise typer.BadParameter("Provide at least two --corpus values")
    a = corpus_a.strip() if corpus_a else unique[0]
    b = corpus_b.strip() if corpus_b else unique[1]
    if a == b:
        raise typer.BadParameter("corpus-a and corpus-b must be different")
    if a not in unique or b not in unique:
        raise typer.BadParameter("corpus-a and corpus-b must be in --corpus list")
    return a, b


@compare_app.command("preview")
def compare_preview(
    project_dir: Path,
    corpus: Annotated[list[str], typer.Option("--corpus", help="Repeat for each corpus")],
    corpus_a: str | None = typer.Option(None, "--corpus-a"),
    corpus_b: str | None = typer.Option(None, "--corpus-b"),
    mode: str = typer.Option("exact+fuzzy", "--mode"),
    threshold: int = typer.Option(90, "--threshold", min=70, max=95),
    key_field: str = typer.Option("headword_norm_effective", "--key-field"),
    limit: int = typer.Option(500, "--limit", min=1),
    coverage_filter: str = typer.Option("all", "--coverage-filter"),
    diff_filter: str = typer.Option("all", "--diff-filter"),
    include_unmatched: bool = typer.Option(True, "--include-unmatched/--no-include-unmatched"),
) -> None:
    paths = init_project(project_dir)
    try:
        a, b = _resolve_compare_pair(corpus, corpus_a, corpus_b)
        coverage = preview_coverage(
            db_path=paths.db_path,
            corpus_ids=corpus,
            limit=limit,
            filters={"mode": coverage_filter},
            key_field=key_field,
        )
        alignment = preview_alignment(
            db_path=paths.db_path,
            corpus_a=a,
            corpus_b=b,
            mode=mode,
            threshold=threshold,
            limit=limit,
            key_field=key_field,
            include_unmatched=include_unmatched,
        )
        diff = preview_diff(
            db_path=paths.db_path,
            run_settings={
                "alignment_rows": alignment["rows"],
                "corpus_a": a,
                "corpus_b": b,
            },
            limit=limit,
            filters={"mode": diff_filter},
        )
    except (CompareWorkflowError, typer.BadParameter) as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    _print_json(
        {
            "coverage": coverage,
            "alignment": alignment,
            "diff": diff,
        }
    )


@compare_app.command("apply")
def compare_apply(
    project_dir: Path,
    corpus: Annotated[list[str], typer.Option("--corpus", help="Repeat for each corpus")],
    corpus_a: str | None = typer.Option(None, "--corpus-a"),
    corpus_b: str | None = typer.Option(None, "--corpus-b"),
    mode: str = typer.Option("exact+fuzzy", "--mode"),
    threshold: int = typer.Option(90, "--threshold", min=70, max=95),
    key_field: str = typer.Option("headword_norm_effective", "--key-field"),
    algorithm: str = typer.Option("greedy", "--algorithm"),
) -> None:
    paths = init_project(project_dir)
    try:
        a, b = _resolve_compare_pair(corpus, corpus_a, corpus_b)
        payload = apply_compare_run(
            db_path=paths.db_path,
            corpus_ids=corpus,
            corpus_a=a,
            corpus_b=b,
            settings={
                "key_field": key_field,
                "mode": mode,
                "fuzzy_threshold": threshold,
                "algorithm": algorithm,
            },
        )
    except (CompareWorkflowError, typer.BadParameter) as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    _print_json(payload)


@compare_app.command("runs")
def compare_runs(project_dir: Path, limit: int = typer.Option(30, "--limit", min=1)) -> None:
    paths = init_project(project_dir)
    rows = list_compare_runs(paths.db_path, limit=limit)
    _print_json({"count": len(rows), "runs": rows})


@compare_app.command("export")
def compare_export(
    project_dir: Path,
    run_id: str = typer.Option(..., "--run-id"),
    kind: str = typer.Option(..., "--kind", help="coverage|alignment|diff"),
    out: Path = typer.Option(..., "--out"),
) -> None:
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    clean_kind = kind.strip().lower()
    try:
        if clean_kind == "coverage":
            path = export_compare_coverage(store, run_id, out)
        elif clean_kind == "alignment":
            path = export_compare_alignment(store, run_id, out)
        elif clean_kind == "diff":
            path = export_compare_diff(store, run_id, out)
        else:
            raise typer.BadParameter("kind must be one of: coverage, alignment, diff")
        payload = load_compare_run_data(paths.db_path, run_id)
    except (CompareWorkflowError, typer.BadParameter) as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    _print_json(
        {
            "run_id": run_id,
            "kind": clean_kind,
            "out": str(path),
            "stats": payload.get("run", {}).get("stats", {}),
        }
    )


@template_app.command("sources")
def template_sources(project_dir: Path) -> None:
    init_project(project_dir)
    sources = list_template_sources(project_dir)
    _print_json(
        {
            "project_dir": str(project_dir.resolve()),
            "count": len(sources),
            "sources": [str(path) for path in sources],
        }
    )


@template_app.command("preview")
def template_preview(
    project_dir: Path,
    corpus: str = typer.Option(..., "--corpus"),
    kind: TemplateKind = typer.Option(..., "--kind"),
    source: Path | None = typer.Option(
        None,
        "--source",
        help="Optional source file path. Defaults to first matching imported source.",
    ),
    params: str | None = typer.Option(
        None,
        "--params",
        help="JSON object with template parameters.",
    ),
    limit: int = typer.Option(200, "--limit", min=1, max=10000),
) -> None:
    init_project(project_dir)
    source_path = _resolve_template_source(project_dir, source, kind)
    params_dict = _parse_json_params(params)
    preview = preview_template_on_source(
        project_dir=project_dir,
        source_path=source_path,
        kind=kind,
        params=params_dict,
        corpus_id=corpus,
        limit=limit,
    )
    preview["corpus_id"] = corpus
    preview["source_path"] = str(source_path)
    preview["kind"] = kind.value
    _print_json(preview)


@template_app.command("apply")
def template_apply(
    project_dir: Path,
    corpus: str = typer.Option(..., "--corpus"),
    kind: TemplateKind = typer.Option(..., "--kind"),
    source: Path | None = typer.Option(
        None,
        "--source",
        help="Optional source file path. Defaults to first matching imported source.",
    ),
    params: str | None = typer.Option(
        None,
        "--params",
        help="JSON object with template parameters.",
    ),
    template_id: str | None = typer.Option(None, "--template-id"),
    version: int = typer.Option(1, "--version", min=1),
    apply_profile: str | None = typer.Option(
        None,
        "--apply-profile",
        help="Optional profile to apply immediately after template extraction.",
    ),
) -> None:
    init_project(project_dir)
    source_path = _resolve_template_source(project_dir, source, kind)
    params_dict = _parse_json_params(params)
    spec = TemplateSpec(
        template_id=template_id or f"{kind.value}_v{version}",
        kind=kind,
        version=version,
        params=params_dict,
    )
    try:
        summary = apply_template_to_corpus(
            project_dir=project_dir,
            corpus_id=corpus,
            source_path=source_path,
            template_spec=spec,
            apply_profile=apply_profile,
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    _print_json(summary)


@app.command("align")
def align_cmd(
    project_dir: Path,
    dict_a: str = typer.Option(..., "--dict-a"),
    dict_b: str = typer.Option(..., "--dict-b"),
    min_score: float = typer.Option(85.0, "--min-score"),
    commit: bool = typer.Option(True, "--commit/--no-commit"),
) -> None:
    paths = project_paths(project_dir)
    store = SQLiteStore(paths.db_path)

    candidates = match_dictionaries(store, dict_a, dict_b, min_fuzzy_score=min_score)
    if commit:
        cluster_matches(store, candidates, min_score=min_score)

    payload = {
        "dict_a": dict_a,
        "dict_b": dict_b,
        "candidates": len(candidates),
        "commit": commit,
    }
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@export_app.command("entries")
def export_entries(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
    fmt: str = typer.Option("csv", "--format", help="csv|jsonl"),
    out: Path | None = typer.Option(None, "--out"),
) -> None:
    store = SQLiteStore(project_paths(project_dir).db_path)
    out = out or (
        project_paths(project_dir).derived_dir / f"{dict_id}.{'jsonl' if fmt == 'jsonl' else 'csv'}"
    )

    if fmt == "jsonl":
        result = export_entries_jsonl(store, dict_id, out)
    else:
        result = export_entries_csv(store, dict_id, out)
    typer.echo(str(result))


@export_app.command("compare")
def export_compare(
    project_dir: Path,
    dict_ids: Annotated[list[str], typer.Option("--dict-id", help="Repeat for each dictionary")],
    xlsx_out: Path | None = typer.Option(None, "--xlsx"),
    docx_out: Path | None = typer.Option(None, "--docx"),
) -> None:
    paths = project_paths(project_dir)
    store = SQLiteStore(paths.db_path)

    xlsx_out = xlsx_out or (paths.derived_dir / "comparison.xlsx")
    docx_out = docx_out or (paths.derived_dir / "comparison.docx")

    xlsx_path = export_comparison_xlsx(store, dict_ids, xlsx_out)
    docx_path = export_comparison_docx(store, dict_ids, docx_out)

    typer.echo(json.dumps({"xlsx": str(xlsx_path), "docx": str(docx_path)}, indent=2))


@app.command("diagnostics")
def diagnostics(project_dir: Path, dict_id: str | None = typer.Option(None, "--dict-id")) -> None:
    store = SQLiteStore(project_paths(project_dir).db_path)
    payload = {
        "entries": store.count_entries(dict_id) if dict_id else None,
        "issues": store.count_issues(dict_id),
        "top_issues": [dict(row) for row in store.top_issues(dict_id=dict_id)],
    }
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@filter_app.command("accent")
def filter_accent(
    project_dir: Path | None = typer.Option(None, "--project"),
    dict_id: str | None = typer.Option(None, "--dict-id"),
    csv_path: Path | None = typer.Option(None, "--csv"),
) -> None:
    """List entries containing accented characters (legacy Extraction replacement)."""
    if csv_path:
        rows = _load_rows_from_csv(csv_path)
    elif project_dir and dict_id:
        rows = _load_rows_from_db(project_dir, dict_id)
    else:
        raise typer.BadParameter("Use --csv OR (--project and --dict-id)")

    filtered = filter_by_accents(rows)
    typer.echo(
        json.dumps({"count": len(filtered), "rows": filtered[:100]}, ensure_ascii=False, indent=2)
    )


@filter_app.command("syllables")
def filter_syllables(
    project_dir: Path,
    dict_id: str,
    value: int,
) -> None:
    rows = _load_rows_from_db(project_dir, dict_id)
    filtered = filter_by_syllables(rows, value)
    typer.echo(
        json.dumps({"count": len(filtered), "rows": filtered[:100]}, ensure_ascii=False, indent=2)
    )


@filter_app.command("startswith")
def filter_startswith(
    project_dir: Path,
    dict_id: str,
    prefixes: Annotated[list[str], typer.Option("--prefix")],
) -> None:
    """Prefix filter with exact string prefixes (fixes startswith(int) legacy bug)."""
    rows = _load_rows_from_db(project_dir, dict_id)
    filtered = filter_by_prefix(rows, prefixes)

    typer.echo(
        json.dumps({"count": len(filtered), "rows": filtered[:100]}, ensure_ascii=False, indent=2)
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
