from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Annotated

import typer

from dicodiachro.core.align.cluster import cluster_matches
from dicodiachro.core.align.match import match_dictionaries
from dicodiachro.core.exporters.csv_jsonl import (
    export_entries_csv,
    export_entries_jsonl,
    import_entries_csv,
    import_entries_jsonl,
)
from dicodiachro.core.exporters.excel_xlsx import export_comparison_xlsx
from dicodiachro.core.exporters.word_docx import export_comparison_docx
from dicodiachro.core.filters import filter_by_accents, filter_by_prefix, filter_by_syllables
from dicodiachro.core.importers.pdf_import import save_pdf_as_text
from dicodiachro.core.importers.text_import import (
    import_text_batch,
    list_text_files,
    merge_text_files,
)
from dicodiachro.core.importers.url_import import import_from_share_link
from dicodiachro.core.models import ParsedEntry, ProfileApplied
from dicodiachro.core.pipeline import (
    PipelineError,
    apply_profile_to_entries,
    register_import_event,
    run_pipeline,
)
from dicodiachro.core.storage.sqlite import SQLiteStore, entry_id_for, init_project, project_paths

app = typer.Typer(help="DicoDiachro CLI", rich_markup_mode=None)
import_app = typer.Typer(help="Import data into a local project", rich_markup_mode=None)
export_app = typer.Typer(help="Export dictionaries and comparisons", rich_markup_mode=None)
filter_app = typer.Typer(help="Filter entries from DB or CSV", rich_markup_mode=None)

app.add_typer(import_app, name="import")
app.add_typer(export_app, name="export")
app.add_typer(filter_app, name="filter")


def _ensure_templates(paths: Path) -> None:
    template_dir = Path("rules") / "templates"
    if not template_dir.exists():
        template_dir = Path(__file__).resolve().parents[3] / "rules" / "templates"
    if not template_dir.exists():
        return
    for template in template_dir.glob("*.yml"):
        dst = paths / template.name
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
def import_pdf(
    project_dir: Path,
    pdf_path: Path,
    use_coords: bool = typer.Option(
        False, help="Use coords-based extraction with pdfplumber.extract_words."
    ),
) -> None:
    paths = init_project(project_dir)
    target_txt = paths.raw_dir / "imports" / f"{pdf_path.stem}.txt"
    output = save_pdf_as_text(pdf_path, target_txt, use_coords=use_coords)
    register_import_event(
        project_dir,
        {
            "type": "pdf",
            "pdf_path": str(pdf_path),
            "output_txt": str(output),
            "coords_based": use_coords,
        },
    )
    typer.echo(f"Converted PDF to text: {output}")


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
        )
    except PipelineError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


@app.command("apply-profile")
def apply_profile_cmd(
    project_dir: Path,
    dict_id: str = typer.Option(..., "--dict-id"),
    profile: str = typer.Option(..., "--profile"),
) -> None:
    summary = apply_profile_to_entries(project_dir, dict_id=dict_id, profile_name=profile)
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


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
