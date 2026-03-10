from __future__ import annotations

from pathlib import Path

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths
from dicodiachro.core.templates.spec import TemplateKind, TemplateSpec
from dicodiachro.core.templates.workflow import apply_template_to_corpus, preview_template_on_source


def _write_shared_source_filters(project_dir: Path) -> None:
    filters_path = project_dir / "rules" / "source_filters" / "source_filters.yml"
    filters_path.parent.mkdir(parents=True, exist_ok=True)
    filters_path.write_text(
        "\n".join(
            [
                "enabled: true",
                "default:",
                "  drop_before_regex: '^alpha beta$'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_template_preview_applies_source_filters_on_text_source(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    source = paths.raw_dir / "imports" / "sample.txt"
    source.write_text("PREFACE\nalpha beta\ngamma\n", encoding="utf-8")
    _write_shared_source_filters(project_dir)

    preview = preview_template_on_source(
        project_dir=project_dir,
        source_path=source,
        kind=TemplateKind.WORDLIST_TOKENS,
        params={},
        corpus_id=None,
        limit=200,
    )

    assert preview["entries_count"] == 3
    assert all(int(row["record_no"]) != 1 for row in preview["rows"])
    assert preview["source_filters"]["dropped_lines"] == 1


def test_template_apply_applies_source_filters_on_text_source(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    source = paths.raw_dir / "imports" / "sample.txt"
    source.write_text("PREFACE\nalpha beta\ngamma\n", encoding="utf-8")
    _write_shared_source_filters(project_dir)

    spec = TemplateSpec(
        template_id="wordlist_tokens",
        kind=TemplateKind.WORDLIST_TOKENS,
        version=1,
        params={},
    )

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=spec,
    )

    assert summary["entries_count"] == 3
    assert summary["source_filters"]["dropped_lines"] == 1

    store = SQLiteStore(project_paths(project_dir).db_path)
    assert store.count_entries("corpus_test") == 3
