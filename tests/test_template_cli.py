from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dicodiachro.cli.app import app
from dicodiachro.core.storage.sqlite import SQLiteStore, project_paths


def test_cli_template_preview_and_apply(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    source_txt = tmp_path / "source.txt"
    source_txt.write_text("alpha beta\n", encoding="utf-8")

    runner = CliRunner()

    init_result = runner.invoke(app, ["init", str(project_dir)])
    assert init_result.exit_code == 0, init_result.output

    import_result = runner.invoke(app, ["import", "text", str(project_dir), str(source_txt)])
    assert import_result.exit_code == 0, import_result.output

    preview_result = runner.invoke(
        app,
        [
            "template",
            "preview",
            str(project_dir),
            "--corpus",
            "corpus_cli",
            "--kind",
            "wordlist_tokens",
        ],
    )
    assert preview_result.exit_code == 0, preview_result.output
    assert '"entries_count": 2' in preview_result.output

    apply_result = runner.invoke(
        app,
        [
            "template",
            "apply",
            str(project_dir),
            "--corpus",
            "corpus_cli",
            "--kind",
            "wordlist_tokens",
        ],
    )
    assert apply_result.exit_code == 0, apply_result.output

    store = SQLiteStore(project_paths(project_dir).db_path)
    assert store.count_entries("corpus_cli") == 2
