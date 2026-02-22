from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dicodiachro.cli.app import app
from dicodiachro.core.parsers.presets import (
    load_parser_preset,
    parse_line_with_preset,
    preset_sha256_from_path,
)
from dicodiachro.core.parsing import parse_lines
from dicodiachro.core.storage.sqlite import SQLiteStore, project_paths


def test_parser_preset_parse_1752_line() -> None:
    spec = load_parser_preset(Path("tests/data/parser_preset_valid.yml"))
    parsed = parse_line_with_preset("4 aʹrtificer, f. L.", spec)

    assert parsed.matched is True
    assert parsed.values["syllables"] == 4
    assert parsed.values["headword_raw"] == "aʹrtificer"
    assert parsed.values["pos_raw"] == "f."
    assert parsed.values["origin_raw"] == "L."
    assert parsed.values["pos_norm"] == "ſ"
    assert parsed.values["origin_norm"] == "Latin"


def test_parser_preset_non_match_emits_unparsed() -> None:
    spec = load_parser_preset(Path("tests/data/parser_preset_valid.yml"))
    entries, issues = parse_lines(
        ["JU", "klick"],
        dict_id="kittredge_1752",
        source_path="sample.txt",
        parser_preset=spec,
        parser_sha256="abc",
    )

    assert entries == []
    assert any(issue.code == "UNPARSED_LINE" for issue in issues)


def test_parser_fallback_without_preset() -> None:
    entries, issues = parse_lines(
        ["JU", "1 jut, v"],
        dict_id="dict_a",
        source_path="sample.txt",
    )

    assert len(entries) == 1
    assert entries[0].headword_raw == "jut"
    assert all(issue.code != "UNPARSED_LINE" for issue in issues)


def test_cli_parser_validate_and_run_with_parser(tmp_path: Path) -> None:
    runner = CliRunner()
    project_dir = tmp_path / "project"
    source_file = tmp_path / "input_1752.txt"
    source_file.write_text("JU\n4 aʹrtificer, f. L.\n", encoding="utf-8")

    init_result = runner.invoke(app, ["init", str(project_dir)])
    assert init_result.exit_code == 0, init_result.output

    validate_result = runner.invoke(
        app,
        ["parser", "validate", "tests/data/parser_preset_valid.yml"],
    )
    assert validate_result.exit_code == 0, validate_result.output

    run_result = runner.invoke(
        app,
        [
            "run",
            str(project_dir),
            "--dict-id",
            "kittredge_1752",
            "--profile",
            "reading_v1",
            "--parser",
            "tests/data/parser_preset_valid.yml",
            "--source",
            str(source_file),
        ],
    )
    assert run_result.exit_code == 0, run_result.output

    payload = json.loads(run_result.output)
    assert payload["entries"] == 1
    assert payload["parser"]["parser_id"] == "syll_headword_pos_origin_v1"

    store = SQLiteStore(project_paths(project_dir).db_path)
    rows = store.entries_for_dict("kittredge_1752")
    assert len(rows) == 1
    row = rows[0]

    assert row["headword_raw"] == "aʹrtificer"
    assert row["pos_raw"] == "f."
    assert row["pos_norm"] == "ſ"
    assert row["origin_raw"] == "L."
    assert row["origin_norm"] == "Latin"
    assert row["parser_id"] == "syll_headword_pos_origin_v1"
    assert row["parser_version"] == 1
    assert row["parser_sha256"] == preset_sha256_from_path(
        Path("tests/data/parser_preset_valid.yml")
    )
