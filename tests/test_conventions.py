from __future__ import annotations

from pathlib import Path

from dicodiachro.core.conventions.workflow import apply_convention, preview_convention
from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.profiles import (
    apply_profile,
    build_profile_from_ui,
    load_profile,
    profile_to_yaml_text,
)
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project, project_paths


def _seed_project(tmp_path: Path) -> tuple[Path, str, SQLiteStore]:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(project_paths(project_dir).db_path)

    corpus_id = "corpus_test"
    store.ensure_dictionary(corpus_id, label="Corpus Test")

    entries = [
        ParsedEntry(
            dict_id=corpus_id,
            section="JU",
            syllables=3,
            headword_raw="júvenal",
            pos_raw="v",
            pron_raw="júvenal",
            source_path=str(paths.raw_dir / "imports" / "sample.txt"),
            line_no=1,
            raw_line="3 júvenal, v",
        ),
        ParsedEntry(
            dict_id=corpus_id,
            section="KI",
            syllables=3,
            headword_raw="alendar",
            pos_raw="ſ",
            pron_raw="áʹlendar",
            source_path=str(paths.raw_dir / "imports" / "sample.txt"),
            line_no=2,
            raw_line="3 áʹlendar, ſ",
        ),
    ]
    store.insert_entries(entries)
    return project_dir, corpus_id, store


def test_convention_preview_contains_norm_and_render(tmp_path: Path) -> None:
    project_dir, corpus_id, _ = _seed_project(tmp_path)
    profile_path = Path("tests/data/profile_convention_render.yml")

    preview = preview_convention(project_dir, corpus_id, str(profile_path), limit=200)

    assert preview["entries_analyzed"] == 2
    first = preview["rows"][0]
    assert "headword_norm" in first
    assert "pron_norm" in first
    assert "pron_render" in first


def test_preview_convention_respects_entry_ids(tmp_path: Path) -> None:
    project_dir, corpus_id, store = _seed_project(tmp_path)
    profile_path = Path("tests/data/profile_convention_render.yml")
    seeded_rows = store.entries_for_dict(corpus_id)
    selected_entry_id = str(seeded_rows[1]["entry_id"])

    preview = preview_convention(
        project_dir,
        corpus_id,
        str(profile_path),
        limit=200,
        entry_ids=[selected_entry_id],
    )

    assert preview["entries_analyzed"] == 1
    assert [str(row["entry_id"]) for row in preview["rows"]] == [selected_entry_id]


def test_convention_apply_writes_fields_and_logs_application(tmp_path: Path) -> None:
    project_dir, corpus_id, store = _seed_project(tmp_path)
    profile_path = Path("tests/data/profile_convention_render.yml")

    result = apply_convention(project_dir, corpus_id, str(profile_path))

    assert result["entries_updated"] == 2

    rows = store.entries_for_dict(corpus_id)
    assert rows[0]["headword_raw"] in {"júvenal", "alendar"}
    assert rows[0]["headword_norm"] is not None
    assert rows[0]["pron_norm"] is not None
    assert rows[0]["pron_render"] is not None

    history = store.list_convention_applications(corpus_id, limit=10)
    assert history
    assert history[0]["profile_id"] == "convention_render_v1"


def test_render_parentheses_basic() -> None:
    profile = load_profile(Path("tests/data/profile_convention_render.yml"))
    applied = apply_profile("áʹlendar", profile)

    assert "(" in applied.form_render
    assert ")" in applied.form_render


def test_qa_unknown_symbol_visible_in_preview(tmp_path: Path) -> None:
    project_dir, corpus_id, store = _seed_project(tmp_path)
    paths = project_paths(project_dir)

    extra = ParsedEntry(
        dict_id=corpus_id,
        section="JU",
        syllables=1,
        headword_raw="abc",
        pos_raw="v",
        pron_raw="abc☃",
        source_path=str(paths.raw_dir / "imports" / "sample.txt"),
        line_no=3,
        raw_line="1 abc, v",
    )
    store.insert_entries([extra])

    preview = preview_convention(
        project_dir,
        corpus_id,
        str(Path("tests/data/profile_convention_render.yml")),
        limit=200,
    )

    assert any("UNKNOWN_SYMBOL" in row["issue_codes"] for row in preview["rows"])


def test_ui_profile_settings_change_convention_preview(tmp_path: Path) -> None:
    project_dir, corpus_id, _ = _seed_project(tmp_path)

    base_profile = load_profile(Path("rules/templates/reading_v1.yml"))
    ui_profile = build_profile_from_ui(
        base_profile,
        {
            "profile_id": "reading_ui_preview_test",
            "name": "Reading UI Preview Test",
            "long_s_to_s": True,
            "lowercase": True,
            "strip_diacritics": True,
            "normalize_spaces": True,
            "remove_punctuation": True,
            "render_mode": "none",
            "stress_mode": "both",
            "require_pronunciation": False,
            "enforce_stress_consistency": False,
        },
    )
    profile_path = tmp_path / "ui_profile.yml"
    profile_path.write_text(profile_to_yaml_text(ui_profile), encoding="utf-8")

    preview = preview_convention(project_dir, corpus_id, str(profile_path), limit=200)
    assert preview["rows"]
    first_row = preview["rows"][0]
    assert first_row["headword_norm"] == "juvenal"
