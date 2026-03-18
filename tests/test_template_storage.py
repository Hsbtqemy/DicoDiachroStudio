from __future__ import annotations

from dicodiachro.core.storage.sqlite import SQLiteStore, init_project
from dicodiachro.core.templates.spec import TemplateKind, TemplateSpec, template_sha256
from dicodiachro.core.templates.workflow import apply_template_to_corpus


def test_template_storage_save_get_active_and_history(tmp_path):
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary("corpus_test", label="Corpus Test")

    store.save_active_template(
        corpus_id="corpus_test",
        template_id="wordlist",
        template_kind=TemplateKind.WORDLIST_TOKENS.value,
        version=1,
        params={"trim_token_punctuation": False},
        sha256="sha_v1",
    )
    store.save_active_template(
        corpus_id="corpus_test",
        template_id="wordlist",
        template_kind=TemplateKind.WORDLIST_TOKENS.value,
        version=2,
        params={"trim_token_punctuation": True},
        sha256="sha_v2",
    )

    active = store.get_active_template("corpus_test")
    assert active is not None
    assert active["version"] == 2
    assert active["sha256"] == "sha_v2"

    store.record_template_application(
        corpus_id="corpus_test",
        template_id="wordlist",
        version=2,
        sha256="sha_v2",
        params={"trim_token_punctuation": True},
        source_ids=["a.txt"],
        records_count=10,
        entries_count=8,
        status="ok",
    )

    history = store.list_template_applications("corpus_test", limit=10)
    assert len(history) == 1
    assert history[0]["entries_count"] == 8


def test_template_sha256_is_stable_for_equivalent_payload() -> None:
    spec_a = TemplateSpec(
        template_id="csv_mapping",
        kind=TemplateKind.CSV_MAPPING,
        version=1,
        params={"headword_column": "head", "split_headword": "semicolon"},
    )
    spec_b = TemplateSpec(
        template_id="csv_mapping",
        kind=TemplateKind.CSV_MAPPING,
        version=1,
        params={"split_headword": "semicolon", "headword_column": "head"},
    )

    assert template_sha256(spec_a) == template_sha256(spec_b)


def test_template_sha256_changes_when_pron_from_headword_changes() -> None:
    spec_a = TemplateSpec(
        template_id="wordlist_tokens",
        kind=TemplateKind.WORDLIST_TOKENS,
        version=1,
        params={"trim_token_punctuation": False, "pron_from_headword": False},
    )
    spec_b = TemplateSpec(
        template_id="wordlist_tokens",
        kind=TemplateKind.WORDLIST_TOKENS,
        version=1,
        params={"trim_token_punctuation": False, "pron_from_headword": True},
    )

    assert template_sha256(spec_a) != template_sha256(spec_b)


def test_apply_template_to_corpus_persists_entries_issues_and_metadata(tmp_path):
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    source = paths.raw_dir / "imports" / "words.txt"
    source.write_text("alpha ... beta\n", encoding="utf-8")

    spec = TemplateSpec(
        template_id="wordlist_tokens",
        kind=TemplateKind.WORDLIST_TOKENS,
        version=1,
        params={"trim_token_punctuation": False},
    )

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=spec,
    )

    assert summary["entries_count"] == 2
    assert summary["issues_count"] == 1
    assert summary["template_id"] == "wordlist_tokens"

    rows = store.entries_for_dict("corpus_test")
    assert len(rows) == 2
    assert all(row["template_id"] == "wordlist_tokens" for row in rows)

    active = store.get_active_template("corpus_test")
    assert active is not None
    assert active["template_id"] == "wordlist_tokens"

    history = store.list_template_applications("corpus_test", limit=5)
    assert len(history) >= 1


def test_apply_template_to_corpus_preserves_pos_from_template_entry(tmp_path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)

    source = paths.raw_dir / "imports" / "fr_en_pron.txt"
    source.write_text("Abcéder , v. a.   To impose.   Toû ïmmpâss.\n", encoding="utf-8")

    spec = TemplateSpec(
        template_id="fr_en_pron_three_cols",
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        version=1,
        params={"separator_mode": "triple_spaces"},
    )

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=spec,
    )

    assert summary["entries_count"] == 1
    rows = store.entries_for_dict("corpus_test")
    assert len(rows) == 1
    assert str(rows[0]["pos_raw"]) == "v. a."
