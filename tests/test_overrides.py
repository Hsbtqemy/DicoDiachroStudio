from __future__ import annotations

from pathlib import Path

from dicodiachro.core.overrides import (
    compute_record_key,
    create_entry,
    create_entry_from_record,
    fill_pron_raw_from_headword,
    list_overrides,
    record_entry_edit,
    restore_entries,
    soft_delete_entries,
    split_entry,
    upsert_override_record,
)
from dicodiachro.core.storage.sqlite import SQLiteStore, init_project
from dicodiachro.core.templates.spec import TemplateKind, TemplateSpec
from dicodiachro.core.templates.workflow import apply_template_to_corpus, preview_template_on_source


def _setup_project(
    tmp_path: Path, corpus_id: str = "corpus_test"
) -> tuple[Path, Path, SQLiteStore]:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)
    store = SQLiteStore(paths.db_path)
    store.ensure_dictionary(dict_id=corpus_id, label=corpus_id)
    source = paths.raw_dir / "imports" / "source.txt"
    return project_dir, source, store


def test_compute_record_key_stable() -> None:
    key_a = compute_record_key("src", "alpha   beta", 1)
    key_b = compute_record_key("src", "alpha beta", 1)
    key_c = compute_record_key("src", "alpha beta", 2)

    assert key_a == key_b
    assert key_a != key_c


def test_create_entry_logs_override(tmp_path: Path) -> None:
    _, _, store = _setup_project(tmp_path)

    entry_id = create_entry(
        store=store,
        corpus_id="corpus_test",
        headword_raw="manual_alpha",
        pron_raw="manual_pron",
        definition_raw="note lexicographique",
        note="manual-create",
    )

    row = store.entry_by_id(entry_id)
    assert row is not None
    assert row["headword_raw"] == "manual_alpha"
    assert row["pron_raw"] == "manual_pron"
    assert row["definition_raw"] == "note lexicographique"
    assert int(row["manual_created"] or 0) == 1
    assert int(row["is_deleted"] or 0) == 0

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
        entry_id=entry_id,
    )
    assert any(item.op == "CREATE_ENTRY" for item in overrides)


def test_create_entry_entry_is_pron(tmp_path: Path) -> None:
    _, _, store = _setup_project(tmp_path)

    entry_id = create_entry(
        store=store,
        corpus_id="corpus_test",
        headword_raw="bléſſing",
        pron_raw=None,
        entry_is_pron=True,
    )

    row = store.entry_by_id(entry_id)
    assert row is not None
    assert row["pron_raw"] == "bléſſing"


def test_create_entry_from_record_links_source(tmp_path: Path) -> None:
    _, source, store = _setup_project(tmp_path)
    source.write_text("unrecognized source line\n", encoding="utf-8")

    source_id = str(source.resolve())
    record_key = compute_record_key(source_id, "unrecognized source line", 1)

    entry_id = create_entry_from_record(
        store=store,
        corpus_id="corpus_test",
        source_id=source_id,
        record_key=record_key,
        headword_raw="rescued_entry",
        source_path=str(source),
        source_record="unrecognized source line",
        line_no=1,
    )

    row = store.entry_by_id(entry_id)
    assert row is not None
    assert row["source_id"] == source_id
    assert row["record_key"] == record_key
    assert row["source_path"] == str(source)

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
        entry_id=entry_id,
    )
    create_events = [item for item in overrides if item.op == "CREATE_ENTRY"]
    assert create_events
    assert create_events[0].source_id == source_id
    assert create_events[0].record_key == record_key


def test_soft_delete_sets_flags_and_logs_override(tmp_path: Path) -> None:
    _, _, store = _setup_project(tmp_path)
    first_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="alpha")
    second_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="beta")

    deleted_count = soft_delete_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[first_id, second_id],
        reason="duplicate",
    )
    assert deleted_count == 2

    first = store.entry_by_id(first_id)
    second = store.entry_by_id(second_id)
    assert first is not None
    assert second is not None
    assert int(first["is_deleted"] or 0) == 1
    assert int(second["is_deleted"] or 0) == 1
    assert str(first["deleted_reason"] or "") == "duplicate"
    assert str(first["deleted_at"] or "").strip()

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
    )
    delete_events = [item for item in overrides if item.op == "DELETE_ENTRY"]
    assert len(delete_events) >= 2


def test_restore_unsets_flags_and_logs_override(tmp_path: Path) -> None:
    _, _, store = _setup_project(tmp_path)
    entry_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="alpha")
    soft_delete_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[entry_id],
        reason="temp",
    )

    restored_count = restore_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[entry_id],
    )
    assert restored_count == 1

    row = store.entry_by_id(entry_id)
    assert row is not None
    assert int(row["is_deleted"] or 0) == 0
    assert row["deleted_at"] is None
    assert row["deleted_reason"] is None

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
        entry_id=entry_id,
    )
    assert any(item.op == "RESTORE_ENTRY" for item in overrides)


def test_list_entries_include_deleted(tmp_path: Path) -> None:
    _, _, store = _setup_project(tmp_path)
    keep_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="keep")
    drop_id = create_entry(store=store, corpus_id="corpus_test", headword_raw="drop")
    soft_delete_entries(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[drop_id],
        reason="test",
    )

    visible = store.list_entries("corpus_test", include_deleted=False)
    visible_ids = {str(row["entry_id"]) for row in visible}
    assert keep_id in visible_ids
    assert drop_id not in visible_ids

    all_rows = store.list_entries("corpus_test", include_deleted=True)
    all_ids = {str(row["entry_id"]) for row in all_rows}
    assert keep_id in all_ids
    assert drop_id in all_ids

    assert store.count_entries("corpus_test", include_deleted=False) == 1
    assert store.count_entries("corpus_test", include_deleted=True) == 2


def test_record_level_skip_applied_on_apply(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alpha beta\n", encoding="utf-8")

    record_key = compute_record_key(str(source.resolve()), "alpha beta", 1)
    upsert_override_record(
        store=store,
        corpus_id="corpus_test",
        source_id=str(source.resolve()),
        record_key=record_key,
        op="SKIP_RECORD",
        before_json={"source": "alpha beta"},
        after_json={"action": "skip"},
    )

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={"trim_token_punctuation": False},
        ),
    )

    assert summary["entries_count"] == 0
    assert summary["overridden_count"] >= 1


def test_record_level_split_applied_on_apply(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alphabeta\n", encoding="utf-8")

    record_key = compute_record_key(str(source.resolve()), "alphabeta", 1)
    upsert_override_record(
        store=store,
        corpus_id="corpus_test",
        source_id=str(source.resolve()),
        record_key=record_key,
        op="SPLIT_RECORD",
        before_json={"source": "alphabeta"},
        after_json={"entries": [{"headword_raw": "alpha"}, {"headword_raw": "beta"}]},
    )

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={"trim_token_punctuation": False},
        ),
    )

    rows = store.entries_for_dict("corpus_test")
    assert summary["entries_count"] == 2
    assert sorted(row["headword_raw"] for row in rows) == ["alpha", "beta"]


def test_wordlist_pron_from_headword_apply_sets_pron_raw(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alpha beta\n", encoding="utf-8")

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={"pron_from_headword": True},
        ),
    )

    rows = store.entries_for_dict("corpus_test")
    assert summary["entries_count"] == 2
    assert all(str(row["pron_raw"] or "") == str(row["headword_raw"] or "") for row in rows)


def test_entry_edit_sets_edit_fields_and_logs(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alpha\n", encoding="utf-8")

    apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={},
        ),
    )

    entry_id = str(store.entries_for_dict("corpus_test")[0]["entry_id"])
    record_entry_edit(
        store=store,
        corpus_id="corpus_test",
        entry_id=entry_id,
        field_changes={"headword_edit": "ALPHA", "pron_edit": "a"},
    )

    edited_row = store.entry_by_id(entry_id)
    assert edited_row is not None
    assert edited_row["headword_edit"] == "ALPHA"
    assert edited_row["pron_edit"] == "a"

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
        entry_id=entry_id,
    )
    assert any(item.op == "EDIT_ENTRY" for item in overrides)


def test_split_entry_creates_entries_and_logs(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alphabeta\n", encoding="utf-8")

    apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={},
        ),
    )

    entry_id = str(store.entries_for_dict("corpus_test")[0]["entry_id"])
    new_ids = split_entry(
        store=store,
        corpus_id="corpus_test",
        entry_id=entry_id,
        parts=["alpha", "beta"],
    )

    assert len(new_ids) == 2
    assert store.count_entries("corpus_test") == 2

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
    )
    assert any(item.op == "SPLIT_ENTRY" for item in overrides)


def test_record_level_split_wordlist_with_diacritics_updates_preview_and_apply(
    tmp_path: Path,
) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("bléſſing Blétſoe\n", encoding="utf-8")

    preview_before = preview_template_on_source(
        project_dir=project_dir,
        source_path=source,
        kind=TemplateKind.WORDLIST_TOKENS,
        params={"trim_token_punctuation": False, "pron_from_headword": True},
        corpus_id="corpus_test",
        limit=200,
    )
    assert preview_before["rows"]
    row = preview_before["rows"][0]
    source_id = str(row["source_id"])
    record_key = str(row["record_key"])
    assert source_id
    assert record_key

    upsert_override_record(
        store=store,
        corpus_id="corpus_test",
        source_id=source_id,
        record_key=record_key,
        op="SPLIT_RECORD",
        before_json={"source": row["source"]},
        after_json={"entries": [{"headword_raw": "bléſſing"}, {"headword_raw": "Blétſoe"}]},
    )

    preview_after = preview_template_on_source(
        project_dir=project_dir,
        source_path=source,
        kind=TemplateKind.WORDLIST_TOKENS,
        params={"trim_token_punctuation": False, "pron_from_headword": True},
        corpus_id="corpus_test",
        limit=200,
    )
    assert preview_after["entries_count"] == 2
    assert any(item["headword_raw"] == "Blétſoe" for item in preview_after["rows"])
    assert any(
        item["headword_raw"] == "Blétſoe" and item["pron_raw"] == "Blétſoe"
        for item in preview_after["rows"]
    )
    assert preview_after["overridden_count"] >= 1

    summary = apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={"trim_token_punctuation": False, "pron_from_headword": True},
        ),
    )

    rows = store.entries_for_dict("corpus_test")
    assert summary["entries_count"] == 2
    assert sorted(row["headword_raw"] for row in rows) == ["Blétſoe", "bléſſing"]
    assert all(str(row["pron_raw"] or "") == str(row["headword_raw"] or "") for row in rows)


def test_fill_pron_raw_from_headword_only_when_empty(tmp_path: Path) -> None:
    project_dir, source, store = _setup_project(tmp_path)
    source.write_text("alpha beta\n", encoding="utf-8")

    apply_template_to_corpus(
        project_dir=project_dir,
        corpus_id="corpus_test",
        source_path=source,
        template_spec=TemplateSpec(
            template_id="wordlist",
            kind=TemplateKind.WORDLIST_TOKENS,
            version=1,
            params={},
        ),
    )

    rows = sorted(store.entries_for_dict("corpus_test"), key=lambda row: str(row["headword_raw"]))
    alpha_id = str(rows[0]["entry_id"])
    beta_id = str(rows[1]["entry_id"])
    store.update_entry_raw_fields(
        entry_id=alpha_id,
        dict_id="corpus_test",
        field_changes={"pron_raw": "already_set"},
    )

    summary = fill_pron_raw_from_headword(
        store=store,
        corpus_id="corpus_test",
        entry_ids=[alpha_id, beta_id],
    )

    alpha_row = store.entry_by_id(alpha_id)
    beta_row = store.entry_by_id(beta_id)
    assert alpha_row is not None
    assert beta_row is not None
    assert alpha_row["pron_raw"] == "already_set"
    assert beta_row["pron_raw"] == beta_row["headword_raw"]
    assert summary["updated"] == 1
    assert summary["skipped_non_empty"] == 1

    overrides = list_overrides(
        store=store,
        corpus_id="corpus_test",
        scope="entry",
        entry_id=beta_id,
    )
    assert any(
        item.op == "EDIT_ENTRY" and str(item.after.get("pron_raw") or "") == "beta"
        for item in overrides
    )
