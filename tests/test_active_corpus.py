from __future__ import annotations

from pathlib import Path

from dicodiachro.core.storage.sqlite import (
    SQLiteStore,
    ensure_active_dict,
    init_project,
    project_paths,
)


def test_ensure_active_dict_creates_when_none(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = init_project(project_dir)

    dict_id = ensure_active_dict(paths.db_path, suggested_name="The Spelling Dictionary (1737)")

    store = SQLiteStore(paths.db_path)
    rows = store.list_dictionaries()
    assert len(rows) == 1
    assert rows[0]["dict_id"] == "the_spelling_dictionary_1737"
    assert rows[0]["label"] == "The Spelling Dictionary (1737)"
    assert dict_id == "the_spelling_dictionary_1737"
    assert store.get_active_dict_id() == dict_id


def test_ensure_active_dict_uses_existing_when_no_active(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    init_project(project_dir)
    db_path = project_paths(project_dir).db_path
    store = SQLiteStore(db_path)
    store.ensure_dictionary("alpha", label="Alpha")
    store.ensure_dictionary("beta", label="Beta")

    assert store.get_active_dict_id() is None

    selected = ensure_active_dict(db_path, suggested_name="Ignored")

    assert selected == "alpha"
    assert store.get_active_dict_id() == "alpha"
