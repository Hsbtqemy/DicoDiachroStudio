from __future__ import annotations

import csv
from pathlib import Path

import pytest

from dicodiachro.core.exporters.index_export import export_entries_index
from dicodiachro.core.storage.sqlite import SQLiteStore, connect, init_project


def _setup_project(tmp_path: Path) -> SQLiteStore:
    init_project(tmp_path)
    return SQLiteStore((tmp_path / "project.sqlite"))


def _insert(store: SQLiteStore, *, dict_id: str, headword_raw: str, page: int | None, index_key: str | None = None) -> str:
    entry_id = store.insert_entry(dict_id=dict_id, headword_raw=headword_raw, page=page)
    if index_key is not None:
        store.update_entry_edit_fields(entry_id, dict_id, {"index_key": index_key})
    return entry_id


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------


class TestIndexKeySchema:
    def test_index_key_column_exists(self, tmp_path: Path) -> None:
        init_project(tmp_path)
        db = tmp_path / "project.sqlite"
        with connect(db) as conn:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
        assert "index_key" in cols

    def test_index_key_updatable(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="Dict1")
        eid = store.insert_entry(dict_id="d1", headword_raw="abbaisser")
        store.update_entry_edit_fields(eid, "d1", {"index_key": "abaisser"})
        with connect(store.db_path) as conn:
            row = conn.execute("SELECT index_key FROM entries WHERE entry_id=?", (eid,)).fetchone()
        assert row["index_key"] == "abaisser"


class TestExportEntriesIndex:
    def test_basic_export(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="richelet", label="Richelet 1680")
        store.ensure_dictionary(dict_id="furetiere", label="Furetière 1690")
        _insert(store, dict_id="richelet", headword_raw="abbaisser", page=3, index_key="abaisser")
        _insert(store, dict_id="furetiere", headword_raw="abaisser", page=4, index_key="abaisser")

        out = tmp_path / "index.csv"
        export_entries_index(store, out)

        rows = _read_csv(out)
        assert len(rows) == 2
        assert all(r["index_key"] == "abaisser" for r in rows)
        dict_ids = {r["dict_id"] for r in rows}
        assert dict_ids == {"richelet", "furetiere"}

    def test_sorted_by_index_key(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="D1")
        _insert(store, dict_id="d1", headword_raw="zèle", page=10, index_key="zèle")
        _insert(store, dict_id="d1", headword_raw="abbaisser", page=3, index_key="abaisser")
        _insert(store, dict_id="d1", headword_raw="bras", page=5, index_key="bras")

        out = tmp_path / "index.csv"
        export_entries_index(store, out)

        rows = _read_csv(out)
        keys = [r["index_key"] for r in rows]
        assert keys == sorted(keys, key=str.lower)

    def test_filter_by_dict_ids(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="D1")
        store.ensure_dictionary(dict_id="d2", label="D2")
        _insert(store, dict_id="d1", headword_raw="abaisser", page=1, index_key="abaisser")
        _insert(store, dict_id="d2", headword_raw="abaisser", page=2, index_key="abaisser")

        out = tmp_path / "index.csv"
        export_entries_index(store, out, dict_ids=["d1"])

        rows = _read_csv(out)
        assert all(r["dict_id"] == "d1" for r in rows)
        assert len(rows) == 1

    def test_only_with_page(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="D1")
        _insert(store, dict_id="d1", headword_raw="abaisser", page=3, index_key="abaisser")
        _insert(store, dict_id="d1", headword_raw="abaissement", page=None, index_key="abaissement")

        out = tmp_path / "index.csv"
        export_entries_index(store, out, only_with_page=True)

        rows = _read_csv(out)
        assert len(rows) == 1
        assert rows[0]["index_key"] == "abaisser"
        assert rows[0]["page"] == "3"

    def test_fallback_to_headword_raw_when_no_index_key(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="D1")
        _insert(store, dict_id="d1", headword_raw="abaisser", page=1)  # no index_key

        out = tmp_path / "index.csv"
        export_entries_index(store, out)

        rows = _read_csv(out)
        assert len(rows) == 1
        assert rows[0]["index_key"] == "abaisser"

    def test_headword_effective_uses_edit(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        store.ensure_dictionary(dict_id="d1", label="D1")
        eid = _insert(store, dict_id="d1", headword_raw="abbaisser", page=1, index_key="abaisser")
        store.update_entry_edit_fields(eid, "d1", {"headword_edit": "abaisser (corr.)"})

        out = tmp_path / "index.csv"
        export_entries_index(store, out)

        rows = _read_csv(out)
        assert rows[0]["headword_effective"] == "abaisser (corr.)"
        assert rows[0]["headword_raw"] == "abbaisser"

    def test_empty_project_returns_empty_csv(self, tmp_path: Path) -> None:
        store = _setup_project(tmp_path)
        out = tmp_path / "index.csv"
        export_entries_index(store, out)
        rows = _read_csv(out)
        assert rows == []
