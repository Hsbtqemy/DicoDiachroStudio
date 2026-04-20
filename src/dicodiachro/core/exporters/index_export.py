from __future__ import annotations

import csv
from pathlib import Path

from ..storage.sqlite import SQLiteStore, connect

_INDEX_KEY_EXPR = (
    "COALESCE("
    "NULLIF(TRIM(COALESCE(index_key,'')), ''), "
    "NULLIF(TRIM(COALESCE(headword_norm,'')), ''), "
    "TRIM(COALESCE(headword_edit, headword_raw, '')))"
)


def export_entries_index(
    store: SQLiteStore,
    out_path: Path,
    *,
    dict_ids: list[str] | None = None,
    only_with_page: bool = False,
) -> Path:
    """
    Export a unified index grouped by index_key.

    Each row: index_key, dict_id, headword_raw, headword_effective, page, line_no.
    Sorted by index_key then dict_id then page.

    dict_ids: restrict to these corpora (None = all).
    only_with_page: skip entries without a page number.
    """
    rows = _fetch_index_rows(store, dict_ids=dict_ids, only_with_page=only_with_page)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["index_key", "dict_id", "headword_raw", "headword_effective", "page", "line_no"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def _fetch_index_rows(
    store: SQLiteStore,
    *,
    dict_ids: list[str] | None,
    only_with_page: bool,
) -> list[dict[str, str]]:
    clauses: list[str] = ["is_deleted=0"]
    params: list[object] = []

    if dict_ids:
        placeholders = ",".join("?" * len(dict_ids))
        clauses.append(f"dict_id IN ({placeholders})")
        params.extend(dict_ids)

    if only_with_page:
        clauses.append("page IS NOT NULL")

    where = " AND ".join(clauses)

    with connect(store.db_path) as conn:
        sql_rows = conn.execute(
            f"""
            SELECT
              {_INDEX_KEY_EXPR} AS index_key,
              dict_id,
              headword_raw,
              COALESCE(NULLIF(TRIM(COALESCE(headword_edit,'')), ''), headword_raw)
                AS headword_effective,
              page,
              line_no
            FROM entries
            WHERE {where}
            ORDER BY LOWER(index_key), dict_id, COALESCE(page, 999999), line_no
            """,
            params,
        ).fetchall()

    return [
        {
            "index_key": row["index_key"] or "",
            "dict_id": row["dict_id"] or "",
            "headword_raw": row["headword_raw"] or "",
            "headword_effective": row["headword_effective"] or "",
            "page": str(row["page"]) if row["page"] is not None else "",
            "line_no": str(row["line_no"]) if row["line_no"] is not None else "",
        }
        for row in sql_rows
    ]
