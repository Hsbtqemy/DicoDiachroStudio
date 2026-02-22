from __future__ import annotations

from .spec import SourceRecord


def available_csv_columns(records: list[SourceRecord]) -> list[str]:
    columns: set[str] = set()
    for record in records:
        if record.csv_row:
            columns.update(record.csv_row.keys())
    return sorted(columns)


def split_cell(value: str, mode: str) -> list[str]:
    text = value or ""
    if mode == "whitespace":
        return [part for part in text.split() if part]
    if mode == "semicolon":
        return [part.strip() for part in text.split(";") if part.strip()]
    if mode == "comma":
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text.strip()] if text.strip() else []
