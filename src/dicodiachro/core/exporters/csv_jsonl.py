from __future__ import annotations

import csv
import json
from pathlib import Path

from ..storage.sqlite import SQLiteStore

ENTRY_FIELDS = [
    "entry_id",
    "dict_id",
    "section",
    "syllables",
    "headword_raw",
    "pos_raw",
    "pron_raw",
    "form_display",
    "form_norm",
    "features_json",
    "source_path",
    "line_no",
    "created_at",
]


def export_entries_csv(store: SQLiteStore, dict_id: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    entries = store.entries_for_dict(dict_id)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ENTRY_FIELDS)
        writer.writeheader()
        for row in entries:
            writer.writerow({field: row[field] for field in ENTRY_FIELDS})
    return out_path


def export_entries_jsonl(store: SQLiteStore, dict_id: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    entries = store.entries_for_dict(dict_id)
    with out_path.open("w", encoding="utf-8") as handle:
        for row in entries:
            payload = {field: row[field] for field in ENTRY_FIELDS}
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return out_path


def import_entries_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def import_entries_jsonl(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            rows.append(json.loads(line))
    return rows
