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
    "headword_edit",
    "headword_effective",
    "pos_raw",
    "pron_raw",
    "pron_edit",
    "pron_effective",
    "definition_raw",
    "definition_edit",
    "definition_effective",
    "status",
    "origin_raw",
    "origin_norm",
    "pos_norm",
    "parser_id",
    "parser_version",
    "parser_sha256",
    "source_record",
    "source_id",
    "record_key",
    "template_id",
    "template_version",
    "template_sha256",
    "form_display",
    "form_display_effective",
    "form_norm",
    "form_norm_effective",
    "headword_norm",
    "pron_norm",
    "pron_render",
    "profile_id",
    "profile_version",
    "profile_sha256",
    "features_json",
    "source_path",
    "line_no",
    "page",
    "created_at",
]


def _effective(row, stem: str) -> str:
    edit = row[f"{stem}_edit"] if f"{stem}_edit" in row.keys() else None
    if edit is not None and str(edit).strip():
        return str(edit)
    raw = row[f"{stem}_raw"] if f"{stem}_raw" in row.keys() else None
    return str(raw or "")


def _export_payload(row) -> dict[str, str]:
    headword_effective = _effective(row, "headword")
    pron_effective = _effective(row, "pron")
    definition_effective = _effective(row, "definition")
    form_display = str(row["form_display"] or "")
    form_norm = str(row["form_norm"] or "")
    headword_norm = str(row["headword_norm"] or "")
    pron_norm = str(row["pron_norm"] or "")
    pron_render = str(row["pron_render"] or "")
    if headword_effective != str(row["headword_raw"] or "") or pron_effective != str(
        row["pron_raw"] or ""
    ):
        form_display_effective = pron_effective or headword_effective
        form_norm_effective = form_norm or form_display_effective.lower()
    else:
        form_display_effective = form_display or pron_effective or headword_effective
        form_norm_effective = form_norm or form_display_effective.lower()

    payload = {field: "" for field in ENTRY_FIELDS}
    payload.update(
        {
            "entry_id": row["entry_id"],
            "dict_id": row["dict_id"],
            "section": row["section"] or "",
            "syllables": str(row["syllables"]),
            "headword_raw": row["headword_raw"] or "",
            "headword_edit": row["headword_edit"] or "",
            "headword_effective": headword_effective,
            "pos_raw": row["pos_raw"] or "",
            "pron_raw": row["pron_raw"] or "",
            "pron_edit": row["pron_edit"] or "",
            "pron_effective": pron_effective,
            "definition_raw": row["definition_raw"] or "",
            "definition_edit": row["definition_edit"] or "",
            "definition_effective": definition_effective,
            "status": row["status"] or "auto",
            "origin_raw": row["origin_raw"] or "",
            "origin_norm": row["origin_norm"] or "",
            "pos_norm": row["pos_norm"] or "",
            "parser_id": row["parser_id"] or "",
            "parser_version": row["parser_version"] or "",
            "parser_sha256": row["parser_sha256"] or "",
            "source_record": row["source_record"] or "",
            "source_id": row["source_id"] or "",
            "record_key": row["record_key"] or "",
            "template_id": row["template_id"] or "",
            "template_version": row["template_version"] or "",
            "template_sha256": row["template_sha256"] or "",
            "form_display": form_display,
            "form_display_effective": form_display_effective,
            "form_norm": form_norm,
            "form_norm_effective": form_norm_effective,
            "headword_norm": headword_norm,
            "pron_norm": pron_norm,
            "pron_render": pron_render,
            "profile_id": row["profile_id"] or "",
            "profile_version": row["profile_version"] or "",
            "profile_sha256": row["profile_sha256"] or "",
            "features_json": row["features_json"] or "{}",
            "source_path": row["source_path"] or "",
            "line_no": row["line_no"] or "",
            "page": row["page"] if row.get("page") is not None else "",
            "created_at": row["created_at"] or "",
        }
    )
    return payload


def export_entries_csv(store: SQLiteStore, dict_id: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    entries = store.entries_for_dict(dict_id)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ENTRY_FIELDS)
        writer.writeheader()
        for row in entries:
            writer.writerow(_export_payload(row))
    return out_path


def export_entries_jsonl(store: SQLiteStore, dict_id: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    entries = store.entries_for_dict(dict_id)
    with out_path.open("w", encoding="utf-8") as handle:
        for row in entries:
            payload = _export_payload(row)
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
