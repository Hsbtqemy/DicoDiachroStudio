from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .models import ParsedEntry, utc_now_iso
from .storage.sqlite import SQLiteStore, entry_id_for

if TYPE_CHECKING:
    from .templates.spec import EntryDraft, PreviewRow

RECORD_OVERRIDE_OPS = {"SPLIT_RECORD", "SKIP_RECORD", "EDIT_RECORD"}
ENTRY_OVERRIDE_OPS = {
    "CREATE_ENTRY",
    "DELETE_ENTRY",
    "EDIT_ENTRY",
    "SPLIT_ENTRY",
    "MERGE_ENTRY",
    "RESTORE_ENTRY",
    "VALIDATE_ENTRY",
    "REVIEW_ENTRY",
}


@dataclass(slots=True)
class OverrideSpec:
    override_id: int
    corpus_id: str
    scope: str
    source_id: str | None
    record_key: str | None
    entry_id: str | None
    op: str
    before: dict[str, Any]
    after: dict[str, Any]
    created_at: str
    note: str | None


class OverrideError(RuntimeError):
    pass


def _loads_json(value: Any) -> dict[str, Any]:
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def compute_record_key(
    source_id: str,
    source_record: str,
    record_index: int,
    strategy: str = "sha256",
) -> str:
    normalized = " ".join(str(source_record).split())
    payload = f"{source_id}|{record_index}|{normalized}"
    if strategy != "sha256":
        raise OverrideError(f"Unsupported record key strategy: {strategy}")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_to_spec(row: Any) -> OverrideSpec:
    return OverrideSpec(
        override_id=int(row["override_id"]),
        corpus_id=str(row["corpus_id"]),
        scope=str(row["scope"]),
        source_id=(str(row["source_id"]) if row["source_id"] else None),
        record_key=(str(row["record_key"]) if row["record_key"] else None),
        entry_id=(str(row["entry_id"]) if row["entry_id"] else None),
        op=str(row["op"]),
        before=_loads_json(row["before_json"]),
        after=_loads_json(row["after_json"]),
        created_at=str(row["created_at"]),
        note=(str(row["note"]) if row["note"] else None),
    )


def list_overrides(
    store: SQLiteStore,
    corpus_id: str,
    scope: str,
    source_id: str | None = None,
    entry_id: str | None = None,
) -> list[OverrideSpec]:
    rows = store.list_overrides(
        corpus_id=corpus_id,
        scope=scope,
        source_id=source_id,
        entry_id=entry_id,
    )
    return [_row_to_spec(row) for row in rows]


def upsert_override_record(
    store: SQLiteStore,
    corpus_id: str,
    source_id: str,
    record_key: str,
    op: str,
    before_json: dict[str, Any],
    after_json: dict[str, Any],
    note: str | None = None,
) -> int:
    if op not in RECORD_OVERRIDE_OPS:
        raise OverrideError(f"Unsupported record override op: {op}")
    store.delete_record_overrides(corpus_id=corpus_id, source_id=source_id, record_key=record_key)
    return store.insert_override(
        corpus_id=corpus_id,
        scope="record",
        source_id=source_id,
        record_key=record_key,
        entry_id=None,
        op=op,
        before_json=before_json,
        after_json=after_json,
        note=note,
    )


def delete_override(
    store: SQLiteStore,
    corpus_id: str,
    scope: str,
    override_id: int | None = None,
    source_id: str | None = None,
    record_key: str | None = None,
    entry_id: str | None = None,
) -> None:
    if override_id is not None:
        store.delete_override_by_id(override_id)
        return

    if scope == "record" and source_id and record_key:
        store.delete_record_overrides(
            corpus_id=corpus_id, source_id=source_id, record_key=record_key
        )
        return

    if scope == "entry" and entry_id:
        store.delete_entry_overrides(corpus_id=corpus_id, entry_id=entry_id)
        return

    raise OverrideError("delete_override requires override_id or scope-specific keys")


def _row_to_payload(row: PreviewRow | dict[str, Any]) -> dict[str, Any]:
    if hasattr(row, "source") and hasattr(row, "record_key"):
        return {
            "source": row.source,
            "headword_raw": row.headword_raw,
            "pron_raw": row.pron_raw,
            "definition_raw": row.definition_raw,
            "source_id": row.source_id,
            "record_key": row.record_key,
            "status": row.status,
            "reason": row.reason,
            "source_path": row.source_path,
            "record_no": row.record_no,
            "issue_code": row.issue_code,
            "override_op": row.override_op,
        }

    payload = dict(row)
    payload.setdefault("source", "")
    payload.setdefault("headword_raw", "")
    payload.setdefault("pron_raw", "")
    payload.setdefault("definition_raw", "")
    payload.setdefault("source_id", payload.get("source_path", ""))
    payload.setdefault("record_key", "")
    payload.setdefault("status", "OK")
    payload.setdefault("reason", "")
    payload.setdefault("source_path", "")
    payload.setdefault("record_no", 0)
    payload.setdefault("issue_code", None)
    payload.setdefault("override_op", None)
    return payload


def _latest_by_record(overrides: list[OverrideSpec]) -> dict[str, OverrideSpec]:
    latest: dict[str, OverrideSpec] = {}
    for spec in overrides:
        if spec.scope != "record" or not spec.record_key:
            continue
        previous = latest.get(spec.record_key)
        if previous is None or spec.override_id > previous.override_id:
            latest[spec.record_key] = spec
    return latest


def _row_uses_pron_from_headword(row: dict[str, Any]) -> bool:
    headword = str(row.get("headword_raw") or "").strip()
    pron = str(row.get("pron_raw") or "").strip()
    return bool(headword) and pron == headword


def _entry_uses_pron_from_headword(entry: EntryDraft) -> bool:
    headword = str(entry.headword_raw or "").strip()
    pron = str(entry.pron_raw or "").strip()
    return bool(headword) and pron == headword


def apply_record_overrides(
    preview_rows: list[PreviewRow | dict[str, Any]],
    overrides: list[OverrideSpec],
) -> list[dict[str, Any]]:
    override_map = _latest_by_record(overrides)
    grouped: dict[str, list[dict[str, Any]]] = {}
    ordered_keys: list[str] = []

    for row in preview_rows:
        payload = _row_to_payload(row)
        key = str(payload.get("record_key") or "")
        if key not in grouped:
            grouped[key] = []
            ordered_keys.append(key)
        grouped[key].append(payload)

    result: list[dict[str, Any]] = []
    for key in ordered_keys:
        rows = grouped[key]
        spec = override_map.get(key)
        if spec is None:
            result.extend(rows)
            continue

        first = rows[0]
        entries_override = spec.after.get("entries")
        if spec.op == "SKIP_RECORD":
            result.append(
                {
                    **first,
                    "headword_raw": "",
                    "pron_raw": "",
                    "definition_raw": "",
                    "status": "Ignoré",
                    "reason": "override:skip",
                    "issue_code": "SKIP_RECORD",
                    "override_op": spec.op,
                }
            )
            continue

        if spec.op in {"SPLIT_RECORD", "EDIT_RECORD"} and isinstance(entries_override, list):
            new_rows: list[dict[str, Any]] = []
            for item in entries_override:
                if not isinstance(item, dict):
                    continue
                headword = str(item.get("headword_raw") or "").strip()
                if not headword:
                    continue
                pron = str(item.get("pron_raw") or "").strip()
                if not pron and _row_uses_pron_from_headword(first):
                    pron = headword
                if not pron:
                    pron = str(first.get("pron_raw") or "").strip()
                new_rows.append(
                    {
                        **first,
                        "headword_raw": headword,
                        "pron_raw": pron,
                        "definition_raw": str(item.get("definition_raw") or "").strip(),
                        "status": "OK",
                        "reason": f"override:{spec.op.lower()}",
                        "issue_code": None,
                        "override_op": spec.op,
                    }
                )
            if new_rows:
                result.extend(new_rows)
                continue

        if spec.op == "EDIT_RECORD":
            edited_headword = str(
                spec.after.get("headword_raw") or first.get("headword_raw") or ""
            ).strip()
            edited_pron = str(spec.after.get("pron_raw") or "").strip()
            if not edited_pron and _row_uses_pron_from_headword(first):
                edited_pron = edited_headword
            if not edited_pron:
                edited_pron = str(first.get("pron_raw") or "")
            result.append(
                {
                    **first,
                    "headword_raw": edited_headword,
                    "pron_raw": edited_pron,
                    "definition_raw": str(
                        spec.after.get("definition_raw") or first.get("definition_raw") or ""
                    ),
                    "status": "OK",
                    "reason": "override:edit",
                    "issue_code": None,
                    "override_op": spec.op,
                }
            )
            continue

        result.extend([{**row, "override_op": spec.op} for row in rows])

    return result


def _apply_record_overrides_to_entries(
    entries: list[EntryDraft],
    overrides: list[OverrideSpec],
) -> list[EntryDraft]:
    from .templates.spec import EntryDraft

    override_map = _latest_by_record(overrides)
    grouped: dict[str, list[EntryDraft]] = {}
    ordered_keys: list[str] = []
    for entry in entries:
        key = entry.record_key
        if key not in grouped:
            grouped[key] = []
            ordered_keys.append(key)
        grouped[key].append(entry)

    output: list[EntryDraft] = []
    for key in ordered_keys:
        group = grouped[key]
        spec = override_map.get(key)
        if spec is None:
            output.extend(group)
            continue

        first = group[0]
        if spec.op == "SKIP_RECORD":
            continue

        entries_override = spec.after.get("entries")
        if spec.op in {"SPLIT_RECORD", "EDIT_RECORD"} and isinstance(entries_override, list):
            for item in entries_override:
                if not isinstance(item, dict):
                    continue
                headword = str(item.get("headword_raw") or "").strip()
                if not headword:
                    continue
                pron = str(item.get("pron_raw") or "").strip()
                if not pron and _entry_uses_pron_from_headword(first):
                    pron = headword
                if not pron:
                    pron = first.pron_raw or ""
                output.append(
                    EntryDraft(
                        headword_raw=headword,
                        pron_raw=pron or None,
                        definition_raw=(
                            str(item.get("definition_raw") or "").strip()
                            or first.definition_raw
                            or None
                        ),
                        source_id=first.source_id,
                        record_key=first.record_key,
                        source_path=first.source_path,
                        record_no=first.record_no,
                        source_record=first.source_record,
                    )
                )
            continue

        if spec.op == "EDIT_RECORD":
            edited_headword = str(spec.after.get("headword_raw") or first.headword_raw).strip()
            edited_pron = str(spec.after.get("pron_raw") or "").strip()
            if not edited_pron and _entry_uses_pron_from_headword(first):
                edited_pron = edited_headword
            if not edited_pron:
                edited_pron = first.pron_raw or ""
            edited = EntryDraft(
                headword_raw=edited_headword,
                pron_raw=edited_pron or None,
                definition_raw=(
                    str(spec.after.get("definition_raw") or "").strip()
                    or first.definition_raw
                    or None
                ),
                source_id=first.source_id,
                record_key=first.record_key,
                source_path=first.source_path,
                record_no=first.record_no,
                source_record=first.source_record,
            )
            output.append(edited)
            continue

        output.extend(group)

    return output


def apply_record_overrides_to_apply(
    entries: list[EntryDraft],
    preview_rows: list[PreviewRow],
    overrides: list[OverrideSpec],
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    from .templates.spec import PreviewRow

    preview_payloads = apply_record_overrides(preview_rows, overrides)
    preview_result = [
        PreviewRow(
            source=str(row.get("source", "")),
            headword_raw=str(row.get("headword_raw", "")),
            pron_raw=str(row.get("pron_raw", "")),
            definition_raw=str(row.get("definition_raw", "")),
            source_id=str(row.get("source_id", "")),
            record_key=str(row.get("record_key", "")),
            status=str(row.get("status", "OK")),
            reason=str(row.get("reason", "")),
            source_path=str(row.get("source_path", "")),
            record_no=int(row.get("record_no", 0)),
            issue_code=(str(row["issue_code"]) if row.get("issue_code") else None),
            override_op=(str(row["override_op"]) if row.get("override_op") else None),
        )
        for row in preview_payloads
    ]
    return _apply_record_overrides_to_entries(entries, overrides), preview_result


def _effective_field(row: Any, field_name: str) -> str | None:
    edit_value = row[f"{field_name}_edit"] if f"{field_name}_edit" in row.keys() else None
    if edit_value is not None and str(edit_value).strip():
        return str(edit_value)
    raw_value = row[f"{field_name}_raw"] if f"{field_name}_raw" in row.keys() else None
    if raw_value is None:
        return None
    return str(raw_value)


def record_entry_edit(
    store: SQLiteStore,
    corpus_id: str,
    entry_id: str,
    field_changes: dict[str, Any],
    note: str | None = None,
) -> int:
    row = store.entry_by_id(entry_id)
    if row is None:
        raise OverrideError(f"Unknown entry_id: {entry_id}")
    if str(row["dict_id"]) != corpus_id:
        raise OverrideError("Entry does not belong to target corpus")

    allowed = {"headword_edit", "pron_edit", "definition_edit", "status"}
    cleaned: dict[str, Any] = {}
    for key, value in field_changes.items():
        if key not in allowed:
            continue
        if key == "status":
            status = str(value or "").strip() or "auto"
            if status not in {"auto", "reviewed", "validated"}:
                raise OverrideError(f"Invalid status: {status}")
            cleaned[key] = status
        else:
            text = str(value or "").strip()
            cleaned[key] = text or None

    if not cleaned:
        raise OverrideError("No editable fields in field_changes")

    before = {
        "headword_edit": row["headword_edit"],
        "pron_edit": row["pron_edit"],
        "definition_edit": row["definition_edit"],
        "status": row["status"],
    }

    store.update_entry_edit_fields(
        entry_id=entry_id,
        dict_id=corpus_id,
        field_changes=cleaned,
    )

    return store.insert_override(
        corpus_id=corpus_id,
        scope="entry",
        source_id=str(row["source_id"] or row["source_path"]),
        record_key=str(row["record_key"] or ""),
        entry_id=entry_id,
        op="EDIT_ENTRY",
        before_json=before,
        after_json=cleaned,
        note=note,
    )


def create_entry(
    store: SQLiteStore,
    corpus_id: str,
    headword_raw: str,
    pron_raw: str | None = None,
    definition_raw: str | None = None,
    source_id: str | None = None,
    record_key: str | None = None,
    note: str | None = None,
    *,
    entry_is_pron: bool = False,
    source_path: str | None = None,
    source_record: str | None = None,
    line_no: int | None = None,
    status: str = "reviewed",
) -> str:
    cleaned_headword = str(headword_raw or "").strip()
    if not cleaned_headword:
        raise OverrideError("headword_raw is required")

    cleaned_pron = str(pron_raw or "").strip()
    if not cleaned_pron and entry_is_pron:
        cleaned_pron = cleaned_headword

    cleaned_definition = str(definition_raw or "").strip()
    source_id_value = str(source_id or "").strip() or None
    source_path_value = str(source_path or "").strip() or source_id_value
    source_record_value = str(source_record or "").strip() or cleaned_headword

    entry_id = store.insert_entry(
        dict_id=corpus_id,
        headword_raw=cleaned_headword,
        pos_raw="p",
        pron_raw=cleaned_pron or None,
        definition_raw=cleaned_definition or None,
        source_id=source_id_value,
        record_key=record_key,
        source_path=source_path_value,
        source_record=source_record_value,
        line_no=line_no,
        status=status,
        manual_created=True,
        section="",
        syllables=1,
    )

    after_payload = {
        "entry_id": entry_id,
        "headword_raw": cleaned_headword,
        "pron_raw": cleaned_pron or None,
        "definition_raw": cleaned_definition or None,
        "source_id": source_id_value,
        "record_key": record_key,
        "status": status,
        "manual_created": True,
    }
    store.insert_override(
        corpus_id=corpus_id,
        scope="entry",
        source_id=source_id_value,
        record_key=record_key,
        entry_id=entry_id,
        op="CREATE_ENTRY",
        before_json=None,
        after_json=after_payload,
        note=note,
    )
    return entry_id


def create_entry_from_record(
    store: SQLiteStore,
    corpus_id: str,
    source_id: str,
    record_key: str,
    headword_raw: str,
    pron_raw: str | None = None,
    definition_raw: str | None = None,
    note: str | None = None,
    *,
    entry_is_pron: bool = False,
    source_path: str | None = None,
    source_record: str | None = None,
    line_no: int | None = None,
    status: str = "reviewed",
) -> str:
    cleaned_source_id = str(source_id or "").strip()
    cleaned_record_key = str(record_key or "").strip()
    if not cleaned_source_id or not cleaned_record_key:
        raise OverrideError("source_id and record_key are required for record-level creation")

    return create_entry(
        store=store,
        corpus_id=corpus_id,
        headword_raw=headword_raw,
        pron_raw=pron_raw,
        definition_raw=definition_raw,
        source_id=cleaned_source_id,
        record_key=cleaned_record_key,
        note=note,
        entry_is_pron=entry_is_pron,
        source_path=source_path,
        source_record=source_record,
        line_no=line_no,
        status=status,
    )


def soft_delete_entries(
    store: SQLiteStore,
    corpus_id: str,
    entry_ids: list[str],
    reason: str | None = None,
    note: str | None = None,
) -> int:
    cleaned_ids: list[str] = []
    seen: set[str] = set()
    for entry_id in entry_ids:
        cleaned = str(entry_id or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        cleaned_ids.append(cleaned)
    if not cleaned_ids:
        return 0

    rows_to_delete: list[Any] = []
    for entry_id in cleaned_ids:
        row = store.entry_by_id(entry_id)
        if row is None:
            continue
        if str(row["dict_id"]) != corpus_id:
            raise OverrideError("Entry does not belong to target corpus")
        if bool(int(row["is_deleted"] or 0)):
            continue
        rows_to_delete.append(row)

    if not rows_to_delete:
        return 0

    deleted_at = utc_now_iso()
    deleted_reason = str(reason or "").strip() or None
    store.update_entries_delete_state(
        dict_id=corpus_id,
        entry_ids=[str(row["entry_id"]) for row in rows_to_delete],
        is_deleted=True,
        deleted_at=deleted_at,
        deleted_reason=deleted_reason,
    )

    for row in rows_to_delete:
        before_json = {
            "headword_raw": row["headword_raw"],
            "pron_raw": row["pron_raw"],
            "status": row["status"],
            "is_deleted": int(row["is_deleted"] or 0),
            "deleted_at": row["deleted_at"],
            "deleted_reason": row["deleted_reason"],
        }
        after_json = {
            "is_deleted": 1,
            "deleted_at": deleted_at,
            "deleted_reason": deleted_reason,
        }
        store.insert_override(
            corpus_id=corpus_id,
            scope="entry",
            source_id=str(row["source_id"] or row["source_path"]),
            record_key=str(row["record_key"] or ""),
            entry_id=str(row["entry_id"]),
            op="DELETE_ENTRY",
            before_json=before_json,
            after_json=after_json,
            note=note,
        )
    return len(rows_to_delete)


def restore_entries(
    store: SQLiteStore,
    corpus_id: str,
    entry_ids: list[str],
    note: str | None = None,
) -> int:
    cleaned_ids: list[str] = []
    seen: set[str] = set()
    for entry_id in entry_ids:
        cleaned = str(entry_id or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        cleaned_ids.append(cleaned)
    if not cleaned_ids:
        return 0

    rows_to_restore: list[Any] = []
    for entry_id in cleaned_ids:
        row = store.entry_by_id(entry_id)
        if row is None:
            continue
        if str(row["dict_id"]) != corpus_id:
            raise OverrideError("Entry does not belong to target corpus")
        if not bool(int(row["is_deleted"] or 0)):
            continue
        rows_to_restore.append(row)

    if not rows_to_restore:
        return 0

    store.update_entries_delete_state(
        dict_id=corpus_id,
        entry_ids=[str(row["entry_id"]) for row in rows_to_restore],
        is_deleted=False,
        deleted_at=None,
        deleted_reason=None,
    )

    for row in rows_to_restore:
        before_json = {
            "headword_raw": row["headword_raw"],
            "pron_raw": row["pron_raw"],
            "status": row["status"],
            "is_deleted": int(row["is_deleted"] or 0),
            "deleted_at": row["deleted_at"],
            "deleted_reason": row["deleted_reason"],
        }
        after_json = {
            "is_deleted": 0,
            "deleted_at": None,
            "deleted_reason": None,
        }
        store.insert_override(
            corpus_id=corpus_id,
            scope="entry",
            source_id=str(row["source_id"] or row["source_path"]),
            record_key=str(row["record_key"] or ""),
            entry_id=str(row["entry_id"]),
            op="RESTORE_ENTRY",
            before_json=before_json,
            after_json=after_json,
            note=note,
        )
    return len(rows_to_restore)


def fill_pron_raw_from_headword(
    store: SQLiteStore,
    corpus_id: str,
    entry_ids: list[str] | None = None,
    note: str | None = None,
) -> dict[str, int]:
    rows: list[Any] = []
    if entry_ids:
        seen: set[str] = set()
        for entry_id in entry_ids:
            cleaned_id = str(entry_id or "").strip()
            if not cleaned_id or cleaned_id in seen:
                continue
            seen.add(cleaned_id)
            row = store.entry_by_id(cleaned_id)
            if row is not None:
                rows.append(row)
    else:
        rows = list(store.entries_for_dict(corpus_id))

    scanned = 0
    updated = 0
    skipped_non_empty = 0
    skipped_no_headword = 0
    skipped_wrong_corpus = 0

    for row in rows:
        if str(row["dict_id"]) != corpus_id:
            skipped_wrong_corpus += 1
            continue
        scanned += 1
        entry_id = str(row["entry_id"])
        pron_raw = str(row["pron_raw"] or "").strip()
        if pron_raw:
            skipped_non_empty += 1
            continue

        headword_effective = str(_effective_field(row, "headword") or "").strip()
        if not headword_effective:
            skipped_no_headword += 1
            continue

        store.update_entry_raw_fields(
            entry_id=entry_id,
            dict_id=corpus_id,
            field_changes={"pron_raw": headword_effective},
        )
        store.insert_override(
            corpus_id=corpus_id,
            scope="entry",
            source_id=str(row["source_id"] or row["source_path"]),
            record_key=str(row["record_key"] or ""),
            entry_id=entry_id,
            op="EDIT_ENTRY",
            before_json={"pron_raw": row["pron_raw"]},
            after_json={"pron_raw": headword_effective},
            note=note or "fill_pron_raw_from_headword",
        )
        updated += 1

    return {
        "scanned": scanned,
        "updated": updated,
        "skipped_non_empty": skipped_non_empty,
        "skipped_no_headword": skipped_no_headword,
        "skipped_wrong_corpus": skipped_wrong_corpus,
    }


def split_entry(
    store: SQLiteStore,
    corpus_id: str,
    entry_id: str,
    parts: list[str],
    note: str | None = None,
) -> list[str]:
    row = store.entry_by_id(entry_id)
    if row is None:
        raise OverrideError(f"Unknown entry_id: {entry_id}")
    if str(row["dict_id"]) != corpus_id:
        raise OverrideError("Entry does not belong to target corpus")

    cleaned_parts = [part.strip() for part in parts if part.strip()]
    if len(cleaned_parts) < 2:
        raise OverrideError("Split requires at least two non-empty parts")

    before = dict(row)
    source_record = str(row["source_record"] or row["headword_raw"])
    source_id = str(row["source_id"] or row["source_path"])
    record_key = str(row["record_key"] or "")

    entries: list[ParsedEntry] = []
    for idx, part in enumerate(cleaned_parts, start=1):
        split_source_path = f"{row['source_path']}#split:{entry_id}:{idx}"
        entries.append(
            ParsedEntry(
                dict_id=corpus_id,
                section=str(row["section"] or ""),
                syllables=int(row["syllables"]),
                headword_raw=part,
                pos_raw=str(row["pos_raw"]),
                pron_raw=_effective_field(row, "pron"),
                source_path=split_source_path,
                line_no=int(row["line_no"]),
                raw_line=source_record,
                origin_raw=(str(row["origin_raw"]) if row["origin_raw"] else None),
                origin_norm=(str(row["origin_norm"]) if row["origin_norm"] else None),
                pos_norm=(str(row["pos_norm"]) if row["pos_norm"] else None),
                parser_id=(str(row["parser_id"]) if row["parser_id"] else None),
                parser_version=(int(row["parser_version"]) if row["parser_version"] else None),
                parser_sha256=(str(row["parser_sha256"]) if row["parser_sha256"] else None),
                definition_raw=_effective_field(row, "definition"),
                source_record=source_record,
                template_id=(str(row["template_id"]) if row["template_id"] else None),
                template_version=(
                    int(row["template_version"]) if row["template_version"] else None
                ),
                template_sha256=(str(row["template_sha256"]) if row["template_sha256"] else None),
                source_id=source_id,
                record_key=record_key,
            )
        )

    new_ids = [entry_id_for(item) for item in entries]
    store.delete_entries([entry_id])
    store.insert_entries(entries)

    before_json = {"entry": before}
    after_json = {"parts": cleaned_parts, "new_entry_ids": new_ids}
    for target_entry_id in [entry_id, *new_ids]:
        store.insert_override(
            corpus_id=corpus_id,
            scope="entry",
            source_id=source_id,
            record_key=record_key,
            entry_id=target_entry_id,
            op="SPLIT_ENTRY",
            before_json=before_json,
            after_json=after_json,
            note=note,
        )
    return new_ids


def merge_entries(
    store: SQLiteStore,
    corpus_id: str,
    entry_id_a: str,
    entry_id_b: str,
    note: str | None = None,
) -> str:
    row_a = store.entry_by_id(entry_id_a)
    row_b = store.entry_by_id(entry_id_b)
    if row_a is None or row_b is None:
        raise OverrideError("One or both entries do not exist")
    if str(row_a["dict_id"]) != corpus_id or str(row_b["dict_id"]) != corpus_id:
        raise OverrideError("Entries must belong to target corpus")

    headword = " ".join(
        [
            str(_effective_field(row_a, "headword") or "").strip(),
            str(_effective_field(row_b, "headword") or "").strip(),
        ]
    ).strip()
    pron = " ".join(
        [
            str(_effective_field(row_a, "pron") or "").strip(),
            str(_effective_field(row_b, "pron") or "").strip(),
        ]
    ).strip()
    definition = " / ".join(
        [
            str(_effective_field(row_a, "definition") or "").strip(),
            str(_effective_field(row_b, "definition") or "").strip(),
        ]
    ).strip()

    merged = ParsedEntry(
        dict_id=corpus_id,
        section=str(row_a["section"] or row_b["section"] or ""),
        syllables=int(row_a["syllables"]),
        headword_raw=headword,
        pos_raw=str(row_a["pos_raw"]),
        pron_raw=pron or None,
        source_path=f"{row_a['source_path']}+{row_b['source_path']}#merge",
        line_no=min(int(row_a["line_no"]), int(row_b["line_no"])),
        raw_line=f"{headword}, {row_a['pos_raw']}",
        origin_raw=(str(row_a["origin_raw"]) if row_a["origin_raw"] else None),
        origin_norm=(str(row_a["origin_norm"]) if row_a["origin_norm"] else None),
        pos_norm=(str(row_a["pos_norm"]) if row_a["pos_norm"] else None),
        parser_id=(str(row_a["parser_id"]) if row_a["parser_id"] else None),
        parser_version=(int(row_a["parser_version"]) if row_a["parser_version"] else None),
        parser_sha256=(str(row_a["parser_sha256"]) if row_a["parser_sha256"] else None),
        definition_raw=definition or None,
        source_record=str(row_a["source_record"] or row_a["headword_raw"]),
        template_id=(str(row_a["template_id"]) if row_a["template_id"] else None),
        template_version=(int(row_a["template_version"]) if row_a["template_version"] else None),
        template_sha256=(str(row_a["template_sha256"]) if row_a["template_sha256"] else None),
        source_id=str(row_a["source_id"] or row_a["source_path"]),
        record_key=str(row_a["record_key"] or ""),
    )

    merged_id = entry_id_for(merged)
    store.delete_entries([entry_id_a, entry_id_b])
    store.insert_entries([merged])

    store.insert_override(
        corpus_id=corpus_id,
        scope="entry",
        source_id=str(row_a["source_id"] or row_a["source_path"]),
        record_key=str(row_a["record_key"] or ""),
        entry_id=merged_id,
        op="MERGE_ENTRY",
        before_json={"entries": [dict(row_a), dict(row_b)]},
        after_json={"entry_id": merged_id},
        note=note,
    )
    return merged_id


def set_entry_status(
    store: SQLiteStore,
    corpus_id: str,
    entry_id: str,
    status: str,
    note: str | None = None,
) -> int:
    cleaned = status.strip().lower()
    if cleaned not in {"auto", "reviewed", "validated"}:
        raise OverrideError(f"Invalid entry status: {status}")

    row = store.entry_by_id(entry_id)
    if row is None:
        raise OverrideError(f"Unknown entry_id: {entry_id}")
    if str(row["dict_id"]) != corpus_id:
        raise OverrideError("Entry does not belong to target corpus")

    previous = str(row["status"] or "auto")
    store.update_entry_edit_fields(
        entry_id=entry_id,
        dict_id=corpus_id,
        field_changes={"status": cleaned},
    )

    op = "VALIDATE_ENTRY" if cleaned == "validated" else "REVIEW_ENTRY"
    return store.insert_override(
        corpus_id=corpus_id,
        scope="entry",
        source_id=str(row["source_id"] or row["source_path"]),
        record_key=str(row["record_key"] or ""),
        entry_id=entry_id,
        op=op,
        before_json={"status": previous},
        after_json={"status": cleaned},
        note=note,
    )
