from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

from ..overrides import compute_record_key
from ..source_filters import SourceFilterConfig, apply_source_filters
from .csv_mapping import split_cell
from .spec import (
    EntryDraft,
    PreviewRow,
    SourceRecord,
    TemplateApplyResult,
    TemplateKind,
    TemplatePreviewResult,
)

PUNCT_ARTIFACTS = {".", "..", "...", "*", "-", "—", "•"}
MULTI_SPACE_RE = re.compile(r"\s{2,}")


class TemplateEngineError(RuntimeError):
    pass


def _compact_csv_row(row: dict[str, str], max_items: int = 6) -> str:
    items = list(row.items())[:max_items]
    compact = " | ".join(f"{key}={value}" for key, value in items)
    return compact.strip()


def load_source_records(
    source_path: Path,
    limit: int | None = None,
    source_filter_config: SourceFilterConfig | None = None,
    return_filter_report: bool = False,
) -> list[SourceRecord] | tuple[list[SourceRecord], dict[str, Any] | None]:
    path = source_path.expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise TemplateEngineError(f"Source introuvable: {path}")

    source_id = str(path)
    records: list[SourceRecord] = []
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for idx, row in enumerate(reader, start=2):
                normalized = {str(key): str(value or "") for key, value in (row or {}).items()}
                compact = _compact_csv_row(normalized)
                records.append(
                    SourceRecord(
                        source_id=source_id,
                        source_path=str(path),
                        record_key=compute_record_key(source_id, compact, idx),
                        record_no=idx,
                        source_type="csv",
                        raw_text=compact,
                        csv_row=normalized,
                    )
                )
                if limit is not None and len(records) >= limit:
                    break
        if return_filter_report:
            return records, None
        return records

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    filtered = apply_source_filters(lines, source_path=path, config=source_filter_config)
    dropped = set(filtered.dropped_line_numbers)
    for idx, raw in enumerate(filtered.lines, start=1):
        if idx in dropped:
            continue
        records.append(
            SourceRecord(
                source_id=source_id,
                source_path=str(path),
                record_key=compute_record_key(source_id, raw, idx),
                record_no=idx,
                source_type="text",
                raw_text=raw,
            )
        )
        if limit is not None and len(records) >= limit:
            break
    if return_filter_report:
        return records, filtered.report
    return records


def _is_punct_only(token: str) -> bool:
    if not token:
        return True
    return not any(ch.isalnum() for ch in token)


def _strip_terminal_punctuation(text: str) -> str:
    return text.rstrip(".,;:!?/\\|")


def _find_separator(text: str, mode: str, custom_separator: str = "") -> tuple[str, str] | None:
    if mode == "comma":
        if "," not in text:
            return None
        left, right = text.split(",", 1)
        return left, right

    if mode == "semicolon":
        if ";" not in text:
            return None
        left, right = text.split(";", 1)
        return left, right

    if mode == "double_space":
        match = MULTI_SPACE_RE.search(text)
        if not match:
            return None
        split_at = match.start()
        return text[:split_at], text[match.end() :]

    if mode == "tab":
        if "\t" not in text:
            return None
        left, right = text.split("\t", 1)
        return left, right

    if mode == "custom":
        if not custom_separator or custom_separator not in text:
            return None
        left, right = text.split(custom_separator, 1)
        return left, right

    return None


def _apply_wordlist(
    record: SourceRecord,
    params: dict[str, Any],
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    trim_punctuation = bool(params.get("trim_token_punctuation", False))
    pron_from_headword = bool(params.get("pron_from_headword", False))
    tokens = record.raw_text.split()
    drafts: list[EntryDraft] = []
    rows: list[PreviewRow] = []

    for token in tokens:
        original = token.strip()
        if not original:
            continue

        if original in PUNCT_ARTIFACTS or _is_punct_only(original):
            rows.append(
                PreviewRow(
                    source=record.raw_text,
                    headword_raw="",
                    pron_raw="",
                    definition_raw="",
                    source_id=record.source_id,
                    record_key=record.record_key,
                    status="Ignoré",
                    reason="ponctuation",
                    source_path=record.source_path,
                    record_no=record.record_no,
                    issue_code="PUNCT_ONLY_TOKEN",
                )
            )
            continue

        headword = _strip_terminal_punctuation(original) if trim_punctuation else original
        if not headword:
            rows.append(
                PreviewRow(
                    source=record.raw_text,
                    headword_raw="",
                    pron_raw="",
                    definition_raw="",
                    source_id=record.source_id,
                    record_key=record.record_key,
                    status="Ignoré",
                    reason="vide",
                    source_path=record.source_path,
                    record_no=record.record_no,
                    issue_code="EMPTY_HEADWORD",
                )
            )
            continue

        drafts.append(
            EntryDraft(
                headword_raw=headword,
                pron_raw=headword if pron_from_headword else None,
                definition_raw=None,
                source_id=record.source_id,
                record_key=record.record_key,
                source_path=record.source_path,
                record_no=record.record_no,
                source_record=record.raw_text,
            )
        )
        rows.append(
            PreviewRow(
                source=record.raw_text,
                headword_raw=headword,
                pron_raw=headword if pron_from_headword else "",
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="OK",
                reason="",
                source_path=record.source_path,
                record_no=record.record_no,
            )
        )

    if not tokens:
        rows.append(
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw="",
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="Ignoré",
                reason="vide",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="EMPTY_HEADWORD",
            )
        )

    return drafts, rows


def _apply_entry_plus_definition(
    record: SourceRecord,
    params: dict[str, Any],
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    separator_mode = str(params.get("separator_mode", "comma"))
    custom_separator = str(params.get("custom_separator", ""))
    split = _find_separator(record.raw_text, separator_mode, custom_separator)
    if split is None:
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw="",
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="Non reconnu",
                reason="pas de séparateur",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="UNRECOGNIZED_RECORD",
            )
        ]

    left, right = split
    headword = left.strip()
    definition = right.strip()
    if not headword:
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw="",
                definition_raw=definition,
                source_id=record.source_id,
                record_key=record.record_key,
                status="Non reconnu",
                reason="headword vide",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="EMPTY_HEADWORD",
            )
        ]

    draft = EntryDraft(
        headword_raw=headword,
        pron_raw=None,
        definition_raw=definition or None,
        source_id=record.source_id,
        record_key=record.record_key,
        source_path=record.source_path,
        record_no=record.record_no,
        source_record=record.raw_text,
    )
    row = PreviewRow(
        source=record.raw_text,
        headword_raw=headword,
        pron_raw="",
        definition_raw=definition,
        source_id=record.source_id,
        record_key=record.record_key,
        status="OK",
        reason="",
        source_path=record.source_path,
        record_no=record.record_no,
    )
    return [draft], [row]


def _apply_headword_plus_pron(
    record: SourceRecord,
    params: dict[str, Any],
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    separator_mode = str(params.get("separator_mode", "tab"))
    custom_separator = str(params.get("custom_separator", ""))
    trim_punctuation = bool(params.get("trim_punctuation", False))

    split = _find_separator(record.raw_text, separator_mode, custom_separator)
    if split is None and separator_mode in {"tab", "multi_spaces"}:
        if separator_mode == "multi_spaces":
            split = _find_separator(record.raw_text, "double_space")
    if split is None:
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw="",
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="Non reconnu",
                reason="pas de séparateur",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="UNRECOGNIZED_RECORD",
            )
        ]

    left, right = split
    headword = left.strip()
    pron = right.strip()
    if trim_punctuation:
        headword = _strip_terminal_punctuation(headword)
        pron = _strip_terminal_punctuation(pron)

    if not headword:
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw=pron,
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="Non reconnu",
                reason="headword vide",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="EMPTY_HEADWORD",
            )
        ]

    draft = EntryDraft(
        headword_raw=headword,
        pron_raw=pron or None,
        definition_raw=None,
        source_id=record.source_id,
        record_key=record.record_key,
        source_path=record.source_path,
        record_no=record.record_no,
        source_record=record.raw_text,
    )
    row = PreviewRow(
        source=record.raw_text,
        headword_raw=headword,
        pron_raw=pron,
        definition_raw="",
        source_id=record.source_id,
        record_key=record.record_key,
        status="OK",
        reason="",
        source_path=record.source_path,
        record_no=record.record_no,
    )
    return [draft], [row]


def _apply_csv_mapping(
    record: SourceRecord,
    params: dict[str, Any],
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    if not record.csv_row:
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw="",
                definition_raw="",
                source_id=record.source_id,
                record_key=record.record_key,
                status="Non reconnu",
                reason="row CSV invalide",
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code="UNRECOGNIZED_RECORD",
            )
        ]

    headword_col = str(params.get("headword_column", "")).strip()
    pron_col = str(params.get("pron_column", "")).strip()
    definition_col = str(params.get("definition_column", "")).strip()
    split_mode = str(params.get("split_headword", "none"))
    ignore_empty = bool(params.get("ignore_empty_headword", True))

    if not headword_col:
        raise TemplateEngineError("CSV_MAPPING requiert un paramètre 'headword_column'.")

    headword_cell = str(record.csv_row.get(headword_col, "") or "")
    values = split_cell(headword_cell, split_mode)
    pron = str(record.csv_row.get(pron_col, "") or "") if pron_col else ""
    definition = str(record.csv_row.get(definition_col, "") or "") if definition_col else ""

    if not values:
        status = "Ignoré" if ignore_empty else "Non reconnu"
        reason = "headword vide"
        code = "EMPTY_HEADWORD"
        return [], [
            PreviewRow(
                source=record.raw_text,
                headword_raw="",
                pron_raw=pron,
                definition_raw=definition,
                source_id=record.source_id,
                record_key=record.record_key,
                status=status,
                reason=reason,
                source_path=record.source_path,
                record_no=record.record_no,
                issue_code=code,
            )
        ]

    drafts: list[EntryDraft] = []
    rows: list[PreviewRow] = []
    for value in values:
        headword = value.strip()
        if not headword:
            continue
        drafts.append(
            EntryDraft(
                headword_raw=headword,
                pron_raw=pron or None,
                definition_raw=definition or None,
                source_id=record.source_id,
                record_key=record.record_key,
                source_path=record.source_path,
                record_no=record.record_no,
                source_record=record.raw_text,
            )
        )
        rows.append(
            PreviewRow(
                source=record.raw_text,
                headword_raw=headword,
                pron_raw=pron,
                definition_raw=definition,
                source_id=record.source_id,
                record_key=record.record_key,
                status="OK",
                reason="",
                source_path=record.source_path,
                record_no=record.record_no,
            )
        )
    return drafts, rows


def _apply_record(
    kind: TemplateKind,
    params: dict[str, Any],
    record: SourceRecord,
) -> tuple[list[EntryDraft], list[PreviewRow]]:
    if kind == TemplateKind.WORDLIST_TOKENS:
        return _apply_wordlist(record, params)
    if kind == TemplateKind.ENTRY_PLUS_DEFINITION:
        return _apply_entry_plus_definition(record, params)
    if kind == TemplateKind.HEADWORD_PLUS_PRON:
        return _apply_headword_plus_pron(record, params)
    if kind == TemplateKind.CSV_MAPPING:
        return _apply_csv_mapping(record, params)
    raise TemplateEngineError(f"Gabarit non supporté: {kind}")


def apply_template_to_records(
    kind: TemplateKind | str,
    params: dict[str, Any],
    records: list[SourceRecord],
) -> TemplateApplyResult:
    template_kind = TemplateKind(kind)
    entries: list[EntryDraft] = []
    preview_rows: list[PreviewRow] = []
    issues_by_code: dict[str, int] = {}

    for record in records:
        drafts, rows = _apply_record(template_kind, params, record)
        entries.extend(drafts)
        preview_rows.extend(rows)
        for row in rows:
            if row.issue_code:
                issues_by_code[row.issue_code] = issues_by_code.get(row.issue_code, 0) + 1

    ignored = sum(1 for row in preview_rows if row.status == "Ignoré")
    unrecognized = sum(1 for row in preview_rows if row.status == "Non reconnu")
    return TemplateApplyResult(
        entries=entries,
        preview_rows=preview_rows,
        records_count=len(records),
        entries_count=len(entries),
        ignored_count=ignored,
        unrecognized_count=unrecognized,
        issues_by_code=issues_by_code,
    )


def preview_template(
    kind: TemplateKind | str,
    params: dict[str, Any],
    records: list[SourceRecord],
) -> TemplatePreviewResult:
    applied = apply_template_to_records(kind=kind, params=params, records=records)
    return TemplatePreviewResult(
        rows=applied.preview_rows,
        records_count=applied.records_count,
        entries_count=applied.entries_count,
        ignored_count=applied.ignored_count,
        unrecognized_count=applied.unrecognized_count,
        issues_by_code=applied.issues_by_code,
    )
