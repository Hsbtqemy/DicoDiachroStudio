from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TemplateKind(str, Enum):
    WORDLIST_TOKENS = "wordlist_tokens"
    ENTRY_PLUS_DEFINITION = "entry_plus_definition"
    HEADWORD_PLUS_PRON = "headword_plus_pron"
    FR_EN_PRON_THREE_COLS = "fr_en_pron_three_cols"
    CSV_MAPPING = "csv_mapping"


@dataclass(slots=True)
class TemplateSpec:
    template_id: str
    kind: TemplateKind
    version: int = 1
    params: dict[str, Any] = field(default_factory=dict)

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "kind": self.kind.value,
            "version": self.version,
            "params": self.params,
        }


@dataclass(slots=True)
class SourceRecord:
    source_id: str
    source_path: str
    record_key: str
    record_no: int
    source_type: str
    raw_text: str
    csv_row: dict[str, str] | None = None


@dataclass(slots=True)
class EntryDraft:
    headword_raw: str
    pron_raw: str | None
    definition_raw: str | None
    source_id: str
    record_key: str
    source_path: str
    record_no: int
    source_record: str
    pos_raw: str | None = None


@dataclass(slots=True)
class PreviewRow:
    source: str
    headword_raw: str
    pron_raw: str
    definition_raw: str
    source_id: str
    record_key: str
    status: str
    reason: str
    source_path: str
    record_no: int
    issue_code: str | None = None
    override_op: str | None = None


@dataclass(slots=True)
class TemplatePreviewResult:
    rows: list[PreviewRow]
    records_count: int
    entries_count: int
    ignored_count: int
    unrecognized_count: int
    issues_by_code: dict[str, int]


@dataclass(slots=True)
class TemplateApplyResult:
    entries: list[EntryDraft]
    preview_rows: list[PreviewRow]
    records_count: int
    entries_count: int
    ignored_count: int
    unrecognized_count: int
    issues_by_code: dict[str, int]


def template_sha256(spec: TemplateSpec) -> str:
    canonical = json.dumps(
        spec.canonical_payload(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    canonical = canonical.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
