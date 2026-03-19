from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


@dataclass(slots=True)
class ParsedEntry:
    dict_id: str
    section: str
    syllables: int
    headword_raw: str
    pos_raw: str
    pron_raw: str | None
    source_path: str
    line_no: int
    raw_line: str
    page: int | None = None
    origin_raw: str | None = None
    origin_norm: str | None = None
    pos_norm: str | None = None
    parser_id: str | None = None
    parser_version: int | None = None
    parser_sha256: str | None = None
    definition_raw: str | None = None
    source_record: str | None = None
    template_id: str | None = None
    template_version: int | None = None
    template_sha256: str | None = None
    source_id: str | None = None
    record_key: str | None = None


@dataclass(slots=True)
class ProfileSpec:
    profile_id: str
    version: int
    dict_id: str | None = None
    name: str = ""
    description: str = ""
    unicode_normalization: str = "NFC"
    display: dict[str, Any] = field(default_factory=dict)
    norm: dict[str, Any] = field(default_factory=dict)
    render: dict[str, Any] = field(default_factory=dict)
    features: dict[str, Any] = field(default_factory=dict)
    qa: dict[str, Any] = field(default_factory=dict)
    validation_warnings: list[str] = field(default_factory=list)
    source_path: str | None = None


@dataclass(slots=True)
class ProfileApplied:
    form_display: str
    form_norm: str
    form_render: str = ""
    features: dict[str, Any] = field(default_factory=dict)
    symbols_used: list[str] = field(default_factory=list)
    unknown_symbols: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Issue:
    dict_id: str
    source_path: str
    line_no: int
    kind: str
    code: str
    raw: str
    details: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class ProjectPaths:
    root: Path
    db_path: Path
    raw_dir: Path
    interim_dir: Path
    derived_dir: Path
    rules_dir: Path
    logs_dir: Path


@dataclass(slots=True)
class MatchCandidate:
    entry_id_a: str
    entry_id_b: str
    dict_id_a: str
    dict_id_b: str
    label_a: str
    label_b: str
    score: float
    status: str


@dataclass(slots=True)
class CompareRow:
    lemma_group_id: str
    lemma_label: str
    values: dict[str, str]


KNOWN_POS = {"a", "v", "p", "ſ", "s"}
