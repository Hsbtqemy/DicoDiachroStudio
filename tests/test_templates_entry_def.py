from __future__ import annotations

from dicodiachro.core.templates.engine import apply_template_to_records
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind


def test_entry_plus_definition_splits_at_comma() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk1",
            record_no=1,
            source_type="text",
            raw_text="jut, définition brève",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.ENTRY_PLUS_DEFINITION,
        params={"separator_mode": "comma"},
        records=records,
    )

    assert len(result.entries) == 1
    assert result.entries[0].headword_raw == "jut"
    assert result.entries[0].definition_raw == "définition brève"


def test_entry_plus_definition_unrecognized_without_separator() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk2",
            record_no=2,
            source_type="text",
            raw_text="ligne sans séparateur",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.ENTRY_PLUS_DEFINITION,
        params={"separator_mode": "comma"},
        records=records,
    )

    assert result.entries_count == 0
    assert result.unrecognized_count == 1
    assert result.issues_by_code.get("UNRECOGNIZED_RECORD") == 1
