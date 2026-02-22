from __future__ import annotations

from dicodiachro.core.templates.csv_mapping import available_csv_columns
from dicodiachro.core.templates.engine import apply_template_to_records
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind


def test_csv_mapping_column_mapping_and_split() -> None:
    records = [
        SourceRecord(
            source_id="sample.csv",
            source_path="sample.csv",
            record_key="rk1",
            record_no=2,
            source_type="csv",
            raw_text="headword=jut;kut | pron=ʒyt | definition=entry",
            csv_row={"head": "jut; kut", "pron": "ʒyt", "def": "entry"},
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.CSV_MAPPING,
        params={
            "headword_column": "head",
            "pron_column": "pron",
            "definition_column": "def",
            "split_headword": "semicolon",
            "ignore_empty_headword": True,
        },
        records=records,
    )

    assert [entry.headword_raw for entry in result.entries] == ["jut", "kut"]
    assert all(entry.pron_raw == "ʒyt" for entry in result.entries)
    assert all(entry.definition_raw == "entry" for entry in result.entries)


def test_csv_mapping_available_columns() -> None:
    records = [
        SourceRecord(
            source_id="sample.csv",
            source_path="sample.csv",
            record_key="rk2",
            record_no=2,
            source_type="csv",
            raw_text="",
            csv_row={"head": "jut", "pron": "ʒyt", "def": "entry"},
        )
    ]

    assert available_csv_columns(records) == ["def", "head", "pron"]
