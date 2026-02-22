from __future__ import annotations

from dicodiachro.core.templates.engine import apply_template_to_records
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind


def test_headword_plus_pron_tab_separator() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk1",
            record_no=1,
            source_type="text",
            raw_text="jut\tʒyt",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.HEADWORD_PLUS_PRON,
        params={"separator_mode": "tab"},
        records=records,
    )

    assert len(result.entries) == 1
    assert result.entries[0].headword_raw == "jut"
    assert result.entries[0].pron_raw == "ʒyt"


def test_headword_plus_pron_multi_spaces_separator() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk2",
            record_no=2,
            source_type="text",
            raw_text="káw   káːw",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.HEADWORD_PLUS_PRON,
        params={"separator_mode": "multi_spaces"},
        records=records,
    )

    assert len(result.entries) == 1
    assert result.entries[0].headword_raw == "káw"
    assert result.entries[0].pron_raw == "káːw"
