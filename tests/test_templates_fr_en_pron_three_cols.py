from __future__ import annotations

from dicodiachro.core.templates.engine import apply_template_to_records
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind


def test_fr_en_pron_three_cols_triple_spaces() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk1",
            record_no=1,
            source_type="text",
            raw_text="Abcéder , v. a.   To impose.   Toû ïmmpâss.",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        params={"separator_mode": "triple_spaces"},
        records=records,
    )

    assert result.entries_count == 1
    entry = result.entries[0]
    assert entry.headword_raw == "Abcéder"
    assert entry.pos_raw == "v. a."
    assert entry.definition_raw == "To impose."
    assert entry.pron_raw == "Toû ïmmpâss."


def test_fr_en_pron_three_cols_tab_separator() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk2",
            record_no=2,
            source_type="text",
            raw_text="Abcès , f. m.\tAbscess.\tAïbbcess.",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        params={"separator_mode": "tab"},
        records=records,
    )

    assert result.entries_count == 1
    entry = result.entries[0]
    assert entry.headword_raw == "Abcès"
    assert entry.pos_raw == "f. m."
    assert entry.definition_raw == "Abscess."
    assert entry.pron_raw == "Aïbbcess."


def test_fr_en_pron_three_cols_unrecognized_without_three_columns() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk3",
            record_no=3,
            source_type="text",
            raw_text="Ligne sans trois colonnes",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        params={"separator_mode": "triple_spaces"},
        records=records,
    )

    assert result.entries_count == 0
    assert result.unrecognized_count == 1
    assert result.issues_by_code.get("UNRECOGNIZED_RECORD") == 1


def test_fr_en_pron_three_cols_auto_fallback_separator() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk4",
            record_no=4,
            source_type="text",
            raw_text="Abhorrer , v. a.\tTo detest.\tTotí ditësst.",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        params={"separator_mode": "auto"},
        records=records,
    )

    assert result.entries_count == 1
    entry = result.entries[0]
    assert entry.headword_raw == "Abhorrer"
    assert entry.pos_raw == "v. a."
    assert entry.definition_raw == "To detest."
    assert entry.pron_raw == "Totí ditësst."


def test_fr_en_pron_three_cols_extracts_pos_without_comma() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk5",
            record_no=5,
            source_type="text",
            raw_text="Abdiquer v. a.   To give over.   Toû guïv' ôre.",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.FR_EN_PRON_THREE_COLS,
        params={"separator_mode": "triple_spaces"},
        records=records,
    )

    assert result.entries_count == 1
    entry = result.entries[0]
    assert entry.headword_raw == "Abdiquer"
    assert entry.pos_raw == "v. a."
