from __future__ import annotations

from dicodiachro.core.templates.engine import apply_template_to_records
from dicodiachro.core.templates.spec import SourceRecord, TemplateKind


def test_wordlist_tokens_splits_and_ignores_punctuation() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk1",
            record_no=1,
            source_type="text",
            raw_text="alpha ... — beta *",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.WORDLIST_TOKENS,
        params={"trim_token_punctuation": False},
        records=records,
    )

    assert [entry.headword_raw for entry in result.entries] == ["alpha", "beta"]
    assert result.issues_by_code.get("PUNCT_ONLY_TOKEN") == 3


def test_wordlist_tokens_trim_punctuation_option() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk2",
            record_no=2,
            source_type="text",
            raw_text="bóarder/",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.WORDLIST_TOKENS,
        params={"trim_token_punctuation": True},
        records=records,
    )

    assert len(result.entries) == 1
    assert result.entries[0].headword_raw == "bóarder"


def test_wordlist_tokens_pron_from_headword_populates_preview_and_entries() -> None:
    records = [
        SourceRecord(
            source_id="sample.txt",
            source_path="sample.txt",
            record_key="rk3",
            record_no=3,
            source_type="text",
            raw_text="alpha beta",
        )
    ]

    result = apply_template_to_records(
        kind=TemplateKind.WORDLIST_TOKENS,
        params={"pron_from_headword": True},
        records=records,
    )

    assert [entry.pron_raw for entry in result.entries] == ["alpha", "beta"]
    assert [row.pron_raw for row in result.preview_rows if row.status == "OK"] == ["alpha", "beta"]
