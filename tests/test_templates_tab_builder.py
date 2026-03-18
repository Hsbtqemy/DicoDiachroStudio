from __future__ import annotations

from dicodiachro.core.templates.spec import SourceRecord, TemplateKind
from dicodiachro_studio.ui.tabs.templates_tab import TemplatesTab


def test_template_builder_recommends_fr_en_pron_three_cols() -> None:
    records = [
        SourceRecord(
            source_id="s1",
            source_path="s1",
            record_key="r1",
            record_no=1,
            source_type="text",
            raw_text="Abcéder , v. a.   To impose.   Toû ïmmpâss.",
        ),
        SourceRecord(
            source_id="s1",
            source_path="s1",
            record_key="r2",
            record_no=2,
            source_type="text",
            raw_text="Abcès , f. m.   Abscess.   Aïbbcess.",
        ),
    ]

    recommendation = TemplatesTab._recommend_template_for_records(records)

    assert recommendation is not None
    kind, params, metrics = recommendation
    assert kind == TemplateKind.FR_EN_PRON_THREE_COLS
    assert str(params.get("separator_mode")) == "auto"
    assert int(metrics.get("ok_records", 0)) == 2


def test_template_builder_recommends_headword_plus_pron_for_two_columns() -> None:
    records = [
        SourceRecord(
            source_id="s2",
            source_path="s2",
            record_key="r1",
            record_no=1,
            source_type="text",
            raw_text="abandon   əˈbændən",
        ),
        SourceRecord(
            source_id="s2",
            source_path="s2",
            record_key="r2",
            record_no=2,
            source_type="text",
            raw_text="abdomen   æbˈdoʊmən",
        ),
    ]

    recommendation = TemplatesTab._recommend_template_for_records(records)

    assert recommendation is not None
    kind, _, metrics = recommendation
    assert kind == TemplateKind.HEADWORD_PLUS_PRON
    assert int(metrics.get("ok_records", 0)) == 2
