from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.qa import lint_lines, validate_entries


def test_qa_detects_missing_comma_invalid_pos_syllables() -> None:
    lines = [
        "JU",
        "2 no_comma v",
        "11 badword, v",
        "3 good, x",
    ]
    issues = lint_lines(lines, dict_id="d1", source_path="sample.txt")
    codes = {issue.code for issue in issues}

    assert "MISSING_COMMA" in codes
    assert "SYLLABLES_OUT_OF_RANGE" in codes
    assert "INVALID_POS" in codes


def test_validate_entries_checks_range() -> None:
    entry = ParsedEntry(
        dict_id="d1",
        section="JU",
        syllables=12,
        headword_raw="test",
        pos_raw="v",
        pron_raw="test",
        source_path="sample.txt",
        line_no=1,
        raw_line="12 test, v",
    )
    issues = validate_entries([entry])
    assert any(issue.code == "SYLLABLES_OUT_OF_RANGE" for issue in issues)
