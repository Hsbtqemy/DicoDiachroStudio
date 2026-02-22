from pathlib import Path

from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.profiles import apply_profile, load_profile
from dicodiachro.core.qa import lint_lines, validate_entries, validate_profile_applied


def _entry(raw_form: str, raw_line: str | None = None) -> ParsedEntry:
    return ParsedEntry(
        dict_id="d1",
        section="JU",
        syllables=2,
        headword_raw=raw_form,
        pos_raw="v",
        pron_raw=raw_form,
        source_path="sample.txt",
        line_no=1,
        raw_line=raw_line or f"2 {raw_form}, v",
    )


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


def test_missing_comma_not_triggered_for_valid_entries() -> None:
    entry = ParsedEntry(
        dict_id="d1",
        section="JU",
        syllables=1,
        headword_raw="jut",
        pos_raw="v",
        pron_raw="jut",
        source_path="sample.txt",
        line_no=1,
        raw_line="1 jut, v",
    )
    issues = validate_entries([entry])
    assert all(issue.code != "MISSING_COMMA" for issue in issues)

    lint_issues = lint_lines(["JU", "1 jut, v"], dict_id="d1", source_path="sample.txt")
    assert all(issue.code != "MISSING_COMMA" for issue in lint_issues)


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


def test_qa_unknown_symbol() -> None:
    profile = load_profile(Path("tests/data/profile_valid.yml"))
    entry = _entry("abc☃")
    applied = apply_profile(entry.pron_raw or entry.headword_raw, profile)

    issues = validate_profile_applied(
        entry=entry,
        entry_id="e1",
        profile=profile,
        applied=applied,
    )

    assert any(issue.code == "UNKNOWN_SYMBOL" for issue in issues)


def test_qa_detached_combining_mark() -> None:
    profile = load_profile(Path("tests/data/profile_valid.yml"))
    entry = _entry("a \u0304")
    applied = apply_profile(entry.pron_raw or entry.headword_raw, profile)

    issues = validate_profile_applied(
        entry=entry,
        entry_id="e1",
        profile=profile,
        applied=applied,
    )

    assert any(issue.code == "DETACHED_COMBINING_MARK" for issue in issues)


def test_inconsistent_stress_disabled_by_default() -> None:
    profile = load_profile(Path("rules/templates/analysis_quantity_v1.yml"))
    entry = _entry("júvenal")
    applied = apply_profile(entry.pron_raw or entry.headword_raw, profile)

    issues = validate_profile_applied(
        entry=entry,
        entry_id="e1",
        profile=profile,
        applied=applied,
    )

    assert all(issue.code != "INCONSISTENT_STRESS" for issue in issues)


def test_inconsistent_stress_enabled_when_configured() -> None:
    profile = load_profile(Path("tests/data/profile_qa_strict.yml"))
    entry = _entry("júvenal")
    applied = apply_profile(entry.pron_raw or entry.headword_raw, profile)

    issues = validate_profile_applied(
        entry=entry,
        entry_id="e1",
        profile=profile,
        applied=applied,
    )

    assert any(issue.code == "INCONSISTENT_STRESS" for issue in issues)
