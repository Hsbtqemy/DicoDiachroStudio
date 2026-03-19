from pathlib import Path

from dicodiachro.core.parsing import parse_lines


def test_parse_sample_input() -> None:
    lines = Path("sample_data/sample_input.txt").read_text(encoding="utf-8").splitlines()
    entries, issues = parse_lines(lines, dict_id="dict_a", source_path="sample_input.txt")

    assert len(entries) == 3
    assert any(issue.code == "PAGE_MARKER" for issue in issues)
    assert any(issue.code == "UNPARSED_LINE" for issue in issues)

    ki_entries = [entry for entry in entries if entry.section == "KI"]
    assert ki_entries, "Expected section ΚΙ to normalize to KI"


def test_parse_lines_with_line_pages() -> None:
    lines = ["A", "1 foo, v", "2 bar, v"]
    line_pages = [1, 1, 2]
    entries, _ = parse_lines(
        lines,
        dict_id="d",
        source_path="x.txt",
        line_pages=line_pages,
    )
    assert len(entries) == 2
    assert entries[0].headword_raw == "foo"
    assert entries[0].page == 1
    assert entries[1].headword_raw == "bar"
    assert entries[1].page == 2
