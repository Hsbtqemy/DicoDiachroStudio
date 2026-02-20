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
