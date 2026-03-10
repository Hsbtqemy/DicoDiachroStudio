from __future__ import annotations

from pathlib import Path

from dicodiachro.core.source_filters import (
    apply_source_filters,
    load_project_source_filters,
    load_source_filter_config,
)


def test_source_filters_drop_ranges_and_regexes(tmp_path: Path) -> None:
    config_path = tmp_path / "rules" / "source_filters" / "source_filters.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "enabled: true",
                "default:",
                "  exclude_line_ranges:",
                "    - 1-2",
                "  drop_after_regex: '^BIBLIOGRAPHY$'",
                "  drop_line_regexes:",
                "    - '^PREFACE$'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_source_filter_config(config_path, project_root=tmp_path)
    source_path = tmp_path / "data" / "raw" / "imports" / "sample.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("", encoding="utf-8")

    lines = ["PREFACE", "INTRO", "alpha", "beta", "BIBLIOGRAPHY", "gamma"]
    result = apply_source_filters(lines, source_path=source_path, config=config)

    assert result.lines == ["", "", "alpha", "beta", "", ""]
    assert result.dropped_line_numbers == [1, 2, 5, 6]
    assert result.report["dropped_lines"] == 4
    assert result.report["kept_lines"] == 2
    assert result.report["dropped_by_ranges"] == 2
    assert result.report["dropped_after_regex"] == 2


def test_source_filters_source_override_matches_basename(tmp_path: Path) -> None:
    config_path = tmp_path / "rules" / "source_filters" / "source_filters.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "enabled: true",
                "default:",
                "  exclude_line_ranges:",
                "    - 1",
                "sources:",
                "  'special*.txt':",
                "    exclude_line_ranges:",
                "      - 3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_source_filter_config(config_path, project_root=tmp_path)
    source_path = tmp_path / "data" / "raw" / "imports" / "special_1737.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("", encoding="utf-8")

    result = apply_source_filters(
        ["a", "b", "c", "d"],
        source_path=source_path,
        config=config,
    )
    assert result.dropped_line_numbers == [3]
    assert result.report["matched_source_patterns"] == ["special*.txt"]


def test_load_project_source_filters_prefers_dict_specific_file(tmp_path: Path) -> None:
    rules_dir = tmp_path / "rules"
    shared_path = rules_dir / "source_filters" / "source_filters.yml"
    dict_path = rules_dir / "dict_a" / "source_filters.yml"
    shared_path.parent.mkdir(parents=True, exist_ok=True)
    dict_path.parent.mkdir(parents=True, exist_ok=True)

    shared_path.write_text("default: {}\n", encoding="utf-8")
    dict_path.write_text("default: {}\n", encoding="utf-8")

    config_a = load_project_source_filters(rules_dir, dict_id="dict_a")
    config_b = load_project_source_filters(rules_dir, dict_id="dict_b")

    assert config_a is not None
    assert config_a.path == dict_path.resolve()
    assert config_b is not None
    assert config_b.path == shared_path.resolve()
