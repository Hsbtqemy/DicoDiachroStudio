from __future__ import annotations

from pathlib import Path

from dicodiachro.core.pipeline import run_pipeline
from dicodiachro.core.storage.sqlite import SQLiteStore, project_paths


def test_run_pipeline_excludes_paratext_with_source_filters(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    source_path = project_dir / "data" / "raw" / "imports" / "sample.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        "\n".join(
            [
                "PREFACE",
                "INTRODUCTION",
                "A",
                "1 alpha, v",
                "1 beta, v",
                "BIBLIOGRAPHY",
                "1 gamma, v",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    filters_path = project_dir / "rules" / "source_filters" / "source_filters.yml"
    filters_path.parent.mkdir(parents=True, exist_ok=True)
    filters_path.write_text(
        "\n".join(
            [
                "enabled: true",
                "default:",
                "  drop_before_regex: '^1 alpha, v$'",
                "  drop_after_regex: '^BIBLIOGRAPHY$'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = run_pipeline(
        project_dir=project_dir,
        dict_id="corpus_test",
        profile_name="reading_v1",
        source_paths=[source_path],
    )

    assert summary["entries"] == 2
    assert summary["source_filters"]["dropped_lines"] == 5

    store = SQLiteStore(project_paths(project_dir).db_path)
    rows = store.entries_for_dict("corpus_test")
    assert [str(row["headword_raw"]) for row in rows] == ["alpha", "beta"]
