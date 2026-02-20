from pathlib import Path

from dicodiachro.core.align.match import match_dictionaries
from dicodiachro.core.models import ParsedEntry
from dicodiachro.core.profiles import apply_profile, load_profile
from dicodiachro.core.storage.sqlite import SQLiteStore, entry_id_for, init_project, project_paths


def test_match_exact_and_fuzzy(tmp_path: Path) -> None:
    init_project(tmp_path)
    store = SQLiteStore(project_paths(tmp_path).db_path)
    store.ensure_dictionary("A")
    store.ensure_dictionary("B")

    profile = load_profile(Path("rules/templates/alignment_v1.yml"))

    entries_a = [
        ParsedEntry("A", "AA", 2, "alpha", "v", "alpha", "a.txt", 1, "2 alpha, v"),
        ParsedEntry("A", "AA", 2, "beta", "v", "beta", "a.txt", 2, "2 beta, v"),
    ]
    entries_b = [
        ParsedEntry("B", "AA", 2, "alpha", "v", "alpha", "b.txt", 1, "2 alpha, v"),
        ParsedEntry("B", "AA", 2, "betta", "v", "betta", "b.txt", 2, "2 betta, v"),
    ]

    applied = {}
    for entry in [*entries_a, *entries_b]:
        applied[entry_id_for(entry)] = apply_profile(entry.pron_raw or entry.headword_raw, profile)

    store.insert_entries(entries_a + entries_b, applied)

    candidates = match_dictionaries(store, "A", "B", min_fuzzy_score=80)
    statuses = {candidate.status for candidate in candidates}

    assert "exact" in statuses
    assert "fuzzy" in statuses
