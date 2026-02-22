from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any
from uuid import uuid4

from rapidfuzz import fuzz, process

from ..storage.sqlite import SQLiteStore

SPACE_RE = re.compile(r"\s+")


class CompareWorkflowError(RuntimeError):
    pass


def _effective(row: dict[str, Any], stem: str) -> str:
    edit = row.get(f"{stem}_edit")
    if edit is not None and str(edit).strip():
        return str(edit).strip()
    return str(row.get(f"{stem}_raw") or "").strip()


def _normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFC", str(value or ""))
    normalized = SPACE_RE.sub(" ", normalized).strip().lower()
    return normalized


def _key_for_row(row: dict[str, Any], key_field: str) -> str:
    if key_field == "headword_norm_effective":
        headword_norm = str(row.get("headword_norm") or "").strip()
        if headword_norm:
            return _normalized_text(headword_norm)

        form_norm = str(row.get("form_norm") or "").strip()
        if form_norm:
            return _normalized_text(form_norm)

        return _normalized_text(_effective(row, "headword"))

    if key_field == "form_norm_effective":
        form_norm = str(row.get("form_norm") or "").strip()
        if form_norm:
            return _normalized_text(form_norm)
        return _normalized_text(_effective(row, "headword"))

    if key_field == "headword_effective":
        return _normalized_text(_effective(row, "headword"))

    if key_field in row and row.get(key_field) is not None:
        return _normalized_text(str(row.get(key_field) or ""))

    return _normalized_text(_effective(row, "headword"))


def _rows_for_corpora(store: SQLiteStore, corpus_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for corpus_id in corpus_ids:
        payload[corpus_id] = [dict(row) for row in store.entries_for_dict(corpus_id)]
    return payload


def _entry_index_by_key(
    rows: list[dict[str, Any]],
    key_field: str,
) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        rows, key=lambda row: (int(row.get("line_no") or 0), str(row.get("entry_id") or ""))
    )
    index: dict[str, dict[str, Any]] = {}
    for row in ordered:
        key = _key_for_row(row, key_field)
        if not key or key in index:
            continue
        index[key] = row
    return index


def _coverage_filter(
    row: dict[str, Any],
    *,
    corpus_ids: list[str],
    mode: str,
) -> bool:
    presence = row.get("presence", {})
    if not isinstance(presence, dict):
        return True

    if not corpus_ids:
        return True

    if mode == "all":
        return True

    if mode == "common_all":
        return all(bool(presence.get(corpus_id, False)) for corpus_id in corpus_ids)

    if len(corpus_ids) < 2:
        return True

    corpus_a = corpus_ids[0]
    corpus_b = corpus_ids[1]
    in_a = bool(presence.get(corpus_a, False))
    in_b = bool(presence.get(corpus_b, False))

    if mode == "only_a":
        return in_a and not any(
            bool(presence.get(corpus_id, False))
            for corpus_id in corpus_ids
            if corpus_id != corpus_a
        )
    if mode == "only_b":
        return in_b and not any(
            bool(presence.get(corpus_id, False))
            for corpus_id in corpus_ids
            if corpus_id != corpus_b
        )
    if mode == "a_not_b":
        return in_a and not in_b
    if mode == "b_not_a":
        return in_b and not in_a

    return True


def preview_coverage(
    db_path: Path,
    corpus_ids: list[str],
    limit: int | None = 500,
    filters: dict[str, Any] | None = None,
    key_field: str = "headword_norm_effective",
) -> dict[str, Any]:
    if len(corpus_ids) < 2:
        raise CompareWorkflowError("At least 2 corpora are required for coverage preview.")

    store = SQLiteStore(db_path)
    by_corpus = _rows_for_corpora(store, corpus_ids)

    presence_by_key: dict[str, dict[str, bool]] = {}
    for corpus_id, rows in by_corpus.items():
        for row in rows:
            key = _key_for_row(row, key_field)
            if not key:
                continue
            presence_by_key.setdefault(key, {})
            presence_by_key[key][corpus_id] = True

    rows: list[dict[str, Any]] = []
    for key in sorted(presence_by_key.keys()):
        presence = {
            corpus_id: bool(presence_by_key[key].get(corpus_id, False)) for corpus_id in corpus_ids
        }
        rows.append(
            {
                "headword_key": key,
                "presence": presence,
            }
        )

    mode = str((filters or {}).get("mode", "all"))
    filtered_rows = [row for row in rows if _coverage_filter(row, corpus_ids=corpus_ids, mode=mode)]

    if limit is not None:
        filtered_rows = filtered_rows[:limit]

    union_count = len(rows)
    common_count = sum(
        1
        for row in rows
        if all(bool(row["presence"].get(corpus_id, False)) for corpus_id in corpus_ids)
    )

    corpus_a = corpus_ids[0]
    corpus_b = corpus_ids[1]
    unique_a = sum(
        1
        for row in rows
        if bool(row["presence"].get(corpus_a, False))
        and not any(
            bool(row["presence"].get(corpus_id, False))
            for corpus_id in corpus_ids
            if corpus_id != corpus_a
        )
    )
    unique_b = sum(
        1
        for row in rows
        if bool(row["presence"].get(corpus_b, False))
        and not any(
            bool(row["presence"].get(corpus_id, False))
            for corpus_id in corpus_ids
            if corpus_id != corpus_b
        )
    )

    return {
        "corpus_ids": corpus_ids,
        "key_field": key_field,
        "filters": {"mode": mode},
        "rows": filtered_rows,
        "counts": {
            "union": union_count,
            "common_all": common_count,
            "unique_a": unique_a,
            "unique_b": unique_b,
            "displayed": len(filtered_rows),
        },
    }


def _alignment_rows_exact(
    *,
    corpus_a: str,
    corpus_b: str,
    index_a: dict[str, dict[str, Any]],
    index_b: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str], set[str]]:
    exact_rows: list[dict[str, Any]] = []
    matched_a: set[str] = set()
    matched_b: set[str] = set()

    for key in sorted(set(index_a.keys()) & set(index_b.keys())):
        row_a = index_a[key]
        row_b = index_b[key]
        exact_rows.append(
            {
                "corpus_a": corpus_a,
                "corpus_b": corpus_b,
                "headword_key": key,
                "entry_id_a": row_a.get("entry_id"),
                "entry_id_b": row_b.get("entry_id"),
                "headword_a": _effective(row_a, "headword"),
                "headword_b": _effective(row_b, "headword"),
                "headword_norm_a": key,
                "headword_norm_b": key,
                "status_a": row_a.get("status") or "auto",
                "status_b": row_b.get("status") or "auto",
                "score": 100.0,
                "method": "exact",
                "reason": "",
            }
        )
        matched_a.add(key)
        matched_b.add(key)

    return exact_rows, matched_a, matched_b


def _alignment_rows_fuzzy(
    *,
    corpus_a: str,
    corpus_b: str,
    index_a: dict[str, dict[str, Any]],
    index_b: dict[str, dict[str, Any]],
    matched_a: set[str],
    matched_b: set[str],
    threshold: int,
) -> tuple[list[dict[str, Any]], set[str], set[str]]:
    rows: list[dict[str, Any]] = []
    remaining_b = sorted([key for key in index_b.keys() if key not in matched_b])
    taken_b: set[str] = set()

    for key_a in sorted([key for key in index_a.keys() if key not in matched_a]):
        choices = [key for key in remaining_b if key not in taken_b]
        if not choices:
            continue

        best = process.extractOne(key_a, choices, scorer=fuzz.ratio)
        if not best:
            continue

        key_b, score, _ = best
        if int(score) < int(threshold):
            continue

        row_a = index_a[key_a]
        row_b = index_b[key_b]
        rows.append(
            {
                "corpus_a": corpus_a,
                "corpus_b": corpus_b,
                "headword_key": key_a,
                "entry_id_a": row_a.get("entry_id"),
                "entry_id_b": row_b.get("entry_id"),
                "headword_a": _effective(row_a, "headword"),
                "headword_b": _effective(row_b, "headword"),
                "headword_norm_a": key_a,
                "headword_norm_b": key_b,
                "status_a": row_a.get("status") or "auto",
                "status_b": row_b.get("status") or "auto",
                "score": float(score),
                "method": "fuzzy",
                "reason": "",
            }
        )
        matched_a.add(key_a)
        matched_b.add(key_b)
        taken_b.add(key_b)

    return rows, matched_a, matched_b


def _alignment_rows_unmatched(
    *,
    corpus_a: str,
    corpus_b: str,
    index_a: dict[str, dict[str, Any]],
    index_b: dict[str, dict[str, Any]],
    matched_a: set[str],
    matched_b: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for key_a in sorted([key for key in index_a.keys() if key not in matched_a]):
        row_a = index_a[key_a]
        rows.append(
            {
                "corpus_a": corpus_a,
                "corpus_b": corpus_b,
                "headword_key": key_a,
                "entry_id_a": row_a.get("entry_id"),
                "entry_id_b": None,
                "headword_a": _effective(row_a, "headword"),
                "headword_b": "",
                "headword_norm_a": key_a,
                "headword_norm_b": "",
                "status_a": row_a.get("status") or "auto",
                "status_b": "",
                "score": 0.0,
                "method": "none",
                "reason": "no_match",
            }
        )

    for key_b in sorted([key for key in index_b.keys() if key not in matched_b]):
        row_b = index_b[key_b]
        rows.append(
            {
                "corpus_a": corpus_a,
                "corpus_b": corpus_b,
                "headword_key": key_b,
                "entry_id_a": None,
                "entry_id_b": row_b.get("entry_id"),
                "headword_a": "",
                "headword_b": _effective(row_b, "headword"),
                "headword_norm_a": "",
                "headword_norm_b": key_b,
                "status_a": "",
                "status_b": row_b.get("status") or "auto",
                "score": 0.0,
                "method": "none",
                "reason": "no_match",
            }
        )

    return rows


def preview_alignment(
    db_path: Path,
    corpus_a: str,
    corpus_b: str,
    mode: str = "exact",
    threshold: int = 90,
    limit: int | None = 500,
    key_field: str = "headword_norm_effective",
    include_unmatched: bool = True,
) -> dict[str, Any]:
    if corpus_a == corpus_b:
        raise CompareWorkflowError("Corpus A and B must be different.")

    clean_mode = mode.strip().lower()
    if clean_mode not in {"exact", "exact+fuzzy", "fuzzy"}:
        raise CompareWorkflowError("mode must be one of: exact, exact+fuzzy, fuzzy")

    store = SQLiteStore(db_path)
    rows_a = [dict(row) for row in store.entries_for_dict(corpus_a)]
    rows_b = [dict(row) for row in store.entries_for_dict(corpus_b)]

    index_a = _entry_index_by_key(rows_a, key_field)
    index_b = _entry_index_by_key(rows_b, key_field)

    exact_rows, matched_a, matched_b = _alignment_rows_exact(
        corpus_a=corpus_a,
        corpus_b=corpus_b,
        index_a=index_a,
        index_b=index_b,
    )

    fuzzy_rows: list[dict[str, Any]] = []
    if clean_mode in {"fuzzy", "exact+fuzzy"}:
        fuzzy_rows, matched_a, matched_b = _alignment_rows_fuzzy(
            corpus_a=corpus_a,
            corpus_b=corpus_b,
            index_a=index_a,
            index_b=index_b,
            matched_a=matched_a,
            matched_b=matched_b,
            threshold=int(threshold),
        )

    unmatched_rows: list[dict[str, Any]] = []
    if include_unmatched:
        unmatched_rows = _alignment_rows_unmatched(
            corpus_a=corpus_a,
            corpus_b=corpus_b,
            index_a=index_a,
            index_b=index_b,
            matched_a=matched_a,
            matched_b=matched_b,
        )

    rows = exact_rows + fuzzy_rows + unmatched_rows
    rows.sort(key=lambda row: (row["method"] != "exact", -float(row["score"]), row["headword_key"]))
    if limit is not None:
        rows = rows[:limit]

    return {
        "corpus_a": corpus_a,
        "corpus_b": corpus_b,
        "mode": clean_mode,
        "threshold": int(threshold),
        "key_field": key_field,
        "include_unmatched": include_unmatched,
        "rows": rows,
        "counts": {
            "total_a": len(index_a),
            "total_b": len(index_b),
            "matched_exact": len(exact_rows),
            "matched_fuzzy": len(fuzzy_rows),
            "unmatched": len(unmatched_rows),
            "displayed": len(rows),
        },
    }


def _features_from_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("features_json")
    if not raw:
        return {}
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _entry_text_for_diff(row: dict[str, Any], stem: str) -> str:
    value = str(row.get(stem) or "").strip()
    if value:
        return value

    if stem == "pron_norm":
        fallback = str(row.get("form_norm") or "").strip()
        if fallback:
            return fallback
        pron_effective = _effective(row, "pron")
        if pron_effective:
            return _normalized_text(pron_effective)

    if stem == "pron_render":
        fallback_display = str(row.get("form_display") or "").strip()
        if fallback_display:
            return fallback_display

    return ""


def _delta_from_rows(row_a: dict[str, Any], row_b: dict[str, Any]) -> dict[str, Any]:
    features_a = _features_from_row(row_a)
    features_b = _features_from_row(row_b)

    stress_a = features_a.get("stress_schema") or features_a.get("stress")
    stress_b = features_b.get("stress_schema") or features_b.get("stress")

    return {
        "syll_count_diff": int(row_a.get("syllables") or 0) - int(row_b.get("syllables") or 0),
        "stress_schema_diff": stress_a != stress_b,
        "prime_count_diff": int(features_a.get("prime_count", 0) or 0)
        - int(features_b.get("prime_count", 0) or 0),
        "accented_vowel_count_diff": int(features_a.get("accented_vowel_count", 0) or 0)
        - int(features_b.get("accented_vowel_count", 0) or 0),
    }


def _diff_filter(row: dict[str, Any], mode: str) -> bool:
    if mode == "all":
        return True

    delta = row.get("delta", {})
    if not isinstance(delta, dict):
        return True

    if mode == "pron_render_diff":
        return row.get("pron_render_a") != row.get("pron_render_b")
    if mode == "syll_count_diff":
        return int(delta.get("syll_count_diff", 0) or 0) != 0
    if mode == "stress_diff":
        return bool(delta.get("stress_schema_diff", False))

    return True


def preview_diff(
    db_path: Path,
    run_settings: dict[str, Any],
    limit: int | None = 500,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = SQLiteStore(db_path)

    if run_settings.get("run_id"):
        run_id = str(run_settings["run_id"])
        pair_rows = [
            dict(row) for row in store.compare_alignment_pairs(run_id, include_unmatched=False)
        ]
        corpus_a = pair_rows[0]["corpus_a"] if pair_rows else ""
        corpus_b = pair_rows[0]["corpus_b"] if pair_rows else ""
    elif run_settings.get("alignment_rows"):
        pair_rows = [
            dict(row)
            for row in run_settings.get("alignment_rows", [])
            if row.get("entry_id_a") and row.get("entry_id_b")
        ]
        corpus_a = str(
            run_settings.get("corpus_a") or (pair_rows[0]["corpus_a"] if pair_rows else "")
        )
        corpus_b = str(
            run_settings.get("corpus_b") or (pair_rows[0]["corpus_b"] if pair_rows else "")
        )
    else:
        corpus_a = str(run_settings.get("corpus_a") or "")
        corpus_b = str(run_settings.get("corpus_b") or "")
        alignment_preview = preview_alignment(
            db_path=db_path,
            corpus_a=corpus_a,
            corpus_b=corpus_b,
            mode=str(run_settings.get("mode", "exact+fuzzy")),
            threshold=int(run_settings.get("fuzzy_threshold", 90)),
            limit=None,
            key_field=str(run_settings.get("key_field", "headword_norm_effective")),
            include_unmatched=False,
        )
        pair_rows = [dict(row) for row in alignment_preview["rows"]]

    rows: list[dict[str, Any]] = []
    for pair in pair_rows:
        entry_id_a = str(pair.get("entry_id_a") or "")
        entry_id_b = str(pair.get("entry_id_b") or "")
        if not entry_id_a or not entry_id_b:
            continue

        row_a = store.entry_by_id(entry_id_a)
        row_b = store.entry_by_id(entry_id_b)
        if row_a is None or row_b is None:
            continue

        row_a_dict = dict(row_a)
        row_b_dict = dict(row_b)
        features_a = _features_from_row(row_a_dict)
        features_b = _features_from_row(row_b_dict)
        delta = _delta_from_rows(row_a_dict, row_b_dict)

        rows.append(
            {
                "corpus_a": pair.get("corpus_a") or corpus_a,
                "corpus_b": pair.get("corpus_b") or corpus_b,
                "headword_key": pair.get("headword_key") or "",
                "entry_id_a": entry_id_a,
                "entry_id_b": entry_id_b,
                "pron_norm_a": _entry_text_for_diff(row_a_dict, "pron_norm"),
                "pron_norm_b": _entry_text_for_diff(row_b_dict, "pron_norm"),
                "pron_render_a": _entry_text_for_diff(row_a_dict, "pron_render"),
                "pron_render_b": _entry_text_for_diff(row_b_dict, "pron_render"),
                "features_a": features_a,
                "features_b": features_b,
                "delta": delta,
            }
        )

    mode = str((filters or {}).get("mode", "all"))
    filtered_rows = [row for row in rows if _diff_filter(row, mode)]
    if limit is not None:
        filtered_rows = filtered_rows[:limit]

    return {
        "corpus_a": corpus_a,
        "corpus_b": corpus_b,
        "filters": {"mode": mode},
        "rows": filtered_rows,
        "counts": {
            "total_pairs": len(rows),
            "displayed": len(filtered_rows),
            "pron_render_diff": sum(
                1 for row in rows if row.get("pron_render_a") != row.get("pron_render_b")
            ),
            "syll_count_diff": sum(
                1 for row in rows if int(row.get("delta", {}).get("syll_count_diff", 0) or 0) != 0
            ),
            "stress_diff": sum(
                1 for row in rows if bool(row.get("delta", {}).get("stress_schema_diff", False))
            ),
        },
    }


def _settings_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    canonical = canonical.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def apply_compare_run(
    db_path: Path,
    corpus_ids: list[str],
    corpus_a: str,
    corpus_b: str,
    settings: dict[str, Any],
) -> dict[str, Any]:
    if len(corpus_ids) < 2:
        raise CompareWorkflowError("At least 2 corpora are required.")
    if corpus_a not in corpus_ids or corpus_b not in corpus_ids:
        raise CompareWorkflowError("corpus_a and corpus_b must be selected in corpus_ids.")

    key_field = str(settings.get("key_field", "headword_norm_effective"))
    mode = str(settings.get("mode", "exact+fuzzy"))
    threshold = int(settings.get("fuzzy_threshold", 90))
    algorithm = str(settings.get("algorithm", "greedy"))

    coverage_preview = preview_coverage(
        db_path=db_path,
        corpus_ids=corpus_ids,
        limit=None,
        filters={"mode": "all"},
        key_field=key_field,
    )
    alignment_preview = preview_alignment(
        db_path=db_path,
        corpus_a=corpus_a,
        corpus_b=corpus_b,
        mode=mode,
        threshold=threshold,
        limit=None,
        key_field=key_field,
        include_unmatched=True,
    )
    diff_preview = preview_diff(
        db_path=db_path,
        run_settings={
            "alignment_rows": alignment_preview["rows"],
            "corpus_a": corpus_a,
            "corpus_b": corpus_b,
        },
        limit=None,
        filters={"mode": "all"},
    )

    run_id = uuid4().hex
    settings_payload = {
        "corpus_ids": sorted(corpus_ids),
        "corpus_a": corpus_a,
        "corpus_b": corpus_b,
        "key_field": key_field,
        "mode": mode,
        "fuzzy_threshold": threshold,
        "algorithm": algorithm,
    }
    settings_sha256 = _settings_hash(settings_payload)

    coverage_rows_for_storage: list[dict[str, Any]] = []
    for row in coverage_preview["rows"]:
        presence = row.get("presence", {})
        if not isinstance(presence, dict):
            continue
        for corpus_id, present in presence.items():
            coverage_rows_for_storage.append(
                {
                    "headword_key": row["headword_key"],
                    "corpus_id": corpus_id,
                    "present": bool(present),
                }
            )

    alignment_rows_for_storage = [dict(row) for row in alignment_preview["rows"]]
    diff_rows_for_storage = [dict(row) for row in diff_preview["rows"]]

    store = SQLiteStore(db_path)
    stats = {
        "coverage": coverage_preview["counts"],
        "alignment": alignment_preview["counts"],
        "diff": diff_preview["counts"],
    }

    store.save_compare_run(
        run_id=run_id,
        corpus_ids=corpus_ids,
        key_field=key_field,
        mode=mode,
        fuzzy_threshold=threshold,
        algorithm=algorithm,
        settings_sha256=settings_sha256,
        stats=stats,
    )
    store.clear_compare_run_data(run_id)
    store.insert_compare_coverage_items(run_id, coverage_rows_for_storage)
    store.insert_compare_alignment_pairs(run_id, alignment_rows_for_storage)
    store.insert_compare_diff_rows(run_id, diff_rows_for_storage)

    return {
        "run_id": run_id,
        "settings_sha256": settings_sha256,
        "corpus_ids": corpus_ids,
        "corpus_a": corpus_a,
        "corpus_b": corpus_b,
        "key_field": key_field,
        "mode": mode,
        "fuzzy_threshold": threshold,
        "algorithm": algorithm,
        "stats": stats,
    }


def list_compare_runs(db_path: Path, limit: int = 30) -> list[dict[str, Any]]:
    store = SQLiteStore(db_path)
    rows = []
    for row in store.list_compare_runs(limit=limit):
        row_dict = dict(row)
        try:
            row_dict["corpus_ids"] = json.loads(str(row_dict.get("corpus_ids_json") or "[]"))
        except json.JSONDecodeError:
            row_dict["corpus_ids"] = []
        try:
            row_dict["stats"] = json.loads(str(row_dict.get("stats_json") or "{}"))
        except json.JSONDecodeError:
            row_dict["stats"] = {}
        rows.append(row_dict)
    return rows


def load_compare_run_data(db_path: Path, run_id: str) -> dict[str, Any]:
    store = SQLiteStore(db_path)
    run = store.compare_run_by_id(run_id)
    if run is None:
        raise CompareWorkflowError(f"Compare run not found: {run_id}")

    run_dict = dict(run)
    try:
        corpus_ids = json.loads(str(run_dict.get("corpus_ids_json") or "[]"))
    except json.JSONDecodeError:
        corpus_ids = []
    try:
        stats = json.loads(str(run_dict.get("stats_json") or "{}"))
    except json.JSONDecodeError:
        stats = {}

    coverage_rows_raw = [dict(row) for row in store.compare_coverage_items(run_id)]
    coverage_by_key: dict[str, dict[str, bool]] = {}
    for row in coverage_rows_raw:
        key = str(row.get("headword_key") or "")
        corpus_id = str(row.get("corpus_id") or "")
        present = bool(int(row.get("present") or 0))
        coverage_by_key.setdefault(key, {})
        coverage_by_key[key][corpus_id] = present

    coverage_rows = [
        {
            "headword_key": key,
            "presence": {
                corpus_id: bool(coverage_by_key[key].get(corpus_id, False))
                for corpus_id in corpus_ids
            },
        }
        for key in sorted(coverage_by_key.keys())
    ]

    alignment_rows = [
        dict(row) for row in store.compare_alignment_pairs(run_id, include_unmatched=True)
    ]

    diff_rows_raw = [dict(row) for row in store.compare_diff_rows(run_id)]
    diff_rows: list[dict[str, Any]] = []
    for row in diff_rows_raw:
        try:
            features_a = json.loads(str(row.get("features_a_json") or "{}"))
        except json.JSONDecodeError:
            features_a = {}
        try:
            features_b = json.loads(str(row.get("features_b_json") or "{}"))
        except json.JSONDecodeError:
            features_b = {}
        try:
            delta = json.loads(str(row.get("delta_json") or "{}"))
        except json.JSONDecodeError:
            delta = {}

        diff_rows.append(
            {
                "corpus_a": alignment_rows[0]["corpus_a"] if alignment_rows else "",
                "corpus_b": alignment_rows[0]["corpus_b"] if alignment_rows else "",
                "headword_key": row.get("headword_key") or "",
                "entry_id_a": row.get("entry_id_a"),
                "entry_id_b": row.get("entry_id_b"),
                "pron_norm_a": row.get("pron_norm_a") or "",
                "pron_norm_b": row.get("pron_norm_b") or "",
                "pron_render_a": row.get("pron_render_a") or "",
                "pron_render_b": row.get("pron_render_b") or "",
                "features_a": features_a,
                "features_b": features_b,
                "delta": delta,
            }
        )

    return {
        "run": {
            **run_dict,
            "corpus_ids": corpus_ids,
            "stats": stats,
        },
        "coverage": {
            "corpus_ids": corpus_ids,
            "rows": coverage_rows,
            "counts": stats.get("coverage", {}),
        },
        "alignment": {
            "rows": alignment_rows,
            "counts": stats.get("alignment", {}),
        },
        "diff": {
            "rows": diff_rows,
            "counts": stats.get("diff", {}),
        },
    }
