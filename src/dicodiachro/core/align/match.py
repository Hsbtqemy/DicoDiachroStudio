from __future__ import annotations

from collections import defaultdict

from rapidfuzz import fuzz, process

from ..models import MatchCandidate
from ..storage.sqlite import SQLiteStore


def match_dictionaries(
    store: SQLiteStore,
    dict_id_a: str,
    dict_id_b: str,
    min_fuzzy_score: float = 85.0,
) -> list[MatchCandidate]:
    rows_a = store.entries_for_dict(dict_id_a)
    rows_b = store.entries_for_dict(dict_id_b)

    b_by_norm: dict[str, list] = defaultdict(list)
    for row in rows_b:
        b_by_norm[(row["form_norm"] or "").strip()].append(row)

    b_norm_keys = [key for key in b_by_norm if key]
    candidates: list[MatchCandidate] = []
    seen_pairs: set[tuple[str, str]] = set()

    for row_a in rows_a:
        norm_a = (row_a["form_norm"] or "").strip()
        if not norm_a:
            continue

        if norm_a in b_by_norm:
            for row_b in b_by_norm[norm_a]:
                pair = (row_a["entry_id"], row_b["entry_id"])
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                candidates.append(
                    MatchCandidate(
                        entry_id_a=row_a["entry_id"],
                        entry_id_b=row_b["entry_id"],
                        dict_id_a=dict_id_a,
                        dict_id_b=dict_id_b,
                        label_a=row_a["form_display"] or norm_a,
                        label_b=row_b["form_display"] or norm_a,
                        score=100.0,
                        status="exact",
                    )
                )
            continue

        if not b_norm_keys:
            continue
        best = process.extractOne(norm_a, b_norm_keys, scorer=fuzz.ratio)
        if not best:
            continue

        best_norm, score, _ = best
        if score < min_fuzzy_score:
            continue

        row_b = b_by_norm[best_norm][0]
        pair = (row_a["entry_id"], row_b["entry_id"])
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        candidates.append(
            MatchCandidate(
                entry_id_a=row_a["entry_id"],
                entry_id_b=row_b["entry_id"],
                dict_id_a=dict_id_a,
                dict_id_b=dict_id_b,
                label_a=row_a["form_display"] or norm_a,
                label_b=row_b["form_display"] or best_norm,
                score=float(score),
                status="fuzzy",
            )
        )

    candidates.sort(key=lambda c: (c.status != "exact", -c.score, c.label_a))
    return candidates
