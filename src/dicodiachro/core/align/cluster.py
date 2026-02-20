from __future__ import annotations

from collections import defaultdict

from ..models import MatchCandidate
from ..storage.sqlite import SQLiteStore


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            return x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def cluster_matches(
    store: SQLiteStore,
    candidates: list[MatchCandidate],
    min_score: float = 85.0,
) -> dict[str, str]:
    uf = UnionFind()

    for candidate in candidates:
        if candidate.score >= min_score:
            uf.union(candidate.entry_id_a, candidate.entry_id_b)

    groups: dict[str, list[str]] = defaultdict(list)
    for entry_id in list(uf.parent):
        root = uf.find(entry_id)
        groups[root].append(entry_id)

    mapping: dict[str, str] = {}
    for _, entry_ids in groups.items():
        entry_rows = [store.entry_by_id(entry_id) for entry_id in entry_ids]
        entry_rows = [row for row in entry_rows if row is not None]
        if not entry_rows:
            continue
        labels = [row["form_norm"] or row["headword_raw"] for row in entry_rows]
        lemma_label = sorted(labels)[0]
        group_id = store.upsert_lemma_group(lemma_label)

        for row in entry_rows:
            store.add_lemma_member(
                lemma_group_id=group_id,
                dict_id=row["dict_id"],
                entry_id=row["entry_id"],
                score=100.0,
                status="auto",
            )
            mapping[row["entry_id"]] = group_id

    return mapping
