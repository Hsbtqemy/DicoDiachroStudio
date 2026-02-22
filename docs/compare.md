# Compare

## But

L'atelier Comparer distingue trois niveaux:

- **Couverture**: présence/absence par corpus sur une clé normalisée.
- **Alignement**: correspondances A/B en exact puis fuzzy (greedy 1-to-1).
- **Diff phonologique**: comparaison de `pron_norm` / `pron_render` / `features` sur paires alignées.

## Recommandation de workflow

1. Importer les sources et appliquer un gabarit.
2. Appliquer les conventions (pour produire `headword_norm`, `pron_norm`, `pron_render`).
3. Ouvrir l'atelier Comparer.
4. Choisir les corpus et prévisualiser.
5. Appliquer le run pour le persister.
6. Exporter couverture / alignement / diff.

## Paramètres de run

- `key_field` (défaut: `headword_norm_effective`)
- `mode`: `exact` ou `exact+fuzzy`
- `fuzzy_threshold`: 70-95 (défaut 90)
- `algorithm`: `greedy`

## Tables SQLite

- `compare_runs`
- `compare_coverage_items`
- `compare_alignment_pairs`
- `compare_diff_rows`

Ces tables permettent de rejouer/inspecter un run sans recalcul, avec hash de paramètres (`settings_sha256`) et statistiques.

## Lecture scientifique

- Une forte zone `A pas dans B` peut signaler un changement de nomenclature.
- Un score fuzzy bas mais conservé peut signaler variation graphique historique.
- `pron_render` différent avec clé alignée stable peut signaler dérive phonologique ou convention de notation différente.
