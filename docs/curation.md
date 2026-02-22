# Curation manuelle

## Principe

DicoDiachro sépare deux couches:

- **Extraction automatique**: gabarits (templates) qui produisent les champs diplomatiques `*_raw`.
- **Correction manuelle**: overrides et champs `*_edit` appliqués par curation humaine.

Les champs diplomatiques `*_raw` ne sont jamais écrasés.

## Effet immédiat + audit

Chaque correction manuelle:

1. met à jour immédiatement l'état visible (preview atelier et table Entrées),
2. est historisée dans `entry_overrides` (before/after, horodatage, opération, note),
3. est rejouable/annulable (`Undo` en atelier et dans la vue Entrées).

## Overrides record-level (Atelier)

Scope `record`:

- `SKIP_RECORD`
- `SPLIT_RECORD`
- `EDIT_RECORD`

Ces overrides sont appliqués au moment de `Apply template` après l'extraction batch, avec matching par `source_id + record_key`.

## Overrides entry-level (Entrées)

Scope `entry`:

- `CREATE_ENTRY` (ajout manuel en Curation ou depuis un record non reconnu)
- `EDIT_ENTRY` (`headword_edit`, `pron_edit`, `definition_edit`)
- `SPLIT_ENTRY`
- `MERGE_ENTRY`
- `REVIEW_ENTRY` / `VALIDATE_ENTRY`

L'export conserve le raw et expose aussi des champs effectifs (`*_effective`) qui reflètent les éditions.

## Ajouter une entrée

Deux points d'entrée UX:

1. **Curation → ⋯ Plus → Ajouter une entrée…**
2. **Atelier gabarits → menu contextuel d'une ligne “Non reconnu” → Créer une entrée…**

Dans les deux cas:

- `headword_raw` est obligatoire,
- `pron_raw` est optionnel, avec option **Entrée = prononciation** (`pron_raw=headword_raw`),
- `definition_raw` est optionnel,
- un log `entry_overrides` est écrit avec `op=CREATE_ENTRY`.

Les entrées créées sont incluses dans les exports, exploitables dans l'atelier Conventions et comparables dans l'atelier Comparer.

## Supprimer / Restaurer (corbeille)

La suppression en Curation est une **corbeille** (soft-delete), pas un effacement physique:

- `is_deleted=1`
- `deleted_at` horodaté
- `deleted_reason` optionnel

Les entrées supprimées sont masquées par défaut. Le toggle **Afficher supprimées** permet:

- de les afficher en grisé avec indicateur corbeille,
- de consulter raison/date en tooltip,
- de les restaurer.

Chaque action est tracée dans `entry_overrides`:

- `DELETE_ENTRY`
- `RESTORE_ENTRY`

Rien n'est perdu: restauration possible sans réimport.
