# Phase 2 : Champs configurables — implémenté

## Ce qui a été fait

### 1. Stockage
- **Colonne `extra_json`** (TEXT) dans la table `entries` : objet JSON clé-valeur pour les champs personnalisés (étymologie, genre, nombre, etc.).
- **Table `corpus_field_schema`** : `(corpus_id, field_id, label, field_type, sort_order, optional)` — définit quels champs existent et leur ordre d’affichage par corpus.

### 2. Store
- `get_field_schema(corpus_id)` → liste de définitions (field_id, label, field_type, sort_order, optional).
- `set_field_schema(corpus_id, schema)` → remplace le schéma du corpus.
- `update_entry_extra(entry_id, updates: dict[str, str])` → fusionne des paires clé-valeur dans `extra_json` de l’entrée.

### 3. UI — Schéma de champs
- **Menu « ⋯ Plus »** dans l’onglet Entrées → **« Champs personnalisés… »**.
- Dialogue **FieldSchemaDialog** : liste des champs (field_id — label), **Ajouter** (saisie id + libellé), **Retirer**, **Monter** / **Descendre**, **Enregistrer**.

### 4. Onglet Entrées
- Au chargement / changement de corpus : lecture du schéma, **colonnes dynamiques** ajoutées après les 10 colonnes fixes (section … page).
- Chaque champ du schéma = une colonne ; valeurs lues depuis `extra_json` ; édition possible (sauf entrées supprimées).
- **Sauvegarde** : les modifications des champs personnalisés sont enregistrées via `update_entry_extra` en même temps que les champs classiques (Save edits).
- **Vue Nomenclature** : les colonnes personnalisées sont masquées (seules section, headword, status, flags, page restent visibles).

## Utilisation
1. Ouvrir un projet et sélectionner un corpus.
2. Menu **⋯ Plus** → **Champs personnalisés…**.
3. Ajouter des champs (ex. `etymology` / Étymologie, `gender` / Genre).
4. Enregistrer → les colonnes apparaissent dans l’onglet Entrées.
5. Saisir ou modifier les valeurs dans le tableau puis **Save edits** (Ctrl+S).

## Évolutions possibles
- Types de champ (texte / nombre / liste) et validation.
- Export CSV/JSONL des champs personnalisés.
- Remplissage depuis le parseur ou des conventions (mapping vers `extra_json`).
