# Atelier de gabarits

## Philosophie

- Les **gabarits** font l'extraction structurée depuis des records source (TXT/PDF texte/CSV) vers des entrées diplomatiques.
- Les **conventions** (profils YAML) restent séparées et transforment ensuite `raw -> display/norm/features`.
- Les champs `*_raw` ne sont jamais modifiés par un profil.

## Workflow

1. Importer une source (`TXT`, `CSV`, ou `PDF texte` converti en `.txt`).
2. Ouvrir l'onglet `Atelier`.
3. Choisir un gabarit.
4. Prévisualiser (200 lignes par défaut).
5. Appliquer au corpus.
6. Vérifier les alertes et, si souhaité, appliquer les conventions.

## Gabarits socle

### 1) `WORDLIST_TOKENS`

- Entrée: ligne texte.
- Sortie: `1..N` entrées (tokens).
- Ignore les tokens ponctuation/artefacts (`.`, `..`, `...`, `*`, `-`, `—`, `•`).
- Option: `trim_token_punctuation`.

### 2) `ENTRY_PLUS_DEFINITION`

- Entrée: ligne texte.
- Sortie: `1` entrée.
- Coupe en deux parties (`headword_raw`, `definition_raw`) selon un séparateur simple:
  - `comma`, `semicolon`, `double_space`, `tab`, `custom`.

### 3) `HEADWORD_PLUS_PRON`

- Entrée: ligne texte.
- Sortie: `1` entrée (`headword_raw`, `pron_raw`).
- Séparateur: `tab`, `multi_spaces`, `custom`.
- Option: `trim_punctuation`.

### 4) `CSV_MAPPING`

- Entrée: ligne CSV (mapping colonne -> valeur).
- Sortie: `1..N` entrées.
- Paramètres:
  - `headword_column` (requis)
  - `pron_column` (optionnel)
  - `definition_column` (optionnel)
  - `split_headword`: `none|whitespace|semicolon|comma`
  - `ignore_empty_headword`

## Alertes de l'atelier

L'application de gabarit peut produire des issues stockées en base:

- `EMPTY_HEADWORD`
- `UNRECOGNIZED_RECORD`
- `PUNCT_ONLY_TOKEN`

## Reproductibilité (versioning)

Chaque application enregistre:

- `template_id`, `template_kind`, `version`
- `params_json`
- `sha256` (hash canonique du template + params)
- compteurs (`records_count`, `entries_count`)
- statut (`ok/error`) et horodatage

Tables SQLite associées:

- `corpus_templates` (template actif par corpus)
- `template_applications` (historique)

## CLI minimale

- `dicodiachro template sources <project_dir>`
- `dicodiachro template preview <project_dir> --corpus <id> --kind <kind> [--source ...] [--params '{...}']`
- `dicodiachro template apply <project_dir> --corpus <id> --kind <kind> [--source ...] [--params '{...}']`
