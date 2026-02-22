# Conventions

## Gabarit vs Convention

- **Gabarit**: extrait des `entries` depuis des records source (TXT/PDF texte/CSV).
- **Convention**: transforme les formes effectives (`*_edit` si présent, sinon `*_raw`) en couches dérivées sans toucher au raw.

## Couches dérivées

Une application de convention produit, par entrée:

- `headword_norm`
- `pron_norm`
- `pron_render`
- `features_json`

Les champs diplomatiques (`headword_raw`, `pron_raw`) restent intacts.

## YAML de convention

Les conventions réutilisent le schéma profile YAML avec un bloc `render`:

```yaml
profile_id: analysis_quantity_v1
version: 1

unicode:
  normalization: NFC

display:
  normalize_primes: true
  keep_long_s: true
  collapse_spaces: true

norm:
  lowercase: true
  strip_diacritics: true
  long_s_to_s: true
  remove_punctuation: true
  keep_hyphen: false
  collapse_spaces: true

render:
  enabled: true
  source: display
  parenthesize_accented_vowel: true
  parenthesize_prime_segment: true
  open_paren: "("
  close_paren: ")"
  collapse_spaces: true

qa:
  enforce_stress_consistency: false
  require_prime_for_primary_stress: false
  require_acute_for_primary_stress: false
  require_pronunciation: false
```

## Workflow atelier

1. Choisir une convention.
2. Prévisualiser sur N entrées.
3. Vérifier les alertes (symboles inconnus, stress, etc.).
4. Appliquer au corpus.
5. Affiner (éditer YAML, re-prévisualiser, réappliquer).

## Historique et reproductibilité

Chaque application est journalisée dans `convention_applications` avec:

- `profile_id`, `profile_version`, `profile_sha256`
- volume traité (`entries_count`)
- alertes (`issues_count`)
- date, statut, détails

Le hash est stable (contenu YAML canonisé, chemins exclus).
