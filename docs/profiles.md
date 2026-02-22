# Profiles

Les profils sont des fichiers YAML déterministes qui définissent la retranscription sans jamais modifier la couche diplomatique (`*_raw`).

## Principes

- `headword_raw` / `pron_raw` sont immuables (diplomatique).
- `form_display` = forme lisible pour l'édition.
- `form_norm` = forme normalisée pour alignement/comparaison.
- `features_json` = traits calculés (stress, quantité, symboles, etc.).
- Chaque application de profil est tracée en base (`profile_applications`) avec `profile_id`, `version`, `sha256`, date et volume traité.

## Schéma YAML stable

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

features:
  vowels: "aeiouyAEIOUY"
  marks:
    prime: "ʹ"
    acute_vowels: "áéíóúÁÉÍÓÚ"
    macron: "\u0304"
    breve: "\u0306"
  rules:
    - when:
        contains_any: "áéíóúÁÉÍÓÚ"
      set:
        has_accented_vowel: true
    - when:
        pattern: "(áʹ|éʹ|íʹ|óʹ|úʹ)"
      set:
        stress: "primary"
    - when:
        contains: "ʹ"
      set:
        has_prime: true

qa:
  enforce_stress_consistency: false
  require_prime_for_primary_stress: false
  require_acute_for_primary_stress: false
  require_pronunciation: false
```

## Validation

Validation stricte sans dépendance externe:

- `profile_id`: string non vide (requis)
- `version`: entier (requis)
- `unicode.normalization`: `NFC|NFD|NFKC|NFKD`
- sections reconnues: `display`, `norm`, `features`, `qa`
- section optionnelle `render` pour la retranscription publication (`pron_render`)

Clés inconnues:

- mode normal: warning de validation
- mode strict (`dicodiachro profile validate --strict`): erreur bloquante

Erreur de schéma/YAML:

- exception `ProfileValidationError`
- issue `PROFILE_INVALID` dans le pipeline/apply

## Transformations prises en charge

- normalisation Unicode (`unicodedata.normalize`)
- normalisation apostrophes/primes (`'`/`’` -> `ʹ`)
- conversion `ſ -> s` (configurable en `norm.long_s_to_s`)
- suppression diacritiques (`norm.strip_diacritics`)
- suppression ponctuation (`norm.remove_punctuation`, `keep_hyphen`)
- réduction espaces (`collapse_spaces`)
- rendu publication (`render`) avec parenthésage configurable des accents/primes

## Features et QA profile-aware

Traits de base calculés:

- `symbols_used`
- `unknown_symbols`
- `has_prime`, `prime_count`
- `accented_vowel_count`
- `combining_detached_count`
- `primary_stress_count`

Issues QA dérivées:

- `UNKNOWN_SYMBOL`
- `DETACHED_COMBINING_MARK`
- `MULTIPLE_PRIMARY_STRESS`
- `INCONSISTENT_STRESS`

### Bloc `qa`

Le bloc `qa` pilote les contrôles de cohérence accentuelle:

- `enforce_stress_consistency` (bool, défaut `false`)
- `require_prime_for_primary_stress` (bool, défaut `false`)
- `require_acute_for_primary_stress` (bool, défaut `false`)
- `require_pronunciation` (bool, défaut `false`) : déclenche `MISSING_PRON` si `pron` absente

Comportement:

- si `enforce_stress_consistency` est `false`, aucune issue `INCONSISTENT_STRESS` n'est générée.
- si `true`:
  - avec `require_prime_for_primary_stress: true`: `accented_vowel_count > 0` et `prime_count == 0` => `INCONSISTENT_STRESS`
  - avec `require_acute_for_primary_stress: true`: `prime_count > 0` et `accented_vowel_count == 0` => `INCONSISTENT_STRESS`

Si les marques nécessaires (`features.marks.prime` / `features.marks.acute_vowels`) ne sont pas définies, la règle est automatiquement désactivée et un warning de validation est exposé.

## Inventaire de symboles

Pour limiter `UNKNOWN_SYMBOL`, définir un inventaire:

- `rules/symbols.yml` ou `rules/<dict_id>/symbols.yml`

Format:

```yaml
symbols:
  "ſ": { category: grapheme }
  "ʹ": { category: accent }
  "\u0304": { category: quantity }
```

Seules les clés de `symbols` sont utilisées comme caractères autorisés additionnels.

## Commandes CLI

- `dicodiachro profile validate <profile_path>`
- `dicodiachro profile preview <project_dir> --dict-id <id> --profile <id|path> --limit 50`
- `dicodiachro profile apply <project_dir> --dict-id <id> --profile <id|path>`

## Templates fournis

- `rules/templates/reading_v1.yml`
- `rules/templates/alignment_v1.yml`
- `rules/templates/analysis_quantity_v1.yml`
