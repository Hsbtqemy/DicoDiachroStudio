# Profiles

Profiles are YAML files (`rules/*.yml`) defining deterministic transcription behavior.

## Minimal keys

- `profile_id`, `name`, `version`
- `display`: readable rendering rules
- `alignment`: normalized form for matching
- `features`: optional stress/quantity extraction rules

## Built-in templates

- `reading_v1`: prime normalization + readable display
- `alignment_v1`: strip diacritics, `ſ -> s`, lowercase, punctuation cleanup
- `analysis_quantity_v1`: skeleton quantity/stress feature extraction

## Utility functions

- `normalize_unicode(form, mode)`
- `strip_diacritics(form)`
- `map_chars(form, mapping)`
- `normalize_apostrophes_primes(form)`
- `compute_features(form, rules)`
