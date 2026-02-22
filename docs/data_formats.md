# Data formats

## entries.csv / entries.jsonl

Columns:

- `entry_id`, `dict_id`, `section`, `syllables`
- `headword_raw`, `pos_raw`, `pron_raw`
- `headword_edit`, `pron_edit`, `definition_edit`, `status`
- `headword_effective`, `pron_effective`, `definition_effective`
- `definition_raw` (if template extraction provided a definition segment)
- `source_record` (original record snapshot used by template extraction)
- `source_id`, `record_key` (record-level provenance for overrides replay)
- `template_id`, `template_version`, `template_sha256` (template provenance)
- `form_display`, `form_norm`, `features_json`
- `form_display_effective`, `form_norm_effective`
- `headword_norm`, `pron_norm`, `pron_render`
- `profile_id`, `profile_version`, `profile_sha256` (last convention/profile applied)
- `source_path`, `line_no`, `created_at`

## comparison.xlsx

- `matrix` sheet: one row per lemma group, one column per dictionary.
- `metadata_flags` sheet: issue frequencies and export metadata.

## comparison.docx

- table or list relecture export for manual review.

## compare workshop exports

- `coverage.csv|xlsx`: `headword_key` + `in_<corpus>` flags + metadata.
- `alignment.csv|xlsx`: paires exact/fuzzy, score, méthode, statuts.
- `diff.csv|xlsx`: `pron_render`, `pron_norm`, `features`, `delta` par paire alignée.
