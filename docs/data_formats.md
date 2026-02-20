# Data formats

## entries.csv / entries.jsonl

Columns:

- `entry_id`, `dict_id`, `section`, `syllables`
- `headword_raw`, `pos_raw`, `pron_raw`
- `form_display`, `form_norm`, `features_json`
- `source_path`, `line_no`, `created_at`

## comparison.xlsx

- `matrix` sheet: one row per lemma group, one column per dictionary.
- `metadata_flags` sheet: issue frequencies and export metadata.

## comparison.docx

- table or list relecture export for manual review.
