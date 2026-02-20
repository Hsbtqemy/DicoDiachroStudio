# Pipeline

## Steps

1. Import raw files into `data/raw/imports` (text/pdf/url).
2. Parse lines into structured entries.
3. Run QA with stable codes (`PAGE_MARKER`, `UNPARSED_LINE`, etc.).
4. Apply YAML transcription profile to derive `form_display`, `form_norm`, `features_json`.
5. Persist entries and issues into SQLite.
6. Export nomenclatures and comparison artifacts.

## Non-destructive rule

Raw diplomatic data is never overwritten. Derived forms are stored in separate DB columns.

## TODO

- eScriptorium/PAGE-XML importer.
- richer coords-based PDF segmentation presets.
