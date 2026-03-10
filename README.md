# DicoDiachro

DicoDiachro Studio is a local-first desktop/CLI toolkit for building diachronic nomenclatures from historical dictionaries (18th-21st centuries), with parsing, QA, profile-driven transcription, alignment, multi-comparison, and exports.

## Install

```bash
pip install -e .
```

## Run

```bash
dicodiachro --help
python -m dicodiachro_studio
```

## Quickstart CLI

```bash
dicodiachro init ./my_project
dicodiachro import text ./my_project ./sample_data/sample_input.txt
dicodiachro import csv ./my_project ./sample_data/sample_entries.csv
dicodiachro import pdf-text ./my_project ./my_abbyy_export.pdf --two-columns
dicodiachro run ./my_project --dict-id dict_001 --profile reading_v1
dicodiachro parser validate ./rules/kittredge_1752/parser_v1.yml
dicodiachro run ./my_project --dict-id kittredge_1752 --profile reading_v1 --parser ./rules/kittredge_1752/parser_v1.yml
dicodiachro template preview ./my_project --corpus dict_001 --kind wordlist_tokens
dicodiachro template apply ./my_project --corpus dict_001 --kind wordlist_tokens --apply-profile reading_v1
dicodiachro convention preview ./my_project --corpus dict_001 --profile analysis_quantity_v1 --limit 200
dicodiachro convention apply ./my_project --corpus dict_001 --profile analysis_quantity_v1
dicodiachro compare preview ./my_project --corpus dict_001 --corpus dict_002 --mode exact+fuzzy --threshold 90
dicodiachro compare apply ./my_project --corpus dict_001 --corpus dict_002 --mode exact+fuzzy --threshold 90
dicodiachro compare runs ./my_project
dicodiachro export entries ./my_project --dict-id dict_001 --format csv
dicodiachro import nomenclature ./my_project ./my_project/data/derived/dict_001.csv --dict-id dict_002
```

## Project structure

A project folder contains:

- `project.sqlite`
- `data/raw` raw imports (never overwritten)
- `data/interim` intermediate merged/restructured text
- `data/derived` nomenclatures and exports
- `rules` profile YAML and symbol inventory
- `logs` JSONL run/import logs

## Core principles

- Diplomatic layer preserved (`headword_raw`, `pron_raw`).
- All retranscription choices are profile-driven (`rules/*.yml`).
- Reproducibility: profile version + SHA256 tracked in DB and logs.
- Cross-platform: `pathlib`, UTF-8, no hardcoded paths.

## Import files in GUI

Import tab now supports beginner-friendly local workflows:

- At first import on a new project, DicoDiachro asks for a corpus name once, creates it, selects it, and continues automatically.
- `Parcourir…` selectors for TXT/folder/PDF (no manual path typing).
- CSV import is available in the same tab (`Importer CSV`) for template mapping workflows.
- Dedicated drag-and-drop area: drop a file and choose the action.
- PDF flow: import a text-selectable PDF exported after external OCR (ABBYY-first).
- Quick actions:
  - `Ouvrir dossier`
  - `Révéler` (Finder/Explorer; Linux opens containing folder)
- URL import keeps a simple URL field and stores downloads automatically in the project.
- Dropped TIFF/scan files are rejected with a clear message: run external OCR and re-export as PDF text.

## Source paratext filtering

- Use `rules/source_filters/source_filters.yml` to exclude front/back matter (preface, intro, bibliography, notes) from analysis.
- Dictionary-specific override is supported with `rules/<dict_id>/source_filters.yml`.
- Filters are applied automatically on text sources in:
  - `dicodiachro run` (pipeline parse + QA + profile)
  - Template workshop preview/apply (`template preview`, `template apply`, GUI Atelier)
- Supported controls:
  - `exclude_line_ranges` (1-based inclusive ranges, for example `1-120`)
  - `drop_before_regex` (drop everything before first match)
  - `drop_after_regex` (drop from first match to end)
  - `drop_line_regexes` (drop matching lines only)
- Run summaries now include a `source_filters` block with dropped/kept line counts, so you can verify filtering is really active.

## Template workshop (Atelier)

- The `Atelier` tab provides a guided cycle: `Choisir gabarit -> Prévisualiser -> Appliquer -> Vérifier`.
- Core template kinds:
  - `Liste de mots` (`WORDLIST_TOKENS`)
  - `Entrée + définition` (`ENTRY_PLUS_DEFINITION`)
  - `Mot + prononciation` (`HEADWORD_PLUS_PRON`)
  - `CSV (mapping)` (`CSV_MAPPING`)
- Templates are versioned per corpus in SQLite (`corpus_templates`, `template_applications`) with hash, params, counts, and timestamp.
- Template extraction writes diplomatic fields (`headword_raw`, `pron_raw`, `definition_raw`) and keeps profile conventions separate.

## Manual curation (overrides)

- Atelier preview supports record-level overrides: `Skip`, `Split`, `Edit`, `Undo`.
- Entries tab supports entry-level actions: add, inline edit, split, merge, reviewed/validated status.
- Entries tab also provides `Undo override` based on override history.
- Non-recognized records in the template workshop can directly create audited entries (`CREATE_ENTRY`).
- Manual actions are applied immediately and logged in SQLite (`entry_overrides`) for audit/replay.
- Exports include both diplomatic fields and effective edited fields (`*_effective`).

## Conventions workshop

- The `Conventions` tab provides: `Choisir -> Prévisualiser -> Appliquer -> Vérifier -> Affiner`.
- Preview displays `raw`, `effective`, `norm`, `render`, feature keys, and QA alerts before write.
- Applying a convention stores `headword_norm`, `pron_norm`, `pron_render`, and `features_json`.
- Convention runs are tracked in SQLite (`convention_applications`) with profile id/version/hash.
- Exports include convention outputs, including `pron_render`.

## Compare workshop

- The `Compare` tab follows: `Choisir -> Prévisualiser -> Appliquer -> Vérifier -> Exporter`.
- Coverage view shows presence/absence matrix across 2+ corpora.
- Alignment view supports `exact` and `exact+fuzzy` on normalized headwords with score/method.
- Diff view highlights `pron_render`, `pron_norm`, and feature deltas on aligned pairs.
- Applied runs are persisted and replayable via `compare_runs` + `compare_*` tables.

## ABBYY-first workflow

1. OCR is done upstream (ABBYY or equivalent).
2. Export a PDF with a selectable text layer.
3. Import in one command:

```bash
dicodiachro import pdf-text ./my_project ./my_abbyy_export.pdf --two-columns
```

4. DicoDiachro extracts text to `data/raw/imports/*.txt` and keeps the source PDF untouched.
5. Run pipeline separately or in one step:

```bash
dicodiachro import pdf-text ./my_project ./my_abbyy_export.pdf --dict-id dict_001 --profile reading_v1
```

If a PDF has no text layer, CLI/GUI stop with:
`PDF sans couche texte. Passez-le par ABBYY (ou un OCR) puis réessayez.`

## Parsing presets

- Presets are YAML configs for line parsing by dictionary.
- Put dictionary-specific presets in `rules/<dict_id>/` (example: `rules/kittredge_1752/parser_v1.yml`).
- Commands:
  - `dicodiachro parser list <project_dir> --dict-id <dict_id>`
  - `dicodiachro parser validate <preset_path>`
- `dicodiachro run` and `dicodiachro import pdf-text` accept `--parser <path|id>`.
- If `--parser` is omitted, DicoDiachro auto-loads a preset from `rules/<dict_id>/` when available, otherwise falls back to the legacy regex parser.

## Legacy scripts

Original workflow scripts are preserved in `legacy/` for traceability. New processing goes through `dicodiachro.core`.
