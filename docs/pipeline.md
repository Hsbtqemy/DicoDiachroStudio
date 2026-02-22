# Pipeline

## Steps

1. Import raw files into `data/raw/imports` (text/pdf/url).
2. ABBYY-first path: import a text-selectable PDF (`import pdf-text`).
3. Optional template workshop:
   - preview/apply a record extraction template (`WORDLIST`, `ENTRY+DEF`, `HEADWORD+PRON`, `CSV_MAPPING`);
   - write extracted diplomatic entries + template issues.
4. Parse lines into structured entries (classic parser workflow).
5. Run QA with stable codes (`PAGE_MARKER`, `UNPARSED_LINE`, etc.).
6. Apply a YAML convention/profile to derive `headword_norm`, `pron_norm`, `pron_render`, `features_json`.
7. Persist entries and issues into SQLite.
8. Run comparison workshop (coverage + alignment + diff) and persist compare runs.
9. Export nomenclatures and comparison artifacts.

## Non-destructive rule

Raw diplomatic data is never overwritten. Derived forms are stored in separate DB columns.

## First import UX

- On the first import of a new project, if no active corpus exists, the GUI prompts for a corpus name.
- After confirmation, the corpus is created, set active automatically, and import resumes without extra setup.

## ABBYY-first PDF text import

- Supported input: PDF with a real text layer (typically exported after ABBYY OCR).
- CLI:
  - `dicodiachro import pdf-text <project_dir> <pdf_path> --two-columns`
  - optional convenience run: add `--dict-id ... --profile ... [--parser ...]`
- Extraction strategy:
  - source PDF is preserved in `data/raw` (non-destructive workflow);
  - extracted text is written to `data/raw/imports/*.txt`;
  - optional double-column mode reads words by coordinates and reconstructs lines
    left column first, then right column.
- If the PDF has no text layer, import fails with code `PDF_NO_TEXT_LAYER` and a clear message.
- GUI Import tab exposes the same flow with:
  - file picker,
  - CSV picker for mapping templates,
  - drag-and-drop PDF support,
  - `Double colonne` option,
  - explicit warning for TIFF/scan files requiring external OCR first.

## Template workshop

- Sources: imported `.txt` and `.csv` records.
- Preview analyzes up to `N` records (default `200`) and displays:
  - source text,
  - extracted `headword_raw` / `pron_raw` / `definition_raw`,
  - status (`OK`, `Ignoré`, `Non reconnu`) and reason.
- Apply stores template metadata and history:
  - active template per corpus in `corpus_templates`,
  - append-only run history in `template_applications`.
- Manual record-level overrides are stored in `entry_overrides` and replayed at apply time.

## Conventions workshop

- The `Conventions` tab follows: `Choisir -> Prévisualiser -> Appliquer -> Vérifier -> Affiner`.
- Preview shows:
  - raw/effective/norm/render columns,
  - feature summaries,
  - actionable QA alerts per row.
- Apply writes derived convention fields and records an application history row in
  `convention_applications` (profile id/version/hash + counts + status).

## Compare workshop

- Preview layers:
  - `Couverture`: union/communs/uniques par corpus.
  - `Alignement`: paires exact/fuzzy A-B avec score, méthode, non-alignés.
  - `Diff phonologique`: `pron_norm`, `pron_render`, `features`, deltas.
- Apply writes cached run tables:
  - `compare_runs`
  - `compare_coverage_items`
  - `compare_alignment_pairs`
  - `compare_diff_rows`

## Manual curation

- Entries table supports immediate manual curation:
  - edit fields into `headword_edit` / `pron_edit` / `definition_edit`,
  - split/merge operations,
  - reviewed/validated status.
- Every operation is logged (before/after JSON) in `entry_overrides`.
- Raw diplomatic fields remain intact; edited state is layered on top.
