# Architecture

## Layers

- `src/dicodiachro/core`: parsing, QA, profiles, storage, importers, exporters, alignment.
- `src/dicodiachro/cli`: Typer commands for project lifecycle and batch workflows.
- `studio/dicodiachro_studio`: PySide6 GUI (tabbed desktop app).
- `legacy/`: historical scripts kept unchanged for traceability only.

## Data model (SQLite)

- `dictionaries`, `profiles`, `entries`, `issues`
- `lemma_groups`, `lemma_members`, `comparison_sessions`

## Determinism

- `entry_id` is hash-based on source coordinates + lexical raw fields.
- profile files are hashed (SHA256) and persisted.
- logs are append-only JSONL under `logs/`.
