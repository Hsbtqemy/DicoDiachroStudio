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
dicodiachro run ./my_project --dict-id dict_001 --profile reading_v1
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

## Legacy scripts

Original workflow scripts are preserved in `legacy/` for traceability. New processing goes through `dicodiachro.core`.
