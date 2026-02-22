# Parsing Presets

## Goal

Parsing presets let you parse source lines with dictionary-specific patterns while keeping a fallback parser for legacy formats.

## Preset YAML schema

Minimum keys:

- `parser_id` (string)
- `version` (int)
- `entry_regex` (regex string)
- `fields` (mapping from semantic field to capture group index)

Optional keys:

- `pos_map` (`str -> str`)
- `origin_map` (`str -> str`)
- `allow_extra_trailing` (bool)

Allowed field names in `fields`:

- `syllables`
- `headword_raw`
- `pos_raw`
- `origin_raw`
- `pron_raw`

Example preset:

```yaml
parser_id: syll_headword_pos_origin_v1
version: 1
entry_regex: '^([1-9]|10)\s+(.+?),\s*([a-z]\.)\s*([A-Z]\.)\s*$'
fields:
  syllables: 1
  headword_raw: 2
  pos_raw: 3
  origin_raw: 4
pos_map:
  "f.": "ſ"
  "a.": "a"
  "v.": "v"
  "p.": "p"
origin_map:
  "L.": "Latin"
  "G.": "Greek"
  "F.": "French"
allow_extra_trailing: true
```

## Where to put presets

- Template: `rules/templates/parsers/syll_headword_pos_origin_v1.yml`
- Project-wide: `rules/parsers/*.yml`
- Dictionary-specific (recommended): `rules/<dict_id>/parser_v1.yml`

`dicodiachro run` will auto-pick a dictionary-specific preset from `rules/<dict_id>/` if `--parser` is not provided.

## CLI

- Validate preset:

```bash
dicodiachro parser validate rules/kittredge_1752/parser_v1.yml
```

- List presets discoverable for a dictionary:

```bash
dicodiachro parser list ./my_project --dict-id kittredge_1752
```

- Run with explicit preset:

```bash
dicodiachro run ./my_project --dict-id kittredge_1752 --profile reading_v1 --parser rules/kittredge_1752/parser_v1.yml
```

## Data model impact

Entries keep diplomatic raw fields and add parser metadata:

- `origin_raw`
- `origin_norm`
- `pos_norm`
- `parser_id`
- `parser_version`
- `parser_sha256`

Raw source text is never overwritten.
