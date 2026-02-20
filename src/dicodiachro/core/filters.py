from __future__ import annotations

from collections.abc import Iterable

ACCENTS = "áàâäéèêëíìîïóòôöúùûüýÿãõ"


def filter_by_accents(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    output = []
    for row in rows:
        value = row.get("form_display") or row.get("headword_raw") or ""
        if any(ch in ACCENTS for ch in value):
            output.append(row)
    return output


def filter_by_syllables(rows: Iterable[dict[str, str]], value: int) -> list[dict[str, str]]:
    output = []
    for row in rows:
        try:
            syllables = int(row.get("syllables") or 0)
        except ValueError:
            continue
        if syllables == value:
            output.append(row)
    return output


def filter_by_prefix(rows: Iterable[dict[str, str]], prefixes: list[str]) -> list[dict[str, str]]:
    output = []
    cleaned_prefixes = [str(prefix) for prefix in prefixes]
    for row in rows:
        value = str(row.get("form_norm") or row.get("headword_raw") or "")
        if any(value.startswith(prefix) for prefix in cleaned_prefixes):
            output.append(row)
    return output
