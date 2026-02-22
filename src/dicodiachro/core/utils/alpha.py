from __future__ import annotations

import unicodedata


def alpha_bucket_of(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return "#"

    for char in value:
        if not char.isalpha():
            continue
        normalized = unicodedata.normalize("NFD", char)
        base = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        if not base:
            return "#"
        letter = base[0].upper()
        if "A" <= letter <= "Z":
            return letter
        return "#"
    return "#"
