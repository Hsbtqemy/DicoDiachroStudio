from __future__ import annotations

import re
import unicodedata

NON_WORD_RE = re.compile(r"[^a-z0-9_]+")
SPACE_RE = re.compile(r"\s+")
UNDERSCORE_RE = re.compile(r"_+")


def slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text or "")
    without_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    lowered = without_accents.lower()
    collapsed_spaces = SPACE_RE.sub("_", lowered)
    cleaned = NON_WORD_RE.sub("_", collapsed_spaces)
    collapsed_underscores = UNDERSCORE_RE.sub("_", cleaned)
    return collapsed_underscores.strip("_")


def unique_slug(base: str, existing: set[str], fallback_prefix: str = "corpus") -> str:
    base_slug = slugify(base)
    if not base_slug:
        counter = 1
        while True:
            candidate = f"{fallback_prefix}_{counter}"
            if candidate not in existing:
                return candidate
            counter += 1
    if base_slug not in existing:
        return base_slug

    counter = 2
    while True:
        candidate = f"{base_slug}_{counter}"
        if candidate not in existing:
            return candidate
        counter += 1
