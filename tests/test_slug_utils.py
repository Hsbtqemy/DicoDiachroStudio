from __future__ import annotations

from dicodiachro.core.utils.slug import slugify, unique_slug


def test_slugify_normalizes_and_filters() -> None:
    assert slugify("The Spelling Dictionary (1737)") == "the_spelling_dictionary_1737"
    assert slugify("  Édition   spéciale  ") == "edition_speciale"


def test_unique_slug_with_collisions() -> None:
    existing = {"corpus", "corpus_2"}
    assert unique_slug("corpus", existing) == "corpus_3"


def test_unique_slug_fallback_when_empty() -> None:
    assert unique_slug("", set()) == "corpus_1"
    assert unique_slug("", {"corpus_1"}) == "corpus_2"
