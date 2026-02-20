from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import yaml

from .models import ProfileApplied, ProfileSpec

APOSTROPHE_MAP = {
    "'": "ʹ",
    "’": "ʹ",
    "ʼ": "ʹ",
    "`": "ʹ",
    "´": "ʹ",
}

PUNCT_RE = re.compile(r"[\.,;:!?\(\)\[\]\{\}\"“”«»]")
SPACE_RE = re.compile(r"\s+")


def normalize_unicode(form: str, mode: str = "NFC") -> str:
    if mode not in {"NFC", "NFD", "NFKC", "NFKD"}:
        raise ValueError(f"Unsupported Unicode normalization mode: {mode}")
    return unicodedata.normalize(mode, form)


def strip_diacritics(form: str) -> str:
    decomposed = unicodedata.normalize("NFD", form)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return unicodedata.normalize("NFC", stripped)


def map_chars(form: str, mapping: dict[str, str] | None) -> str:
    if not mapping:
        return form
    return "".join(mapping.get(ch, ch) for ch in form)


def normalize_apostrophes_primes(form: str) -> str:
    return "".join(APOSTROPHE_MAP.get(ch, ch) for ch in form)


def _remove_punctuation(form: str, remove_hyphens: bool) -> str:
    out = PUNCT_RE.sub(" ", form)
    if remove_hyphens:
        out = out.replace("-", " ")
    return out


def _normalize_spaces(form: str) -> str:
    return SPACE_RE.sub(" ", form).strip()


def compute_features(form: str, rules: dict[str, Any] | None) -> dict[str, Any]:
    if not rules:
        return {}

    features: dict[str, Any] = {}
    stress_marks = set(rules.get("stress_marks", []))
    quantity_marks = rules.get("quantity_marks", {})

    if stress_marks:
        features["stress"] = any(mark in form for mark in stress_marks)
    if quantity_marks:
        quantity_values = [label for mark, label in quantity_marks.items() if mark in form]
        if quantity_values:
            features["quantity"] = quantity_values

    # Deterministic output for logs/exports.
    return json.loads(json.dumps(features, sort_keys=True))


def load_profile(path: Path) -> ProfileSpec:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    profile_id = str(data.get("profile_id") or path.stem)
    name = str(data.get("name") or profile_id)
    version = str(data.get("version") or "1")
    dict_id = data.get("dict_id")
    description = str(data.get("description") or "")

    return ProfileSpec(
        profile_id=profile_id,
        dict_id=str(dict_id) if dict_id else None,
        name=name,
        version=version,
        description=description,
        unicode_mode=str(data.get("unicode", "NFC")),
        display=dict(data.get("display") or {}),
        alignment=dict(data.get("alignment") or {}),
        features_rules=dict(data.get("features") or {}),
    )


def profile_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def apply_profile(form_raw: str, profile: ProfileSpec) -> ProfileApplied:
    display_cfg = profile.display
    align_cfg = profile.alignment

    display = normalize_unicode(form_raw, display_cfg.get("unicode", profile.unicode_mode))
    if display_cfg.get("normalize_primes", True):
        display = normalize_apostrophes_primes(display)
    display = map_chars(display, display_cfg.get("char_map"))
    if display_cfg.get("lowercase", False):
        display = display.lower()
    if display_cfg.get("collapse_spaces", True):
        display = _normalize_spaces(display)

    norm = normalize_unicode(form_raw, align_cfg.get("unicode", profile.unicode_mode))
    if align_cfg.get("normalize_primes", True):
        norm = normalize_apostrophes_primes(norm)
    norm = map_chars(norm, align_cfg.get("char_map"))
    if align_cfg.get("strip_diacritics", False):
        norm = strip_diacritics(norm)
    if align_cfg.get("lowercase", True):
        norm = norm.lower()
    if align_cfg.get("remove_punctuation", False):
        norm = _remove_punctuation(norm, bool(align_cfg.get("remove_hyphens", True)))
    if align_cfg.get("collapse_spaces", True):
        norm = _normalize_spaces(norm)

    features = compute_features(display, profile.features_rules)
    return ProfileApplied(form_display=display, form_norm=norm, features=features)
