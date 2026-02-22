from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from copy import deepcopy
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

DEFAULT_ACUTE_VOWELS = "áéíóúÁÉÍÓÚ"
SPACE_RE = re.compile(r"\s+")

ALLOWED_TOP_KEYS = {
    "profile_id",
    "version",
    "dict_id",
    "name",
    "description",
    "unicode",
    "display",
    "norm",
    "render",
    "output",
    "alignment",
    "features",
    "qa",
}
ALLOWED_UNICODE_KEYS = {"normalization"}
ALLOWED_DISPLAY_KEYS = {
    "normalize_primes",
    "keep_long_s",
    "collapse_spaces",
    "lowercase",
    "char_map",
    "allowed_extra_symbols",
}
ALLOWED_NORM_KEYS = {
    "normalize_primes",
    "lowercase",
    "strip_diacritics",
    "long_s_to_s",
    "remove_punctuation",
    "keep_hyphen",
    "collapse_spaces",
    "char_map",
    "allowed_extra_symbols",
}
ALLOWED_RENDER_KEYS = {
    "enabled",
    "source",
    "parenthesize_accented_vowel",
    "parenthesize_prime_segment",
    "open_paren",
    "close_paren",
    "collapse_spaces",
}
ALLOWED_FEATURE_KEYS = {
    "vowels",
    "marks",
    "rules",
    "stress_consistency",
    "stress_marks",
    "quantity_marks",
}
ALLOWED_QA_KEYS = {
    "enforce_stress_consistency",
    "require_prime_for_primary_stress",
    "require_acute_for_primary_stress",
    "require_pronunciation",
}
ALLOWED_MARK_KEYS = {"prime", "acute_vowels", "macron", "breve"}
ALLOWED_RULE_KEYS = {"when", "set"}
ALLOWED_WHEN_KEYS = {"contains", "contains_any", "pattern"}


class ProfileValidationError(ValueError):
    def __init__(self, errors: list[str], warnings: list[str] | None = None):
        self.errors = errors
        self.warnings = warnings or []
        message = "; ".join(errors)
        if self.warnings:
            message = f"{message} | warnings: {'; '.join(self.warnings)}"
        super().__init__(message)


class ProfileRuleRuntimeError(RuntimeError):
    pass


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


def _normalize_spaces(form: str) -> str:
    return SPACE_RE.sub(" ", form).strip()


def _remove_punctuation(form: str, keep_hyphen: bool) -> str:
    out: list[str] = []
    for ch in form:
        category = unicodedata.category(ch)
        if ch.isspace() or category.startswith(("L", "N", "M")):
            out.append(ch)
            continue
        if keep_hyphen and ch == "-":
            out.append(ch)
            continue
        out.append(" ")
    return "".join(out)


def canonicalize_profile_text(profile_yaml_text: str) -> str:
    return profile_yaml_text.replace("\r\n", "\n").replace("\r", "\n")


def profile_sha256(profile_yaml_text: str) -> str:
    normalized = canonicalize_profile_text(profile_yaml_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def profile_sha256_from_path(path: Path) -> str:
    return profile_sha256(path.read_text(encoding="utf-8"))


def _as_dict(value: Any, path: str, errors: list[str]) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        errors.append(f"{path} must be a mapping")
        return {}
    return dict(value)


def _push_unknown_keys(
    section: dict[str, Any],
    allowed: set[str],
    path: str,
    warnings: list[str],
    strict: bool,
    errors: list[str],
) -> None:
    for key in section:
        if key not in allowed:
            message = f"Unknown key: {path}.{key}"
            if strict:
                errors.append(message)
            else:
                warnings.append(message)


def _parse_profile_data(
    data: dict[str, Any],
    source_path: Path | None,
    strict: bool,
) -> ProfileSpec:
    errors: list[str] = []
    warnings: list[str] = []

    _push_unknown_keys(data, ALLOWED_TOP_KEYS, "profile", warnings, strict, errors)

    profile_id = data.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id.strip():
        errors.append("profile_id is required and must be a non-empty string")
        profile_id = source_path.stem if source_path else "profile"

    version_raw = data.get("version")
    version: int
    if isinstance(version_raw, int):
        version = version_raw
    elif isinstance(version_raw, str) and version_raw.strip().isdigit():
        version = int(version_raw.strip())
    else:
        errors.append("version is required and must be an integer")
        version = 1

    unicode_raw = data.get("unicode", {})
    if isinstance(unicode_raw, str):
        unicode_cfg = {"normalization": unicode_raw}
    else:
        unicode_cfg = _as_dict(unicode_raw, "unicode", errors)
    _push_unknown_keys(
        unicode_cfg,
        ALLOWED_UNICODE_KEYS,
        "unicode",
        warnings,
        strict,
        errors,
    )
    normalization = str(unicode_cfg.get("normalization", "NFC"))
    if normalization not in {"NFC", "NFD", "NFKC", "NFKD"}:
        errors.append("unicode.normalization must be one of NFC, NFD, NFKC, NFKD")

    display = _as_dict(data.get("display", {}), "display", errors)
    _push_unknown_keys(
        display,
        ALLOWED_DISPLAY_KEYS,
        "display",
        warnings,
        strict,
        errors,
    )

    norm_raw = data.get("norm")
    if norm_raw is None:
        norm_raw = data.get("alignment", {})
    norm = _as_dict(norm_raw, "norm", errors)
    if "remove_hyphens" in norm and "keep_hyphen" not in norm:
        # Backward compatibility with old alignment schema.
        norm["keep_hyphen"] = not bool(norm.get("remove_hyphens", True))
    _push_unknown_keys(norm, ALLOWED_NORM_KEYS, "norm", warnings, strict, errors)

    render_raw = data.get("render")
    if render_raw is None:
        render_raw = data.get("output", {})
    render = _as_dict(render_raw, "render", errors)
    _push_unknown_keys(
        render,
        ALLOWED_RENDER_KEYS,
        "render",
        warnings,
        strict,
        errors,
    )

    features = _as_dict(data.get("features", {}), "features", errors)
    _push_unknown_keys(
        features,
        ALLOWED_FEATURE_KEYS,
        "features",
        warnings,
        strict,
        errors,
    )

    marks = _as_dict(features.get("marks", {}), "features.marks", errors)
    _push_unknown_keys(
        marks,
        ALLOWED_MARK_KEYS,
        "features.marks",
        warnings,
        strict,
        errors,
    )
    for key, value in marks.items():
        if not isinstance(value, str):
            errors.append(f"features.marks.{key} must be a string")

    qa = _as_dict(data.get("qa", {}), "qa", errors)
    _push_unknown_keys(
        qa,
        ALLOWED_QA_KEYS,
        "qa",
        warnings,
        strict,
        errors,
    )
    qa_defaults = {
        "enforce_stress_consistency": False,
        "require_prime_for_primary_stress": False,
        "require_acute_for_primary_stress": False,
        "require_pronunciation": False,
    }
    qa_cfg: dict[str, bool] = {}
    for key, default in qa_defaults.items():
        value = qa.get(key, default)
        if not isinstance(value, bool):
            errors.append(f"qa.{key} must be a boolean")
            qa_cfg[key] = default
        else:
            qa_cfg[key] = value

    rules = features.get("rules", [])
    if rules is None:
        rules = []
    if not isinstance(rules, list):
        errors.append("features.rules must be a list")
    else:
        for idx, rule in enumerate(rules):
            if not isinstance(rule, dict):
                errors.append(f"features.rules[{idx}] must be a mapping")
                continue
            _push_unknown_keys(
                rule,
                ALLOWED_RULE_KEYS,
                f"features.rules[{idx}]",
                warnings,
                strict,
                errors,
            )
            when = _as_dict(rule.get("when", {}), f"features.rules[{idx}].when", errors)
            _push_unknown_keys(
                when,
                ALLOWED_WHEN_KEYS,
                f"features.rules[{idx}].when",
                warnings,
                strict,
                errors,
            )
            rule_set = rule.get("set")
            if not isinstance(rule_set, dict):
                errors.append(f"features.rules[{idx}].set must be a mapping")
            pattern = when.get("pattern")
            if pattern is not None:
                if not isinstance(pattern, str):
                    errors.append(f"features.rules[{idx}].when.pattern must be a string")
                else:
                    try:
                        re.compile(pattern)
                    except re.error as exc:
                        errors.append(f"features.rules[{idx}].when.pattern invalid regex: {exc}")

    if qa_cfg.get("enforce_stress_consistency", False):
        has_marks = isinstance(features.get("marks"), dict)
        has_prime = bool(marks.get("prime")) if has_marks else False
        has_acute = bool(marks.get("acute_vowels")) if has_marks else False
        if qa_cfg.get("require_prime_for_primary_stress", False) and not has_prime:
            warnings.append(
                "qa.enforce_stress_consistency enabled but features.marks.prime missing; "
                "prime consistency check will be disabled"
            )
        if qa_cfg.get("require_acute_for_primary_stress", False) and not has_acute:
            warnings.append(
                "qa.enforce_stress_consistency enabled but features.marks.acute_vowels missing; "
                "acute consistency check will be disabled"
            )

    if errors:
        raise ProfileValidationError(errors=errors, warnings=warnings)

    return ProfileSpec(
        profile_id=profile_id,
        version=version,
        dict_id=str(data.get("dict_id")) if data.get("dict_id") else None,
        name=str(data.get("name") or profile_id),
        description=str(data.get("description") or ""),
        unicode_normalization=normalization,
        display=display,
        norm=norm,
        render=render,
        features=features,
        qa=qa_cfg,
        validation_warnings=warnings,
        source_path=str(source_path) if source_path else None,
    )


def load_profile(path: Path, strict: bool = False) -> ProfileSpec:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ProfileValidationError([f"Unable to read profile: {exc}"]) from exc

    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise ProfileValidationError([f"Invalid YAML: {exc}"]) from exc

    if not isinstance(data, dict):
        raise ProfileValidationError(["Profile YAML root must be a mapping"])

    return _parse_profile_data(data, source_path=path, strict=strict)


def profile_to_payload(profile: ProfileSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "profile_id": profile.profile_id,
        "name": profile.name or profile.profile_id,
        "version": int(profile.version),
        "description": profile.description or "",
        "unicode": {
            "normalization": profile.unicode_normalization,
        },
        "display": dict(profile.display),
        "norm": dict(profile.norm),
        "render": dict(profile.render),
        "features": deepcopy(profile.features),
        "qa": dict(profile.qa),
    }
    if profile.dict_id:
        payload["dict_id"] = profile.dict_id
    return payload


def profile_to_yaml_text(profile: ProfileSpec) -> str:
    payload = profile_to_payload(profile)
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)


def build_profile_from_ui(
    base_profile: ProfileSpec,
    ui_settings: dict[str, Any],
) -> ProfileSpec:
    settings = dict(ui_settings)
    display = dict(base_profile.display)
    norm = dict(base_profile.norm)
    render = dict(base_profile.render)
    features = deepcopy(base_profile.features) if isinstance(base_profile.features, dict) else {}
    qa = dict(base_profile.qa)

    def _get_bool(key: str, default: bool) -> bool:
        value = settings.get(key, default)
        return value if isinstance(value, bool) else default

    normalize_spaces = _get_bool(
        "normalize_spaces",
        bool(norm.get("collapse_spaces", display.get("collapse_spaces", True))),
    )
    norm["long_s_to_s"] = _get_bool("long_s_to_s", bool(norm.get("long_s_to_s", False)))
    norm["lowercase"] = _get_bool("lowercase", bool(norm.get("lowercase", True)))
    norm["strip_diacritics"] = _get_bool(
        "strip_diacritics", bool(norm.get("strip_diacritics", False))
    )
    norm["remove_punctuation"] = _get_bool(
        "remove_punctuation",
        bool(norm.get("remove_punctuation", False)),
    )
    norm["collapse_spaces"] = normalize_spaces
    display["collapse_spaces"] = normalize_spaces

    qa["require_pronunciation"] = _get_bool(
        "require_pronunciation",
        bool(qa.get("require_pronunciation", False)),
    )
    qa["enforce_stress_consistency"] = _get_bool(
        "enforce_stress_consistency",
        bool(qa.get("enforce_stress_consistency", False)),
    )

    stress_mode = str(settings.get("stress_mode", "both")).strip().lower()
    if stress_mode == "prime":
        qa["require_prime_for_primary_stress"] = True
        qa["require_acute_for_primary_stress"] = False
    elif stress_mode == "acute":
        qa["require_prime_for_primary_stress"] = False
        qa["require_acute_for_primary_stress"] = True
    else:
        qa["require_prime_for_primary_stress"] = True
        qa["require_acute_for_primary_stress"] = True

    render_mode = str(settings.get("render_mode", "none")).strip().lower()
    if render_mode == "none":
        render["enabled"] = False
        render["parenthesize_accented_vowel"] = False
        render["parenthesize_prime_segment"] = False
    elif render_mode == "accent":
        render["enabled"] = True
        render["source"] = "norm"
        render["parenthesize_accented_vowel"] = True
        render["parenthesize_prime_segment"] = False
    elif render_mode == "prime":
        render["enabled"] = True
        render["source"] = "display"
        render["parenthesize_accented_vowel"] = False
        render["parenthesize_prime_segment"] = True

    profile_id = str(settings.get("profile_id") or base_profile.profile_id).strip()
    name = str(settings.get("name") or base_profile.name or profile_id).strip()
    description = str(settings.get("description") or base_profile.description or "").strip()
    version_raw = settings.get("version", base_profile.version)
    version = int(version_raw) if isinstance(version_raw, int) else int(base_profile.version)

    return ProfileSpec(
        profile_id=profile_id or base_profile.profile_id,
        version=version,
        dict_id=base_profile.dict_id,
        name=name or profile_id or base_profile.profile_id,
        description=description,
        unicode_normalization=base_profile.unicode_normalization,
        display=display,
        norm=norm,
        render=render,
        features=features,
        qa=qa,
        validation_warnings=list(base_profile.validation_warnings),
        source_path=base_profile.source_path,
    )


def _apply_display(base: str, profile: ProfileSpec) -> str:
    cfg = profile.display
    out = base
    if cfg.get("normalize_primes", True):
        out = normalize_apostrophes_primes(out)
    if not cfg.get("keep_long_s", True):
        out = out.replace("ſ", "s")
    out = map_chars(out, cfg.get("char_map"))
    if cfg.get("lowercase", False):
        out = out.lower()
    if cfg.get("collapse_spaces", True):
        out = _normalize_spaces(out)
    return out


def _apply_norm(base: str, profile: ProfileSpec) -> str:
    cfg = profile.norm
    out = base
    if cfg.get("normalize_primes", True):
        out = normalize_apostrophes_primes(out)
    if cfg.get("long_s_to_s", False):
        out = out.replace("ſ", "s")
    out = map_chars(out, cfg.get("char_map"))
    if cfg.get("strip_diacritics", False):
        out = strip_diacritics(out)
    if cfg.get("lowercase", True):
        out = out.lower()
    if cfg.get("remove_punctuation", False):
        out = _remove_punctuation(out, keep_hyphen=bool(cfg.get("keep_hyphen", False)))
    if cfg.get("collapse_spaces", True):
        out = _normalize_spaces(out)
    return out


def _parenthesize_prime_segments(
    form: str,
    *,
    prime: str,
    open_paren: str,
    close_paren: str,
) -> str:
    if not form or not prime:
        return form

    escaped_prime = re.escape(prime)
    paired = re.sub(
        rf"{escaped_prime}(.+?){escaped_prime}",
        lambda match: f"{open_paren}{match.group(1)}{close_paren}",
        form,
    )
    single = re.sub(
        rf"(\S){escaped_prime}",
        lambda match: f"{open_paren}{match.group(1)}{close_paren}",
        paired,
    )
    return single.replace(prime, "")


def _apply_render(display: str, norm: str, profile: ProfileSpec) -> str:
    cfg = profile.render if isinstance(profile.render, dict) else {}
    if not cfg.get("enabled", True):
        return norm

    source = str(cfg.get("source", "norm")).strip().lower()
    out = display if source == "display" else norm

    open_paren = str(cfg.get("open_paren", "("))
    close_paren = str(cfg.get("close_paren", ")"))

    marks = profile.features.get("marks", {}) if isinstance(profile.features, dict) else {}
    prime = str(marks.get("prime", "ʹ")) if isinstance(marks, dict) else "ʹ"
    acute_vowels = (
        str(marks.get("acute_vowels", DEFAULT_ACUTE_VOWELS))
        if isinstance(marks, dict)
        else DEFAULT_ACUTE_VOWELS
    )

    if cfg.get("parenthesize_prime_segment", False):
        out = _parenthesize_prime_segments(
            out,
            prime=prime,
            open_paren=open_paren,
            close_paren=close_paren,
        )

    if cfg.get("parenthesize_accented_vowel", False):
        acute_set = set(acute_vowels)
        out = "".join(
            f"{open_paren}{char}{close_paren}" if char in acute_set else char for char in out
        )

    if cfg.get("collapse_spaces", True):
        out = _normalize_spaces(out)
    return out


def _count_detached_combining_marks(form: str) -> int:
    detached = 0
    for idx, char in enumerate(form):
        if not unicodedata.combining(char):
            continue
        if idx == 0:
            detached += 1
            continue
        prev = form[idx - 1]
        if prev.isspace() or unicodedata.combining(prev):
            detached += 1
    return detached


def _symbols_used(form: str) -> list[str]:
    symbols = {
        ch
        for ch in form
        if not ch.isspace() and (not ch.isalnum() or bool(unicodedata.combining(ch)))
    }
    return sorted(symbols)


def _build_allowed_symbols(
    profile: ProfileSpec, symbols_inventory: set[str]
) -> tuple[set[str], set[str]]:
    raw_marks = profile.features.get("marks", {})
    marks = raw_marks if isinstance(raw_marks, dict) else {}

    allowed_symbols = {"ʹ", "ſ", "-", *symbols_inventory}
    allowed_symbols.update({str(ch) for ch in profile.display.get("allowed_extra_symbols", [])})
    allowed_symbols.update({str(ch) for ch in profile.norm.get("allowed_extra_symbols", [])})

    for key in ["prime", "acute_vowels", "macron", "breve"]:
        value = marks.get(key)
        if isinstance(value, str):
            allowed_symbols.update(value)

    for mapping in [profile.display.get("char_map"), profile.norm.get("char_map")]:
        if isinstance(mapping, dict):
            for from_char, to_char in mapping.items():
                allowed_symbols.update(str(from_char))
                allowed_symbols.update(str(to_char))

    allowed_combining = {ch for ch in allowed_symbols if unicodedata.combining(ch)}
    return allowed_symbols, allowed_combining


def _unknown_symbols(
    form: str,
    profile: ProfileSpec,
    symbols_inventory: set[str] | None,
) -> list[str]:
    symbols_inventory = symbols_inventory or set()
    allowed_symbols, allowed_combining = _build_allowed_symbols(profile, symbols_inventory)

    unknown: set[str] = set()
    for char in form:
        if char.isspace() or char.isalnum():
            continue
        category = unicodedata.category(char)
        if category.startswith(("L", "N")):
            continue
        if unicodedata.combining(char):
            if char in allowed_combining:
                continue
            unknown.add(char)
            continue
        if char not in allowed_symbols:
            unknown.add(char)

    return sorted(unknown)


def _primary_stress_count(form: str, prime: str, acute_vowels: str) -> int:
    anchors: set[int] = set()
    acute_set = set(acute_vowels)
    for idx, char in enumerate(form):
        if char in acute_set:
            anchors.add(idx)
    for idx, char in enumerate(form):
        if char != prime:
            continue
        anchor = max(0, idx - 1)
        anchors.add(anchor)
    return len(anchors)


def _evaluate_rule(text: str, when: dict[str, Any]) -> bool:
    if "contains" in when:
        value = str(when["contains"])
        if value not in text:
            return False
    if "contains_any" in when:
        values = str(when["contains_any"])
        if not any(ch in text for ch in values):
            return False
    if "pattern" in when:
        pattern = str(when["pattern"])
        if not re.search(pattern, text):
            return False
    return True


def compute_features(form: str, rules: dict[str, Any] | None) -> dict[str, Any]:
    if not rules:
        return {}

    features: dict[str, Any] = {}
    marks = rules.get("marks", {}) if isinstance(rules, dict) else {}

    prime = str(marks.get("prime", "ʹ"))
    acute_vowels = str(marks.get("acute_vowels", DEFAULT_ACUTE_VOWELS))

    features["has_prime"] = prime in form
    features["prime_count"] = form.count(prime)
    features["accented_vowel_count"] = sum(1 for ch in form if ch in set(acute_vowels))
    features["has_accented_vowel"] = bool(features["accented_vowel_count"])
    features["combining_detached_count"] = _count_detached_combining_marks(form)
    features["primary_stress_count"] = _primary_stress_count(form, prime, acute_vowels)

    # Backward compatibility with the previous schema.
    stress_marks = set(rules.get("stress_marks", [])) if isinstance(rules, dict) else set()
    if stress_marks:
        features["stress"] = "primary" if any(mark in form for mark in stress_marks) else "none"

    quantity_marks = rules.get("quantity_marks", {}) if isinstance(rules, dict) else {}
    if isinstance(quantity_marks, dict) and quantity_marks:
        quantity_values = [label for mark, label in quantity_marks.items() if mark in form]
        if quantity_values:
            features["quantity"] = quantity_values

    for rule in rules.get("rules", []) if isinstance(rules, dict) else []:
        when = rule.get("when", {}) if isinstance(rule, dict) else {}
        set_values = rule.get("set", {}) if isinstance(rule, dict) else {}
        if not isinstance(when, dict) or not isinstance(set_values, dict):
            continue
        if _evaluate_rule(form, when):
            for key, value in set_values.items():
                features[key] = value

    # Deterministic output for logs/exports.
    return json.loads(json.dumps(features, ensure_ascii=False, sort_keys=True))


def _stress_inconsistent(features: dict[str, Any], profile: ProfileSpec) -> bool:
    enforce_cfg = bool(profile.qa.get("enforce_stress_consistency", False))
    if not enforce_cfg:
        return False

    marks = profile.features.get("marks")
    if not isinstance(marks, dict):
        return False

    check_prime = bool(profile.qa.get("require_prime_for_primary_stress", False))
    check_acute = bool(profile.qa.get("require_acute_for_primary_stress", False))

    if check_prime and not isinstance(marks.get("prime"), str):
        check_prime = False
    if check_acute and not isinstance(marks.get("acute_vowels"), str):
        check_acute = False

    if not check_prime and not check_acute:
        return False

    prime_count = int(features.get("prime_count", 0) or 0)
    accented = int(features.get("accented_vowel_count", 0) or 0)

    if check_prime and accented > 0 and prime_count == 0:
        return True
    if check_acute and prime_count > 0 and accented == 0:
        return True
    return False


def apply_profile(
    form_raw: str,
    profile: ProfileSpec,
    symbols_inventory: set[str] | None = None,
) -> ProfileApplied:
    base = normalize_unicode(form_raw, profile.unicode_normalization)

    display = _apply_display(base, profile)
    norm = _apply_norm(base, profile)
    render = _apply_render(display, norm, profile)

    features = compute_features(display, profile.features)
    symbols_used = _symbols_used(display)
    unknown_symbols = _unknown_symbols(display, profile, symbols_inventory)

    warnings: list[str] = []
    if unknown_symbols:
        warnings.append("UNKNOWN_SYMBOL")
    if int(features.get("combining_detached_count", 0) or 0) > 0:
        warnings.append("DETACHED_COMBINING_MARK")
    if int(features.get("primary_stress_count", 0) or 0) > 1:
        warnings.append("MULTIPLE_PRIMARY_STRESS")
    if _stress_inconsistent(features, profile):
        warnings.append("INCONSISTENT_STRESS")

    features["symbols_used"] = symbols_used
    features["unknown_symbols"] = unknown_symbols
    features = json.loads(json.dumps(features, ensure_ascii=False, sort_keys=True))

    return ProfileApplied(
        form_display=display,
        form_norm=norm,
        form_render=render,
        features=features,
        symbols_used=symbols_used,
        unknown_symbols=unknown_symbols,
        warnings=sorted(set(warnings)),
    )


def load_symbols_inventory(project_rules_dir: Path, dict_id: str | None = None) -> set[str]:
    candidates: list[Path] = []
    if dict_id:
        candidates.extend(
            [
                project_rules_dir / dict_id / "symbols.yml",
                project_rules_dir / dict_id / "symbols.yaml",
            ]
        )
    candidates.extend(
        [
            project_rules_dir / "symbols.yml",
            project_rules_dir / "symbols.yaml",
            Path("rules") / "templates" / "symbols.example.yml",
            Path(__file__).resolve().parents[3] / "rules" / "templates" / "symbols.example.yml",
        ]
    )

    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError):
            continue

        if isinstance(data, dict):
            symbols = data.get("symbols")
            if isinstance(symbols, dict):
                return set(symbols.keys())
            if isinstance(symbols, list):
                return {str(item) for item in symbols}

    return set()
