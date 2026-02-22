from pathlib import Path

import pytest

from dicodiachro.core.profiles import (
    ProfileValidationError,
    apply_profile,
    build_profile_from_ui,
    load_profile,
    profile_sha256,
    profile_to_yaml_text,
)


def test_profile_load_validate_ok() -> None:
    profile = load_profile(Path("tests/data/profile_valid.yml"))
    assert profile.profile_id == "test_profile"
    assert profile.version == 1
    assert profile.unicode_normalization == "NFC"


def test_profile_hash_stable() -> None:
    yaml_lf = "profile_id: p\nversion: 1\nunicode:\n  normalization: NFC\n"
    yaml_crlf = yaml_lf.replace("\n", "\r\n")

    assert profile_sha256(yaml_lf) == profile_sha256(yaml_crlf)
    assert profile_sha256(yaml_lf) != profile_sha256(yaml_lf + " ")


def test_profile_invalid_raises() -> None:
    with pytest.raises(ProfileValidationError):
        load_profile(Path("tests/data/profile_invalid.yml"))


def test_apply_alignment_strips_diacritics_and_long_s() -> None:
    profile = load_profile(Path("rules/templates/alignment_v1.yml"))
    applied = apply_profile("Júvénaſ", profile)
    assert applied.form_norm == "juvenas"


def test_features_primary_stress_detected() -> None:
    profile = load_profile(Path("tests/data/profile_valid.yml"))
    applied = apply_profile("áʹlendar", profile)
    assert applied.features.get("stress") == "primary"
    assert applied.features.get("primary_stress_count") == 1


def test_build_profile_from_ui_generates_valid_yaml(tmp_path: Path) -> None:
    base = load_profile(Path("rules/templates/reading_v1.yml"))
    ui_profile = build_profile_from_ui(
        base,
        {
            "profile_id": "reading_ui_test",
            "name": "Reading UI Test",
            "description": "Generated from UI controls",
            "long_s_to_s": True,
            "lowercase": True,
            "strip_diacritics": True,
            "normalize_spaces": True,
            "remove_punctuation": True,
            "stress_mode": "prime",
            "require_pronunciation": True,
            "enforce_stress_consistency": True,
            "render_mode": "prime",
        },
    )
    yaml_text = profile_to_yaml_text(ui_profile)
    target = tmp_path / "ui_profile.yml"
    target.write_text(yaml_text, encoding="utf-8")

    loaded = load_profile(target)
    assert loaded.profile_id == "reading_ui_test"
    assert loaded.norm.get("long_s_to_s") is True
    assert loaded.norm.get("strip_diacritics") is True
    assert loaded.qa.get("require_pronunciation") is True
    assert loaded.render.get("parenthesize_prime_segment") is True


def test_build_profile_from_ui_changes_normalization_output() -> None:
    base = load_profile(Path("rules/templates/reading_v1.yml"))
    aligned = build_profile_from_ui(
        base,
        {
            "long_s_to_s": True,
            "lowercase": True,
            "strip_diacritics": True,
            "normalize_spaces": True,
            "remove_punctuation": True,
            "render_mode": "none",
            "stress_mode": "both",
            "require_pronunciation": False,
            "enforce_stress_consistency": False,
        },
    )

    base_applied = apply_profile("Júvénaſ", base)
    aligned_applied = apply_profile("Júvénaſ", aligned)
    assert base_applied.form_norm != aligned_applied.form_norm
    assert aligned_applied.form_norm == "juvenas"
