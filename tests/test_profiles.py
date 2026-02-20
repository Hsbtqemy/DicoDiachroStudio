from pathlib import Path

from dicodiachro.core.profiles import apply_profile, load_profile


def test_alignment_profile_strips_accents_and_long_s() -> None:
    profile = load_profile(Path("rules/templates/alignment_v1.yml"))
    applied = apply_profile("Júvénaſ", profile)
    assert applied.form_norm == "juvenas"
