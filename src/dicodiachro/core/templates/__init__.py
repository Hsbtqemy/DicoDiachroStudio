"""Template workshop core for record -> entry extraction."""

from .engine import (
    TemplateEngineError,
    apply_template_to_records,
    load_source_records,
    preview_template,
)
from .spec import (
    EntryDraft,
    PreviewRow,
    SourceRecord,
    TemplateApplyResult,
    TemplateKind,
    TemplatePreviewResult,
    TemplateSpec,
    template_sha256,
)

__all__ = [
    "EntryDraft",
    "PreviewRow",
    "SourceRecord",
    "TemplateApplyResult",
    "TemplateEngineError",
    "TemplateKind",
    "TemplatePreviewResult",
    "TemplateSpec",
    "apply_template_to_records",
    "load_source_records",
    "preview_template",
    "template_sha256",
]
