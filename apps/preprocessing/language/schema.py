from __future__ import annotations

from dataclasses import dataclass

from apps.preprocessing.normalization import NormalizedDocument


SUPPORTED_PREPROCESSING_LANGUAGES = frozenset({"ru"})
UNKNOWN_LANGUAGE = "unknown"


@dataclass(slots=True)
class LanguageAnnotatedDocument(NormalizedDocument):
    language: str
    language_confidence: float
    is_supported_language: bool
