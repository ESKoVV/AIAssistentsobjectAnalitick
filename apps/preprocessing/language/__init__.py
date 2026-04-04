from .detector import annotate_language
from .schema import (
    LanguageAnnotatedDocument,
    SUPPORTED_PREPROCESSING_LANGUAGES,
    UNKNOWN_LANGUAGE,
)

__all__ = [
    "LanguageAnnotatedDocument",
    "SUPPORTED_PREPROCESSING_LANGUAGES",
    "UNKNOWN_LANGUAGE",
    "annotate_language",
]
