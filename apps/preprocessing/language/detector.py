from __future__ import annotations

from dataclasses import asdict
from typing import Callable

from apps.preprocessing.normalization import NormalizedDocument

from .schema import (
    LanguageAnnotatedDocument,
    SUPPORTED_PREPROCESSING_LANGUAGES,
    UNKNOWN_LANGUAGE,
)


LanguagePrediction = tuple[str, float]
LanguageDetector = Callable[[str], LanguagePrediction]


def annotate_language(
    document: NormalizedDocument,
    detector: LanguageDetector | None = None,
) -> LanguageAnnotatedDocument:
    normalized_text = document.text.strip()
    if not _has_language_signal(normalized_text):
        return _build_annotated_document(
            document=document,
            language=UNKNOWN_LANGUAGE,
            language_confidence=0.0,
        )

    detect_language = detector or _detect_language_with_fasttext_lid
    try:
        language, confidence = detect_language(normalized_text)
    except Exception:
        language, confidence = UNKNOWN_LANGUAGE, 0.0

    normalized_language = (language or UNKNOWN_LANGUAGE).strip().lower() or UNKNOWN_LANGUAGE
    normalized_confidence = max(0.0, min(float(confidence), 1.0))

    return _build_annotated_document(
        document=document,
        language=normalized_language,
        language_confidence=normalized_confidence,
    )


def _detect_language_with_fasttext_lid(text: str) -> LanguagePrediction:
    from fast_langdetect import detect

    results = detect(text, model="lite", k=1)
    if not results:
        return UNKNOWN_LANGUAGE, 0.0

    best_match = results[0]
    language = str(best_match.get("lang") or UNKNOWN_LANGUAGE)
    confidence = float(best_match.get("score") or 0.0)
    return language, confidence


def _has_language_signal(text: str) -> bool:
    return any(character.isalpha() for character in text)


def _build_annotated_document(
    document: NormalizedDocument,
    language: str,
    language_confidence: float,
) -> LanguageAnnotatedDocument:
    return LanguageAnnotatedDocument(
        **asdict(document),
        language=language,
        language_confidence=language_confidence,
        is_supported_language=language in SUPPORTED_PREPROCESSING_LANGUAGES,
    )
