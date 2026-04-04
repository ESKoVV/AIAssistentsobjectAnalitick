from __future__ import annotations

import re
from typing import Sequence

from .schema import SummarizationDocumentRecord, ValidationResult


FORBIDDEN_WORDS = {
    "жалоба",
    "жалобы",
    "скандал",
    "катастрофа",
    "ужас",
    "беда",
    "виноват",
    "халтура",
    "проблема",
}
WORD_PATTERN = re.compile(r"\w+", re.UNICODE)
WHITESPACE_PATTERN = re.compile(r"\s+")


def validate_description(
    summary: str,
    key_phrases: Sequence[str],
    documents: Sequence[SummarizationDocumentRecord],
) -> ValidationResult:
    normalized_summary = summary.strip()
    if not normalized_summary:
        return ValidationResult(valid=False, reason="описание отсутствует")

    words = [word.casefold() for word in WORD_PATTERN.findall(normalized_summary)]
    if len(words) < 10:
        return ValidationResult(valid=False, reason="описание слишком короткое")
    if len(words) > 120:
        return ValidationResult(valid=False, reason="описание слишком длинное")

    forbidden_found = sorted({word for word in words if word in FORBIDDEN_WORDS})
    if forbidden_found:
        return ValidationResult(
            valid=False,
            reason=f"запрещённые слова: {', '.join(forbidden_found)}",
        )

    if not 5 <= len(key_phrases) <= 7:
        return ValidationResult(valid=False, reason="нужно 5-7 ключевых фраз")

    normalized_phrases = [_normalize_text(phrase) for phrase in key_phrases]
    if len(set(normalized_phrases)) != len(normalized_phrases):
        return ValidationResult(valid=False, reason="ключевые фразы повторяются")

    document_texts = [_normalize_text(document.text) for document in documents]
    for phrase, normalized_phrase in zip(key_phrases, normalized_phrases):
        if not normalized_phrase:
            return ValidationResult(valid=False, reason="обнаружена пустая ключевая фраза")
        if not any(normalized_phrase in document_text for document_text in document_texts):
            return ValidationResult(
                valid=False,
                reason=f"ключевая фраза не найдена в текстах: {phrase}",
            )

    return ValidationResult(valid=True)


def _normalize_text(text: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", text.casefold()).strip()
