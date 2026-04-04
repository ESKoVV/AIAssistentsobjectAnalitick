from __future__ import annotations

import re


LABEL_PATTERN = re.compile(r"^\s*(ОПИСАНИЕ|ФРАЗЫ)\s*:\s*(.*)$", re.IGNORECASE)
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
WORD_PATTERN = re.compile(r"\w+", re.UNICODE)


def parse_response(text: str) -> tuple[str, list[str]]:
    normalized_text = text.strip()
    if not normalized_text:
        return "", []

    summary_lines: list[str] = []
    phrase_lines: list[str] = []
    current_section: str | None = None

    for raw_line in normalized_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = LABEL_PATTERN.match(line)
        if match is not None:
            label = match.group(1).casefold()
            content = match.group(2).strip()
            current_section = "summary" if label == "описание" else "phrases"
            if current_section == "summary" and content:
                summary_lines.append(content)
            if current_section == "phrases" and content:
                phrase_lines.append(content)
            continue

        if current_section == "summary":
            summary_lines.append(line)
        elif current_section == "phrases":
            phrase_lines.append(line)

    summary = " ".join(summary_lines).strip()
    key_phrases = _normalize_key_phrases(" ".join(phrase_lines).strip())

    if not summary:
        summary = _fallback_summary(normalized_text)

    return _sanitize_summary(summary), key_phrases


def _normalize_key_phrases(raw_text: str) -> list[str]:
    if not raw_text:
        return []

    parts = raw_text.split(";")
    if len(parts) == 1:
        parts = [part.strip() for part in raw_text.splitlines() if part.strip()]

    phrases: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = " ".join(part.split()).strip(" -\t")
        if not normalized:
            continue
        fingerprint = normalized.casefold()
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        phrases.append(normalized)
    return phrases


def _fallback_summary(text: str) -> str:
    paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
    if paragraphs:
        return paragraphs[0]
    return text[:300].strip()


def _sanitize_summary(summary: str) -> str:
    normalized = " ".join(summary.split()).strip()
    if not normalized:
        return ""

    sentences = [sentence.strip() for sentence in SENTENCE_SPLIT_PATTERN.split(normalized) if sentence.strip()]
    if len(sentences) > 3:
        normalized = " ".join(sentences[:3]).strip()

    words = WORD_PATTERN.findall(normalized)
    if len(words) > 120:
        limited_words = words[:120]
        normalized = " ".join(limited_words).strip()

    if normalized and normalized[-1] not in ".!?":
        normalized = f"{normalized}."
    return normalized
