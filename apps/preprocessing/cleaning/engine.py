from __future__ import annotations

import re
import unicodedata
from dataclasses import asdict

from apps.preprocessing.filtering import FilterStatus, FilteredDocument

from .schema import CleanedDocument


URL_PATTERN = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
MENTION_PATTERN = re.compile(r"(?<!\w)@[A-Za-z0-9_][A-Za-z0-9_.-]*")
EMOJI_TOKEN_PATTERN = re.compile(r":([a-z0-9_]+):", re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r"\s+")


def clean_text(document: FilteredDocument) -> CleanedDocument:
    if document.filter_status not in {FilterStatus.PASS, FilterStatus.REVIEW}:
        raise ValueError("text cleaning supports only pass/review documents")

    cleanup_flags: list[str] = []
    normalized_text = document.text

    without_urls = URL_PATTERN.sub(" ", normalized_text)
    if without_urls != normalized_text:
        cleanup_flags.append("url_removed")
        normalized_text = without_urls

    without_mentions = MENTION_PATTERN.sub("USER", normalized_text)
    if without_mentions != normalized_text:
        cleanup_flags.append("mention_normalized")
        normalized_text = without_mentions

    demojized_text = _demojize_text(normalized_text)
    if demojized_text != normalized_text:
        cleanup_flags.append("emoji_demojized")
        normalized_text = demojized_text

    compact_text = WHITESPACE_PATTERN.sub(" ", normalized_text).strip()
    if compact_text != normalized_text:
        cleanup_flags.append("whitespace_normalized")
        normalized_text = compact_text

    return CleanedDocument(
        **asdict(document),
        normalized_text=normalized_text,
        token_count=len(normalized_text.split()) if normalized_text else 0,
        cleanup_flags=tuple(cleanup_flags),
    )


def _demojize_text(text: str) -> str:
    try:
        import emoji
    except ImportError:
        return _fallback_demojize(text)

    demojized = emoji.demojize(text)
    return EMOJI_TOKEN_PATTERN.sub(_emoji_token_to_words, demojized)


def _fallback_demojize(text: str) -> str:
    parts: list[str] = []

    for character in text:
        if character in {"\u200d", "\ufe0f"}:
            parts.append(" ")
            continue

        if _looks_like_emoji(character):
            emoji_name = unicodedata.name(character, "").lower()
            if emoji_name:
                parts.append(f" {emoji_name.replace('-', ' ')} ")
                continue

        parts.append(character)

    return "".join(parts)


def _looks_like_emoji(character: str) -> bool:
    codepoint = ord(character)
    return (
        0x1F300 <= codepoint <= 0x1FAFF
        or 0x2600 <= codepoint <= 0x26FF
        or 0x2700 <= codepoint <= 0x27BF
    )


def _emoji_token_to_words(match: re.Match[str]) -> str:
    return f" {match.group(1).replace('_', ' ')} "
