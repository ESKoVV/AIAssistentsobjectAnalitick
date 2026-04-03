from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Protocol

from apps.preprocessing.enrichment import EnrichedDocument


URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
MENTION_PATTERN = re.compile(r"@\w+")
WHITESPACE_PATTERN = re.compile(r"\s+")


class TokenizerProtocol(Protocol):
    def encode(self, text: str, *, add_special_tokens: bool = False) -> list[int]:
        ...

    def decode(self, token_ids: list[int], *, skip_special_tokens: bool = True) -> str:
        ...


@dataclass(frozen=True, slots=True)
class PreparedDocument:
    text_used: str
    token_ids: tuple[int, ...]
    chunks: tuple[str, ...]
    token_count: int
    truncated: bool


def prepare_text(document: EnrichedDocument) -> str:
    text = URL_PATTERN.sub(" ", document.text)
    text = MENTION_PATTERN.sub(" ", text)
    text = _demojize_text(text)

    text = WHITESPACE_PATTERN.sub(" ", text).strip()
    return f"passage: [{document.source_type.value}] {text}".strip()


def prepare_document(
    document: EnrichedDocument,
    tokenizer: TokenizerProtocol,
    *,
    max_tokens: int,
    overlap: int,
) -> PreparedDocument:
    text_used = prepare_text(document)
    token_ids = tuple(tokenizer.encode(text_used, add_special_tokens=False))
    chunk_token_ids = chunk_token_windows(token_ids, max_tokens=max_tokens, overlap=overlap)
    chunks = tuple(
        tokenizer.decode(list(chunk), skip_special_tokens=True).strip()
        for chunk in chunk_token_ids
    )

    return PreparedDocument(
        text_used=text_used,
        token_ids=token_ids,
        chunks=chunks if chunks else (text_used,),
        token_count=len(token_ids),
        truncated=len(chunk_token_ids) > 1,
    )


def chunk_token_windows(
    token_ids: tuple[int, ...] | list[int],
    *,
    max_tokens: int,
    overlap: int,
) -> tuple[tuple[int, ...], ...]:
    if max_tokens <= 0:
        raise ValueError("max_tokens must be positive")
    if overlap < 0:
        raise ValueError("overlap must be non-negative")
    if overlap >= max_tokens:
        raise ValueError("overlap must be smaller than max_tokens")

    normalized_ids = tuple(token_ids)
    if not normalized_ids:
        return (tuple(),)
    if len(normalized_ids) <= max_tokens:
        return (normalized_ids,)

    chunks: list[tuple[int, ...]] = []
    start = 0
    step = max_tokens - overlap

    while start < len(normalized_ids):
        end = min(start + max_tokens, len(normalized_ids))
        chunks.append(normalized_ids[start:end])
        if end >= len(normalized_ids):
            break
        start += step

    return tuple(chunks)


def _demojize_text(text: str) -> str:
    try:
        import emoji
    except ImportError:
        return _fallback_demojize(text)

    localized = emoji.demojize(text, language="ru")
    if localized != text:
        return localized

    default = emoji.demojize(text)
    if default != text:
        return default

    return _fallback_demojize(text)


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
