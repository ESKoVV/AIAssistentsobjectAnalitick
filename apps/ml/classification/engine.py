from __future__ import annotations

import re

from .config import load_taxonomy_config
from .schema import ClassificationResult, TaxonomyConfig

TOKEN_PATTERN = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)
COMMON_ENDINGS = (
    "иями",
    "ями",
    "ами",
    "иях",
    "иях",
    "его",
    "ому",
    "ыми",
    "ими",
    "ого",
    "ему",
    "иях",
    "ах",
    "ях",
    "ий",
    "ый",
    "ой",
    "ая",
    "яя",
    "ое",
    "ее",
    "ые",
    "ие",
    "ам",
    "ям",
    "ом",
    "ем",
    "ов",
    "ев",
    "ую",
    "юю",
    "ой",
    "ей",
    "а",
    "я",
    "ы",
    "и",
    "у",
    "ю",
    "е",
    "о",
    "ь",
)


def classify_document(text: str, config: TaxonomyConfig | None = None) -> ClassificationResult:
    taxonomy = config or load_taxonomy_config()
    normalized_tokens = _normalize_tokens(text or "")

    category_hits: dict[str, int] = {}
    matched_keywords: dict[str, tuple[str, ...]] = {}
    for key, category in taxonomy.categories.items():
        hits = 0
        category_matches: list[str] = []
        for keyword in category.keywords:
            count = _count_keyword_hits(normalized_tokens, keyword)
            if count <= 0:
                continue
            hits += count
            category_matches.append(keyword)
        category_hits[key] = hits
        matched_keywords[key] = tuple(category_matches)

    ranked = sorted(
        taxonomy.categories.items(),
        key=lambda item: (-category_hits[item[0]], item[0]),
    )
    top_key, top_category = ranked[0]
    top_hits = category_hits[top_key]

    if top_hits == 0:
        other = taxonomy.categories["other"]
        return ClassificationResult(
            category="other",
            category_label=other.label,
            confidence=1.0,
            secondary_category=None,
            matched_keywords=(),
        )

    secondary_category: str | None = None
    if len(ranked) > 1:
        second_key, _ = ranked[1]
        second_hits = category_hits[second_key]
        if second_hits > 0 and (top_hits - second_hits) < 2:
            secondary_category = second_key

    return ClassificationResult(
        category=top_key,
        category_label=top_category.label,
        confidence=float(top_hits) / float(top_hits + 1),
        secondary_category=secondary_category,
        matched_keywords=matched_keywords[top_key],
    )


def _count_keyword_hits(normalized_tokens: list[str], keyword: str) -> int:
    keyword_tokens = _normalize_tokens(keyword)
    if not keyword_tokens or not normalized_tokens:
        return 0

    window = len(keyword_tokens)
    hits = 0
    for index in range(len(normalized_tokens) - window + 1):
        if normalized_tokens[index : index + window] == keyword_tokens:
            hits += 1
    return hits


def _normalize_tokens(text: str) -> list[str]:
    return [_normalize_token(token) for token in TOKEN_PATTERN.findall(text.lower()) if token]


def _normalize_token(token: str) -> str:
    for ending in COMMON_ENDINGS:
        if len(token) - len(ending) < 3:
            continue
        if token.endswith(ending):
            return token[: -len(ending)]
    return token
