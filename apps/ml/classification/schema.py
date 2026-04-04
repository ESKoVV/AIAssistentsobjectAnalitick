from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TaxonomyCategory:
    key: str
    label: str
    keywords: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TaxonomyConfig:
    categories: dict[str, TaxonomyCategory]


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    category: str
    category_label: str
    confidence: float
    secondary_category: str | None
    matched_keywords: tuple[str, ...]
