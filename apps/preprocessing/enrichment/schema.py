from __future__ import annotations

from dataclasses import dataclass, field

from apps.preprocessing.geo_enrichment import GeoEnrichedDocument


@dataclass(slots=True)
class EnrichedDocument(GeoEnrichedDocument):
    engagement: int
    metadata_version: str
    category: str = field(default="other", kw_only=True)
    category_label: str = field(default="Прочее", kw_only=True)
    category_confidence: float = field(default=0.0, kw_only=True)
    secondary_category: str | None = field(default=None, kw_only=True)
