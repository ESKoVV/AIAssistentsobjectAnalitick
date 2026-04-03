from __future__ import annotations

from dataclasses import dataclass

from apps.preprocessing.deduplication import DeduplicatedDocument


@dataclass(slots=True)
class GeoEnrichedDocument(DeduplicatedDocument):
    region_id: str | None
    municipality_id: str | None
    geo_confidence: float
    geo_source: str
    geo_evidence: tuple[str, ...]
