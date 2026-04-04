from __future__ import annotations

from dataclasses import dataclass

from apps.preprocessing.geo_enrichment import GeoEnrichedDocument


@dataclass(slots=True)
class EnrichedDocument(GeoEnrichedDocument):
    engagement: int
    metadata_version: str
