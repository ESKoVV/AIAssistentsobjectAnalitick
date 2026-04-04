from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from apps.preprocessing.enrichment import EnrichedDocument


@dataclass(slots=True)
class EmbeddedDocument(EnrichedDocument):
    embedding: list[float]
    model_name: str
    model_version: str
    embedded_at: datetime
    text_used: str
    token_count: int
    truncated: bool
