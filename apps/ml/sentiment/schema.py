from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class SentimentPrediction:
    label: str
    score: float
    raw_result: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DocumentSentiment:
    doc_id: str
    sentiment_score: float
    model_name: str
    model_version: str
    processed_at: datetime
    raw_result: dict[str, Any]
