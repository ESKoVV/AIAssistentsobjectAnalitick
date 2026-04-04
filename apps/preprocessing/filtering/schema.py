from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from apps.preprocessing.language import LanguageAnnotatedDocument


class FilterStatus(str, Enum):
    PASS = "pass"
    REVIEW = "review"
    DROP = "drop"


@dataclass(slots=True)
class FilteredDocument(LanguageAnnotatedDocument):
    filter_status: FilterStatus
    filter_reasons: tuple[str, ...]
    quality_weight: float
    anomaly_flags: tuple[str, ...] = field(default_factory=tuple, kw_only=True)
    anomaly_confidence: float = field(default=0.0, kw_only=True)
