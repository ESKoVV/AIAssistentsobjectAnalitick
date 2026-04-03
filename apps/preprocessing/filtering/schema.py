from __future__ import annotations

from dataclasses import dataclass
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
