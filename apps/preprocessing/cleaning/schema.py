from __future__ import annotations

from dataclasses import dataclass

from apps.preprocessing.filtering import FilteredDocument


@dataclass(slots=True)
class CleanedDocument(FilteredDocument):
    normalized_text: str
    token_count: int
    cleanup_flags: tuple[str, ...]
