from __future__ import annotations

from dataclasses import dataclass

from apps.preprocessing.cleaning import CleanedDocument


@dataclass(slots=True)
class DeduplicatedDocument(CleanedDocument):
    text_sha256: str
    duplicate_group_id: str
    near_duplicate_flag: bool
    duplicate_cluster_size: int
    canonical_doc_id: str
