from .engine import DEFAULT_DEDUPLICATION_CONFIG, DeduplicationConfig, deduplicate_documents
from .schema import DeduplicatedDocument

__all__ = [
    "DEFAULT_DEDUPLICATION_CONFIG",
    "DeduplicatedDocument",
    "DeduplicationConfig",
    "deduplicate_documents",
]
