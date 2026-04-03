from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol, Sequence

from apps.ml.embeddings.inference import EmbeddingBatchResult, EmbeddingPipeline
from apps.ml.embeddings.metrics import log_embedding_metrics
from apps.ml.embeddings.schema import EmbeddedDocument
from apps.ml.embeddings.spool import SQLiteEmbeddingSpool
from apps.ml.embeddings.storage import (
    EmbeddingCacheProtocol,
    EmbeddingRepositoryProtocol,
    NullEmbeddingCache,
)
from apps.preprocessing.enrichment import EnrichedDocument


logger = logging.getLogger(__name__)


class SpoolProtocol(Protocol):
    def buffer_documents(self, documents: Sequence[EmbeddedDocument]) -> None:
        ...

    def peek(self, *, limit: int = 100) -> tuple[EmbeddedDocument, ...]:
        ...

    def acknowledge(self, doc_ids: Sequence[str]) -> None:
        ...


@dataclass(frozen=True, slots=True)
class ProcessedEmbeddingBatch:
    documents: tuple[EmbeddedDocument, ...]
    buffered: bool
    metrics: object


class EmbeddingBatchService:
    def __init__(
        self,
        *,
        pipeline: EmbeddingPipeline,
        repository: EmbeddingRepositoryProtocol,
        cache: EmbeddingCacheProtocol | None = None,
        spool: SpoolProtocol | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._repository = repository
        self._cache = cache or NullEmbeddingCache()
        self._spool = spool or SQLiteEmbeddingSpool("/tmp/embedding_spool.sqlite3")

    def ensure_model_compatibility(self, *, model_name: str, model_version: str) -> None:
        self._repository.ensure_model_compatibility(
            model_name=model_name,
            model_version=model_version,
        )

    def process_batch(self, documents: Sequence[EnrichedDocument]) -> ProcessedEmbeddingBatch:
        batch_result = self._pipeline.embed_documents(documents)
        buffered = False

        try:
            self._repository.upsert_embeddings(batch_result.documents)
        except Exception:
            buffered = True
            logger.exception("embedding repository upsert failed; buffering documents locally")
            self._spool.buffer_documents(batch_result.documents)

        try:
            self._cache.cache_embeddings(batch_result.documents)
        except Exception:
            logger.exception("embedding cache update failed")

        log_embedding_metrics(batch_result.metrics)
        return ProcessedEmbeddingBatch(
            documents=batch_result.documents,
            buffered=buffered,
            metrics=batch_result.metrics,
        )

    def replay_buffered(self, *, limit: int = 100) -> tuple[EmbeddedDocument, ...]:
        buffered_documents = self._spool.peek(limit=limit)
        if not buffered_documents:
            return ()

        self._repository.upsert_embeddings(buffered_documents)
        self._spool.acknowledge([document.doc_id for document in buffered_documents])
        return buffered_documents
