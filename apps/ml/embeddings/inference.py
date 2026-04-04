from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol, Sequence

from apps.ml.embeddings.config import EmbeddingServiceConfig
from apps.ml.embeddings.metrics import BatchEmbeddingMetrics, DailyCosineTracker, build_batch_metrics
from apps.ml.embeddings.preparation import PreparedDocument, TokenizerProtocol, prepare_document
from apps.ml.embeddings.schema import EmbeddedDocument
from apps.preprocessing.enrichment import EnrichedDocument


class EmbeddingBackendProtocol(Protocol):
    def encode(self, texts: Sequence[str], *, batch_size: int) -> list[list[float]]:
        ...


@dataclass(frozen=True, slots=True)
class EmbeddingBatchResult:
    documents: tuple[EmbeddedDocument, ...]
    metrics: BatchEmbeddingMetrics


class EmbeddingPipeline:
    def __init__(
        self,
        *,
        config: EmbeddingServiceConfig,
        tokenizer: TokenizerProtocol,
        backend: EmbeddingBackendProtocol,
        clock: Callable[[], datetime] | None = None,
        perf_counter: Callable[[], float] | None = None,
        metrics_tracker: DailyCosineTracker | None = None,
    ) -> None:
        self._config = config
        self._tokenizer = tokenizer
        self._backend = backend
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._perf_counter = perf_counter or time.perf_counter
        self._metrics_tracker = metrics_tracker or DailyCosineTracker()

    def embed_documents(
        self,
        documents: Sequence[EnrichedDocument],
    ) -> EmbeddingBatchResult:
        if not documents:
            metrics = build_batch_metrics(
                raw_embeddings=(),
                truncated_count=0,
                latency_ms=0.0,
                tracker=self._metrics_tracker,
                now=self._clock(),
            )
            return EmbeddingBatchResult(documents=(), metrics=metrics)

        prepared_documents = [
            prepare_document(
                document,
                self._tokenizer,
                max_tokens=self._config.max_tokens,
                overlap=self._config.chunk_overlap,
            )
            for document in documents
        ]
        chunk_texts = [chunk for prepared in prepared_documents for chunk in prepared.chunks]

        started_at = self._perf_counter()
        chunk_embeddings = self._backend.encode(chunk_texts, batch_size=self._config.batch_size)
        latency_ms = (self._perf_counter() - started_at) * 1000.0

        embedded_at = self._clock()
        raw_document_embeddings = self._pool_chunk_embeddings(prepared_documents, chunk_embeddings)
        normalized_document_embeddings = [
            _normalize_vector(embedding)
            for embedding in raw_document_embeddings
        ]

        metrics = build_batch_metrics(
            raw_embeddings=raw_document_embeddings,
            truncated_count=sum(1 for prepared in prepared_documents if prepared.truncated),
            latency_ms=latency_ms,
            tracker=self._metrics_tracker,
            now=embedded_at,
        )

        embedded_documents = tuple(
            EmbeddedDocument(
                **_embedded_document_base_kwargs(document),
                embedding=normalized_document_embeddings[index],
                model_name=self._config.model_name,
                model_version=self._config.model_version,
                embedded_at=embedded_at,
                text_used=prepared_documents[index].text_used,
                token_count=prepared_documents[index].token_count,
                truncated=prepared_documents[index].truncated,
            )
            for index, document in enumerate(documents)
        )

        return EmbeddingBatchResult(documents=embedded_documents, metrics=metrics)

    def _pool_chunk_embeddings(
        self,
        prepared_documents: Sequence[PreparedDocument],
        chunk_embeddings: Sequence[Sequence[float]],
    ) -> list[list[float]]:
        pooled_embeddings: list[list[float]] = []
        cursor = 0

        for prepared in prepared_documents:
            next_cursor = cursor + len(prepared.chunks)
            document_chunk_embeddings = chunk_embeddings[cursor:next_cursor]
            if not document_chunk_embeddings:
                raise ValueError("embedding backend returned fewer chunks than expected")

            pooled_embeddings.append(_mean_pool(document_chunk_embeddings))
            cursor = next_cursor

        if cursor != len(chunk_embeddings):
            raise ValueError("embedding backend returned more chunks than expected")

        for embedding in pooled_embeddings:
            if len(embedding) != self._config.embedding_dimension:
                raise ValueError(
                    "embedding dimension mismatch: "
                    f"expected {self._config.embedding_dimension}, got {len(embedding)}"
                )

        return pooled_embeddings


class TransformerEmbeddingBackend:
    def __init__(self, *, model_name: str, model_version: str, device: str = "cpu") -> None:
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except ImportError as exc:
            raise RuntimeError(
                "TransformerEmbeddingBackend requires 'torch' and 'transformers' to be installed",
            ) from exc

        self._torch = torch
        self._tokenizer = AutoTokenizer.from_pretrained(model_name, revision=model_version)
        self._model = AutoModel.from_pretrained(model_name, revision=model_version).to(device)
        self._device = device

    @property
    def tokenizer(self) -> TokenizerProtocol:
        return self._tokenizer

    def encode(self, texts: Sequence[str], *, batch_size: int) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), batch_size):
            batch = list(texts[start : start + batch_size])
            vectors.extend(self._encode_batch(batch))
        return vectors

    def _encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        torch = self._torch
        encoded = self._tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            return_tensors="pt",
        ).to(self._device)

        self._model.eval()
        with torch.no_grad():
            outputs = self._model(**encoded)
            token_embeddings = outputs.last_hidden_state
            attention_mask = encoded["attention_mask"].unsqueeze(-1)
            masked_embeddings = token_embeddings * attention_mask
            pooled = masked_embeddings.sum(dim=1) / attention_mask.sum(dim=1).clamp(min=1)
        return pooled.cpu().tolist()


def _mean_pool(vectors: Sequence[Sequence[float]]) -> list[float]:
    if not vectors:
        raise ValueError("mean pooling requires at least one vector")

    dimension = len(vectors[0])
    pooled = [0.0] * dimension
    for vector in vectors:
        if len(vector) != dimension:
            raise ValueError("all chunk embeddings must have the same dimension")
        for index, value in enumerate(vector):
            pooled[index] += float(value)
    return [value / len(vectors) for value in pooled]


def _normalize_vector(vector: Sequence[float]) -> list[float]:
    norm = sum(value * value for value in vector) ** 0.5
    if norm == 0.0:
        return [0.0 for _ in vector]
    return [float(value) / norm for value in vector]


def _embedded_document_base_kwargs(document: EnrichedDocument) -> dict[str, object]:
    base_kwargs = asdict(document)
    base_kwargs.pop("token_count", None)
    return base_kwargs
