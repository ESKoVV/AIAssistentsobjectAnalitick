from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol, Sequence

from apps.preprocessing.enrichment import EnrichedDocument

from .config import SentimentServiceConfig
from .schema import DocumentSentiment, SentimentPrediction
from .storage import SentimentRepositoryProtocol


class SentimentBackendProtocol(Protocol):
    def predict(self, texts: Sequence[str]) -> Sequence[SentimentPrediction]:
        ...


class TransformerSentimentBackend:
    def __init__(self, *, model_name: str, device: str) -> None:
        self._model_name = model_name
        self._device = device
        self._pipeline = self._build_pipeline()

    def predict(self, texts: Sequence[str]) -> Sequence[SentimentPrediction]:
        if not texts:
            return ()
        outputs = self._pipeline(list(texts), truncation=True, top_k=None)
        predictions: list[SentimentPrediction] = []
        for output in outputs:
            normalized_output = _normalize_pipeline_output(output)
            label, score = _select_best_label(normalized_output)
            predictions.append(
                SentimentPrediction(
                    label=label,
                    score=_label_scores_to_sentiment(normalized_output, fallback_label=label, fallback_score=score),
                    raw_result={"labels": normalized_output},
                ),
            )
        return tuple(predictions)

    def _build_pipeline(self):  # type: ignore[no-untyped-def]
        try:
            from transformers import pipeline
        except ImportError as exc:
            raise RuntimeError(
                "TransformerSentimentBackend requires 'transformers' and its runtime dependencies",
            ) from exc

        return pipeline(
            task="text-classification",
            model=self._model_name,
            tokenizer=self._model_name,
            top_k=None,
            device=_resolve_device_index(self._device),
        )


class SentimentBatchService:
    def __init__(
        self,
        *,
        repository: SentimentRepositoryProtocol,
        backend: SentimentBackendProtocol,
        config: SentimentServiceConfig,
    ) -> None:
        self._repository = repository
        self._backend = backend
        self._config = config

    def initialize(self) -> None:
        self._repository.ensure_schema()

    def process_batch(
        self,
        documents: Sequence[EnrichedDocument],
        *,
        now: datetime | None = None,
    ) -> tuple[DocumentSentiment, ...]:
        if not documents:
            return ()

        processed_at = now or datetime.now(timezone.utc)
        texts = [_document_text_for_sentiment(document) for document in documents]
        predictions = self._backend.predict(texts)
        if len(predictions) != len(documents):
            raise RuntimeError("sentiment backend returned a different number of predictions than documents")

        sentiments = tuple(
            DocumentSentiment(
                doc_id=document.doc_id,
                sentiment_score=max(-1.0, min(1.0, float(prediction.score))),
                model_name=self._config.model_name,
                model_version=self._config.model_version,
                processed_at=processed_at,
                raw_result=dict(prediction.raw_result),
            )
            for document, prediction in zip(documents, predictions, strict=True)
        )
        self._repository.upsert_document_sentiments(sentiments)
        return sentiments


def _document_text_for_sentiment(document: EnrichedDocument) -> str:
    text = (document.normalized_text or document.text).strip()
    return text or document.text.strip()


def _resolve_device_index(device: str) -> int:
    normalized = device.strip().casefold()
    if normalized.startswith("cuda"):
        if ":" in normalized:
            suffix = normalized.split(":", 1)[1]
            return int(suffix) if suffix.isdigit() else 0
        return 0
    return -1


def _normalize_pipeline_output(output: Any) -> list[dict[str, Any]]:
    if isinstance(output, dict):
        return [dict(output)]
    if isinstance(output, list):
        return [dict(item) for item in output]
    raise TypeError(f"unsupported sentiment output type: {type(output)!r}")


def _select_best_label(items: Sequence[dict[str, Any]]) -> tuple[str, float]:
    if not items:
        return ("neutral", 0.0)
    best = max(items, key=lambda item: float(item.get("score", 0.0)))
    return (str(best.get("label", "neutral")), float(best.get("score", 0.0)))


def _label_scores_to_sentiment(
    items: Sequence[dict[str, Any]],
    *,
    fallback_label: str,
    fallback_score: float,
) -> float:
    label_scores = {
        _normalize_label(str(item.get("label", ""))): float(item.get("score", 0.0))
        for item in items
    }
    positive = label_scores.get("positive")
    negative = label_scores.get("negative")
    neutral = label_scores.get("neutral")
    if positive is not None and negative is not None:
        raw_score = positive - negative
        if neutral is not None:
            raw_score *= max(0.0, 1.0 - neutral)
        return raw_score

    normalized_label = _normalize_label(fallback_label)
    if normalized_label == "negative":
        return -abs(fallback_score)
    if normalized_label == "positive":
        return abs(fallback_score)
    return 0.0


def _normalize_label(label: str) -> str:
    normalized = label.strip().casefold()
    mapping = {
        "negative": "negative",
        "neg": "negative",
        "label_0": "negative",
        "neutral": "neutral",
        "neu": "neutral",
        "label_1": "neutral",
        "positive": "positive",
        "pos": "positive",
        "label_2": "positive",
    }
    return mapping.get(normalized, normalized)
