from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, pstdev
from typing import Sequence


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BatchEmbeddingMetrics:
    batch_size: int
    latency_ms: float
    raw_norm_mean: float
    raw_norm_std: float
    raw_norm_min: float
    raw_norm_max: float
    truncated_ratio: float
    batch_mean_cosine: float
    daily_mean_cosine: float


class DailyCosineTracker:
    def __init__(self) -> None:
        self._day: datetime.date | None = None
        self._sum = 0.0
        self._count = 0

    def observe(self, cosine_value: float, *, now: datetime) -> float:
        normalized_now = now.astimezone(timezone.utc)
        current_day = normalized_now.date()
        if self._day != current_day:
            self._day = current_day
            self._sum = 0.0
            self._count = 0

        self._sum += cosine_value
        self._count += 1
        return self._sum / self._count


def build_batch_metrics(
    *,
    raw_embeddings: Sequence[Sequence[float]],
    truncated_count: int,
    latency_ms: float,
    tracker: DailyCosineTracker,
    now: datetime,
) -> BatchEmbeddingMetrics:
    norms = [_l2_norm(embedding) for embedding in raw_embeddings]
    batch_mean_cosine = compute_mean_pairwise_cosine(raw_embeddings)
    daily_mean_cosine = tracker.observe(batch_mean_cosine, now=now)

    return BatchEmbeddingMetrics(
        batch_size=len(raw_embeddings),
        latency_ms=latency_ms,
        raw_norm_mean=mean(norms) if norms else 0.0,
        raw_norm_std=pstdev(norms) if len(norms) > 1 else 0.0,
        raw_norm_min=min(norms) if norms else 0.0,
        raw_norm_max=max(norms) if norms else 0.0,
        truncated_ratio=(truncated_count / len(raw_embeddings)) if raw_embeddings else 0.0,
        batch_mean_cosine=batch_mean_cosine,
        daily_mean_cosine=daily_mean_cosine,
    )


def log_embedding_metrics(metrics: BatchEmbeddingMetrics) -> None:
    logger.info(
        "embedding_batch_processed",
        extra={
            "batch_size": metrics.batch_size,
            "latency_ms": metrics.latency_ms,
            "raw_norm_mean": metrics.raw_norm_mean,
            "raw_norm_std": metrics.raw_norm_std,
            "raw_norm_min": metrics.raw_norm_min,
            "raw_norm_max": metrics.raw_norm_max,
            "truncated_ratio": metrics.truncated_ratio,
            "batch_mean_cosine": metrics.batch_mean_cosine,
            "daily_mean_cosine": metrics.daily_mean_cosine,
        },
    )


def compute_mean_pairwise_cosine(embeddings: Sequence[Sequence[float]]) -> float:
    if len(embeddings) < 2:
        return 1.0 if embeddings else 0.0

    cosine_values: list[float] = []
    for left_index in range(len(embeddings)):
        for right_index in range(left_index + 1, len(embeddings)):
            cosine_values.append(_cosine_similarity(embeddings[left_index], embeddings[right_index]))

    return mean(cosine_values) if cosine_values else 0.0


def _cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    left_norm = _l2_norm(left)
    right_norm = _l2_norm(right)
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return sum(left_value * right_value for left_value, right_value in zip(left, right)) / (
        left_norm * right_norm
    )


def _l2_norm(vector: Sequence[float]) -> float:
    return math.sqrt(sum(value * value for value in vector))
