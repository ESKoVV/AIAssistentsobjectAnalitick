from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_MODEL_NAME = "blanchefort/rubert-base-cased-sentiment"
DEFAULT_INPUT_TOPIC = "preprocessed.documents"
DEFAULT_BATCH_SIZE = 16
DEFAULT_MAX_BATCH_WAIT_MS = 250


@dataclass(frozen=True, slots=True)
class SentimentServiceConfig:
    postgres_dsn: str | None
    model_name: str = DEFAULT_MODEL_NAME
    model_version: str = ""
    device: str = "cpu"
    input_topic: str = DEFAULT_INPUT_TOPIC
    batch_size: int = DEFAULT_BATCH_SIZE
    max_batch_wait_ms: int = DEFAULT_MAX_BATCH_WAIT_MS

    def __post_init__(self) -> None:
        if not self.model_name.strip():
            raise ValueError("model_name must be non-empty")
        if not self.model_version.strip():
            raise ValueError("model_version must be non-empty and pinned")
        if not self.input_topic.strip():
            raise ValueError("input_topic must be non-empty")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_batch_wait_ms < 0:
            raise ValueError("max_batch_wait_ms must be non-negative")

    @classmethod
    def from_env(cls, *, database_url: str | None = None) -> "SentimentServiceConfig":
        device = os.getenv("SENTIMENT_DEVICE", "cpu")
        return cls(
            postgres_dsn=os.getenv("SENTIMENT_POSTGRES_DSN") or database_url,
            model_name=os.getenv("SENTIMENT_MODEL_NAME", DEFAULT_MODEL_NAME),
            model_version=os.getenv("SENTIMENT_MODEL_VERSION", "").strip(),
            device=device,
            input_topic=os.getenv("SENTIMENT_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            batch_size=int(os.getenv("SENTIMENT_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            max_batch_wait_ms=int(
                os.getenv("SENTIMENT_MAX_BATCH_WAIT_MS", DEFAULT_MAX_BATCH_WAIT_MS),
            ),
        )
