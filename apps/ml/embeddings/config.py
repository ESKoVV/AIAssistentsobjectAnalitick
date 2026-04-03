from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_GPU_BATCH_SIZE = 32
DEFAULT_CPU_BATCH_SIZE = 8
DEFAULT_MODEL_NAME = "intfloat/multilingual-e5-large"
DEFAULT_INPUT_TOPIC = "preprocessed.documents"
DEFAULT_OUTPUT_TOPIC = "embedded.documents"
DEFAULT_MAX_TOKENS = 512
DEFAULT_CHUNK_OVERLAP = 64
DEFAULT_REDIS_TTL_SECONDS = 24 * 60 * 60
DEFAULT_MAX_BATCH_WAIT_MS = 250
DEFAULT_EMBEDDING_DIMENSION = 1024
DEFAULT_SPOOL_PATH = "/tmp/embedding_spool.sqlite3"


@dataclass(frozen=True, slots=True)
class EmbeddingServiceConfig:
    model_name: str
    model_version: str
    device: str = "cpu"
    batch_size: int = DEFAULT_CPU_BATCH_SIZE
    max_batch_wait_ms: int = DEFAULT_MAX_BATCH_WAIT_MS
    max_tokens: int = DEFAULT_MAX_TOKENS
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
    input_topic: str = DEFAULT_INPUT_TOPIC
    output_topic: str = DEFAULT_OUTPUT_TOPIC
    postgres_dsn: str | None = None
    redis_dsn: str | None = None
    spool_path: str = DEFAULT_SPOOL_PATH
    redis_ttl_seconds: int = DEFAULT_REDIS_TTL_SECONDS
    embedding_dimension: int = DEFAULT_EMBEDDING_DIMENSION

    def __post_init__(self) -> None:
        if not self.model_name.strip():
            raise ValueError("model_name must be non-empty")
        if not self.model_version.strip():
            raise ValueError("model_version must be non-empty and pinned")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_batch_wait_ms < 0:
            raise ValueError("max_batch_wait_ms must be non-negative")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if self.chunk_overlap < 0:
            raise ValueError("chunk_overlap must be non-negative")
        if self.chunk_overlap >= self.max_tokens:
            raise ValueError("chunk_overlap must be smaller than max_tokens")
        if self.redis_ttl_seconds <= 0:
            raise ValueError("redis_ttl_seconds must be positive")
        if self.embedding_dimension <= 0:
            raise ValueError("embedding_dimension must be positive")

    @classmethod
    def from_env(cls) -> "EmbeddingServiceConfig":
        device = os.getenv("EMBEDDINGS_DEVICE", "cpu")
        default_batch_size = DEFAULT_GPU_BATCH_SIZE if device.startswith("cuda") else DEFAULT_CPU_BATCH_SIZE

        return cls(
            model_name=os.getenv("EMBEDDINGS_MODEL_NAME", DEFAULT_MODEL_NAME),
            model_version=os.getenv("EMBEDDINGS_MODEL_VERSION", "").strip(),
            device=device,
            batch_size=int(os.getenv("EMBEDDINGS_BATCH_SIZE", default_batch_size)),
            max_batch_wait_ms=int(os.getenv("EMBEDDINGS_MAX_BATCH_WAIT_MS", DEFAULT_MAX_BATCH_WAIT_MS)),
            max_tokens=int(os.getenv("EMBEDDINGS_MAX_TOKENS", DEFAULT_MAX_TOKENS)),
            chunk_overlap=int(os.getenv("EMBEDDINGS_CHUNK_OVERLAP", DEFAULT_CHUNK_OVERLAP)),
            input_topic=os.getenv("EMBEDDINGS_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            output_topic=os.getenv("EMBEDDINGS_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC),
            postgres_dsn=os.getenv("EMBEDDINGS_POSTGRES_DSN") or None,
            redis_dsn=os.getenv("EMBEDDINGS_REDIS_DSN") or None,
            spool_path=os.getenv("EMBEDDINGS_SPOOL_PATH", DEFAULT_SPOOL_PATH),
            redis_ttl_seconds=int(os.getenv("EMBEDDINGS_REDIS_TTL_SECONDS", DEFAULT_REDIS_TTL_SECONDS)),
            embedding_dimension=int(
                os.getenv("EMBEDDINGS_DIMENSION", DEFAULT_EMBEDDING_DIMENSION),
            ),
        )
