from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from apps.ml.embeddings import (
    EmbeddingPipeline,
    EmbeddingServiceConfig,
    TransformerEmbeddingBackend,
    deserialize_enriched_document,
    serialize_document,
)
from apps.ml.embeddings.service import EmbeddingBatchService
from apps.ml.embeddings.spool import SQLiteEmbeddingSpool
from apps.ml.embeddings.storage import (
    InMemoryEmbeddingRepository,
    NullEmbeddingCache,
    PostgresEmbeddingRepository,
    RedisEmbeddingCache,
)


logger = logging.getLogger(__name__)


class ConsumerMessageProtocol(Protocol):
    value: Any

    async def commit(self) -> None:
        ...


class ProducerProtocol(Protocol):
    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        ...


class ConsumerProtocol(Protocol):
    async def getmany(self, *, max_records: int, timeout_ms: int) -> Sequence[ConsumerMessageProtocol]:
        ...


@dataclass(frozen=True, slots=True)
class EmbeddingConsumerDependencies:
    service: EmbeddingBatchService
    producer: ProducerProtocol


class EmbeddingConsumer:
    def __init__(
        self,
        *,
        config: EmbeddingServiceConfig,
        dependencies: EmbeddingConsumerDependencies,
    ) -> None:
        self._config = config
        self._service = dependencies.service
        self._producer = dependencies.producer

    async def handle_messages(self, messages: Sequence[ConsumerMessageProtocol]) -> int:
        if not messages:
            return 0

        documents = [deserialize_enriched_document(_coerce_payload(message.value)) for message in messages]
        processed_batch = self._service.process_batch(documents)

        if self._config.output_topic.strip():
            for document in processed_batch.documents:
                await self._producer.publish(
                    self._config.output_topic,
                    serialize_document(document),
                )

        for message in messages:
            await message.commit()

        return len(processed_batch.documents)

    async def replay_spool(self, *, limit: int = 100) -> int:
        replayed = self._service.replay_buffered(limit=limit)
        return len(replayed)


class KafkaEmbeddingConsumerApp:
    def __init__(
        self,
        *,
        config: EmbeddingServiceConfig,
        consumer: ConsumerProtocol,
        producer: ProducerProtocol,
        service: EmbeddingBatchService,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._embedding_consumer = EmbeddingConsumer(
            config=config,
            dependencies=EmbeddingConsumerDependencies(service=service, producer=producer),
        )

    async def run_forever(self) -> None:
        while True:
            messages = await self._consumer.getmany(
                max_records=self._config.batch_size,
                timeout_ms=self._config.max_batch_wait_ms,
            )
            if messages:
                await self._embedding_consumer.handle_messages(messages)
            await self._embedding_consumer.replay_spool(limit=self._config.batch_size)
            await asyncio.sleep(0)


def build_default_service(config: EmbeddingServiceConfig) -> EmbeddingBatchService:
    backend = TransformerEmbeddingBackend(
        model_name=config.model_name,
        model_version=config.model_version,
        device=config.device,
    )
    pipeline = EmbeddingPipeline(
        config=config,
        tokenizer=backend.tokenizer,
        backend=backend,
    )
    repository = (
        PostgresEmbeddingRepository(
            config.postgres_dsn,
            embedding_dimension=config.embedding_dimension,
        )
        if config.postgres_dsn
        else InMemoryEmbeddingRepository()
    )
    cache = (
        RedisEmbeddingCache(config.redis_dsn, ttl_seconds=config.redis_ttl_seconds)
        if config.redis_dsn
        else NullEmbeddingCache()
    )
    service = EmbeddingBatchService(
        pipeline=pipeline,
        repository=repository,
        cache=cache,
        spool=SQLiteEmbeddingSpool(config.spool_path),
    )
    service.ensure_model_compatibility(
        model_name=config.model_name,
        model_version=config.model_version,
    )
    return service


class AioKafkaProducerAdapter:
    def __init__(self, producer: Any) -> None:
        self._producer = producer

    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic,
            json.dumps(value, ensure_ascii=False).encode("utf-8"),
        )


def _coerce_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    raise TypeError(f"unsupported message payload type: {type(value)!r}")
