from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from apps.ml.embeddings.serde import deserialize_enriched_document
from apps.ml.sentiment import (
    InMemorySentimentRepository,
    PostgresSentimentRepository,
    SentimentBatchService,
    SentimentServiceConfig,
    TransformerSentimentBackend,
)


class ConsumerMessageProtocol(Protocol):
    value: Any

    async def commit(self) -> None:
        ...


class ConsumerProtocol(Protocol):
    async def getmany(self, *, max_records: int, timeout_ms: int) -> Sequence[ConsumerMessageProtocol]:
        ...


@dataclass(frozen=True, slots=True)
class SentimentConsumerDependencies:
    service: SentimentBatchService


class SentimentConsumer:
    def __init__(
        self,
        *,
        config: SentimentServiceConfig,
        dependencies: SentimentConsumerDependencies,
    ) -> None:
        self._config = config
        self._service = dependencies.service

    async def handle_messages(self, messages: Sequence[ConsumerMessageProtocol]) -> int:
        if not messages:
            return 0

        documents = [deserialize_enriched_document(_coerce_payload(message.value)) for message in messages]
        sentiments = self._service.process_batch(documents)
        for message in messages:
            await message.commit()
        return len(sentiments)


class KafkaSentimentConsumerApp:
    def __init__(
        self,
        *,
        config: SentimentServiceConfig,
        consumer: ConsumerProtocol,
        service: SentimentBatchService,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._sentiment_consumer = SentimentConsumer(
            config=config,
            dependencies=SentimentConsumerDependencies(service=service),
        )

    async def run_forever(self) -> None:
        while True:
            messages = await self._consumer.getmany(
                max_records=self._config.batch_size,
                timeout_ms=self._config.max_batch_wait_ms,
            )
            if messages:
                await self._sentiment_consumer.handle_messages(messages)
            await asyncio.sleep(0)


def build_default_sentiment_service(config: SentimentServiceConfig) -> SentimentBatchService:
    repository = (
        PostgresSentimentRepository(config.postgres_dsn)
        if config.postgres_dsn
        else InMemorySentimentRepository()
    )
    backend = TransformerSentimentBackend(
        model_name=config.model_name,
        device=config.device,
    )
    service = SentimentBatchService(
        repository=repository,
        backend=backend,
        config=config,
    )
    service.initialize()
    return service


def _coerce_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    raise TypeError(f"unsupported message payload type: {type(value)!r}")
