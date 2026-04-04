from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from apps.ml.summarization import (
    ClusterDescriptionService,
    SummarizationServiceConfig,
    serialize_payload,
)
from apps.ml.summarization.storage import (
    InMemorySummarizationRepository,
    PostgresSummarizationRepository,
)


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
class ClusterDescriptionConsumerDependencies:
    service: ClusterDescriptionService
    producer: ProducerProtocol


class ClusterDescriptionConsumer:
    def __init__(
        self,
        *,
        config: SummarizationServiceConfig,
        dependencies: ClusterDescriptionConsumerDependencies,
    ) -> None:
        self._config = config
        self._service = dependencies.service
        self._producer = dependencies.producer

    async def handle_messages(self, messages: Sequence[ConsumerMessageProtocol]) -> int:
        if not messages:
            return 0

        changed_cluster_ids: list[str] = []
        seen: set[str] = set()
        for message in messages:
            payload = _coerce_payload(message.value)
            for cluster_id in payload.get("changed_cluster_ids", ()):
                normalized = str(cluster_id)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                changed_cluster_ids.append(normalized)

        result = await self._service.process_cluster_ids(changed_cluster_ids)
        if result.event is not None:
            await self._producer.publish(
                self._config.output_topic,
                serialize_payload(result.event),
            )

        for message in messages:
            await message.commit()

        return len(result.updated_cluster_ids)


class KafkaClusterDescriptionConsumerApp:
    def __init__(
        self,
        *,
        config: SummarizationServiceConfig,
        consumer: ConsumerProtocol,
        producer: ProducerProtocol,
        service: ClusterDescriptionService,
    ) -> None:
        self._config = config
        self._consumer = consumer
        self._cluster_description_consumer = ClusterDescriptionConsumer(
            config=config,
            dependencies=ClusterDescriptionConsumerDependencies(service=service, producer=producer),
        )

    async def run_forever(self) -> None:
        while True:
            messages = await self._consumer.getmany(
                max_records=100,
                timeout_ms=250,
            )
            if messages:
                await self._cluster_description_consumer.handle_messages(messages)
            await asyncio.sleep(0)


def build_default_summarization_service(
    config: SummarizationServiceConfig,
    *,
    llm_client: Any,
) -> ClusterDescriptionService:
    repository = (
        PostgresSummarizationRepository(
            config.postgres_dsn,
            embeddings_table=config.embeddings_table,
            documents_table=config.documents_table,
        )
        if config.postgres_dsn
        else InMemorySummarizationRepository()
    )
    service = ClusterDescriptionService(
        repository=repository,
        llm_client=llm_client,
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
