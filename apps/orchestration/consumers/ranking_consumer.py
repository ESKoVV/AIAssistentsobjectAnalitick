from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from apps.ml.ranking import (
    InMemoryRankingRepository,
    PostgresRankingRepository,
    RankingService,
    RankingServiceConfig,
    serialize_payload,
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
class RankingConsumerDependencies:
    service: RankingService
    producer: ProducerProtocol


class RankingConsumer:
    def __init__(
        self,
        *,
        config: RankingServiceConfig,
        dependencies: RankingConsumerDependencies,
    ) -> None:
        self._config = config
        self._service = dependencies.service
        self._producer = dependencies.producer

    async def handle_messages(self, messages: Sequence[ConsumerMessageProtocol]) -> int:
        if not messages:
            return 0

        for message in messages:
            _coerce_payload(message.value)

        results = self._service.refresh_all_windows(mode="descriptions_updated")
        primary_result = _select_primary_result(results)
        await self._producer.publish(
            self._config.output_topic,
            serialize_payload(primary_result.event),
        )

        for message in messages:
            await message.commit()

        return len(primary_result.items)


class KafkaRankingConsumerApp:
    def __init__(
        self,
        *,
        config: RankingServiceConfig,
        consumer: ConsumerProtocol,
        producer: ProducerProtocol,
        service: RankingService,
) -> None:
        self._consumer = consumer
        self._ranking_consumer = RankingConsumer(
            config=config,
            dependencies=RankingConsumerDependencies(service=service, producer=producer),
        )

    async def run_forever(self) -> None:
        while True:
            messages = await self._consumer.getmany(
                max_records=100,
                timeout_ms=250,
            )
            if messages:
                await self._ranking_consumer.handle_messages(messages)
            await asyncio.sleep(0)


def build_default_ranking_service(config: RankingServiceConfig) -> RankingService:
    repository = (
        PostgresRankingRepository(
            config.postgres_dsn,
            documents_table=config.documents_table,
            sentiments_table=config.sentiments_table,
        )
        if config.postgres_dsn
        else InMemoryRankingRepository()
    )
    service = RankingService(
        repository=repository,
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


def _select_primary_result(results):  # type: ignore[no-untyped-def]
    for result in results:
        if result.ranking.period_hours == 24:
            return result
    return results[-1]
