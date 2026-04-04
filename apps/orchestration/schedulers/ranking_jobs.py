from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Protocol

from apps.ml.ranking import (
    InMemoryRankingRepository,
    PostgresRankingRepository,
    RankingService,
    RankingServiceConfig,
    serialize_payload,
)


class RankingEventPublisherProtocol(Protocol):
    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        ...


class NullRankingEventPublisher:
    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        return None


class AioKafkaRankingEventPublisher:
    def __init__(self, producer: Any) -> None:
        self._producer = producer

    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic,
            json.dumps(value, ensure_ascii=False).encode("utf-8"),
        )


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
    service = RankingService(repository=repository, config=config)
    service.initialize()
    return service


async def run_ranking_refresh(
    *,
    service: RankingService,
    publisher: RankingEventPublisherProtocol,
    topic: str,
    now: datetime | None = None,
    mode: str = "scheduled_refresh",
) -> object:
    results = service.refresh_all_windows(now=now, mode=mode)
    result = next((item for item in results if item.ranking.period_hours == 24), results[-1])
    await publisher.publish(topic, serialize_payload(result.event))
    return results
