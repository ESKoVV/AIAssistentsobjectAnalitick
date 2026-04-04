from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Protocol

from apps.ml.clustering import ClusteringServiceConfig, serialize_payload
from apps.ml.clustering.service import ClusteringService
from apps.ml.clustering.storage import InMemoryClusteringRepository, PostgresClusteringRepository


class ClusterEventPublisherProtocol(Protocol):
    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        ...


class NullClusterEventPublisher:
    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        return None


class AioKafkaClusterEventPublisher:
    def __init__(self, producer: Any) -> None:
        self._producer = producer

    async def publish(self, topic: str, value: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic,
            json.dumps(value, ensure_ascii=False).encode("utf-8"),
        )


def build_default_clustering_service(config: ClusteringServiceConfig) -> ClusteringService:
    repository = (
        PostgresClusteringRepository(
            config.postgres_dsn,
            embeddings_table=config.embeddings_table,
            documents_table=config.documents_table,
        )
        if config.postgres_dsn
        else InMemoryClusteringRepository()
    )
    service = ClusteringService(repository=repository, config=config)
    service.initialize()
    return service


async def run_full_recompute(
    *,
    service: ClusteringService,
    publisher: ClusterEventPublisherProtocol,
    topic: str,
    now: datetime | None = None,
) -> object:
    result = service.run_full_recompute(now=now)
    if result.event is not None:
        await publisher.publish(topic, serialize_payload(result.event))
    return result


def run_online_cycle(*, service: ClusteringService) -> object:
    return service.run_online_cycle()
