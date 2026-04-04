from __future__ import annotations

import asyncio
import contextlib
import os

from apps.ml.clustering import ClusteringServiceConfig
from apps.orchestration.schedulers.clustering_jobs import (
    AioKafkaClusterEventPublisher,
    build_default_clustering_service,
    run_full_recompute,
)

from .kafka_runtime import create_producer


async def _main() -> None:
    config = ClusteringServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("CLUSTERING_POSTGRES_DSN must be configured")
    bootstrap_servers = os.getenv("CLUSTERING_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("CLUSTERING_KAFKA_BOOTSTRAP_SERVERS must be configured")

    producer = await create_producer(bootstrap_servers=bootstrap_servers)
    try:
        await run_full_recompute(
            service=build_default_clustering_service(config),
            publisher=AioKafkaClusterEventPublisher(producer),
            topic=config.updated_topic,
        )
    finally:
        with contextlib.suppress(Exception):
            await producer.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
