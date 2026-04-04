from __future__ import annotations

import asyncio
import contextlib
import os

from apps.ml.ranking import RankingServiceConfig
from apps.orchestration.consumers import (
    AioKafkaProducerAdapter,
    KafkaRankingConsumerApp,
    build_default_ranking_service,
)

from .kafka_runtime import AIOKafkaBatchConsumerAdapter, create_consumer, create_producer


async def _main() -> None:
    config = RankingServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("RANKING_POSTGRES_DSN must be configured")
    bootstrap_servers = os.getenv("RANKING_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("RANKING_KAFKA_BOOTSTRAP_SERVERS must be configured")

    raw_consumer = await create_consumer(
        topic=config.input_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=os.getenv("RANKING_CONSUMER_GROUP_ID", "ranking-consumer-group"),
    )
    raw_producer = await create_producer(bootstrap_servers=bootstrap_servers)
    try:
        app = KafkaRankingConsumerApp(
            config=config,
            consumer=AIOKafkaBatchConsumerAdapter(raw_consumer),
            producer=AioKafkaProducerAdapter(raw_producer),
            service=build_default_ranking_service(config),
        )
        await app.run_forever()
    finally:
        with contextlib.suppress(Exception):
            await raw_consumer.stop()
        with contextlib.suppress(Exception):
            await raw_producer.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
