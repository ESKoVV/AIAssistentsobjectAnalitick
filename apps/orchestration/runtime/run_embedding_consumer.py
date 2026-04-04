from __future__ import annotations

import asyncio
import contextlib
import os

from apps.ml.embeddings import EmbeddingServiceConfig
from apps.orchestration.consumers import (
    AioKafkaProducerAdapter,
    KafkaEmbeddingConsumerApp,
    build_default_service,
)

from .kafka_runtime import AIOKafkaBatchConsumerAdapter, create_consumer, create_producer


class NullProducer:
    async def publish(self, topic: str, value: dict[str, object]) -> None:
        del topic, value
        return None


async def _main() -> None:
    config = EmbeddingServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("EMBEDDINGS_POSTGRES_DSN must be configured")
    bootstrap_servers = os.getenv("EMBEDDINGS_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("EMBEDDINGS_KAFKA_BOOTSTRAP_SERVERS must be configured")

    raw_consumer = await create_consumer(
        topic=config.input_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=os.getenv("EMBEDDINGS_CONSUMER_GROUP_ID", "embeddings-consumer-group"),
    )
    raw_producer = None
    try:
        producer = NullProducer()
        if config.output_topic.strip():
            raw_producer = await create_producer(bootstrap_servers=bootstrap_servers)
            producer = AioKafkaProducerAdapter(raw_producer)
        app = KafkaEmbeddingConsumerApp(
            config=config,
            consumer=AIOKafkaBatchConsumerAdapter(raw_consumer),
            producer=producer,
            service=build_default_service(config),
        )
        await app.run_forever()
    finally:
        with contextlib.suppress(Exception):
            await raw_consumer.stop()
        if raw_producer is not None:
            with contextlib.suppress(Exception):
                await raw_producer.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
