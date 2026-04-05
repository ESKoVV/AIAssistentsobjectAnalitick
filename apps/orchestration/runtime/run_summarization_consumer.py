from __future__ import annotations

import asyncio
import contextlib
import os

from apps.ml.summarization import SummarizationServiceConfig, build_alicagpt_client_from_env
from apps.orchestration.consumers import (
    AioKafkaProducerAdapter,
    KafkaClusterDescriptionConsumerApp,
    build_default_summarization_service,
)

from .kafka_runtime import AIOKafkaBatchConsumerAdapter, create_consumer, create_producer


async def _main() -> None:
    config = SummarizationServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("SUMMARIZATION_POSTGRES_DSN must be configured")
    bootstrap_servers = os.getenv("SUMMARIZATION_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("SUMMARIZATION_KAFKA_BOOTSTRAP_SERVERS must be configured")

    raw_consumer = await create_consumer(
        topic=config.input_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=os.getenv("SUMMARIZATION_CONSUMER_GROUP_ID", "summarization-consumer-group"),
    )
    raw_producer = await create_producer(bootstrap_servers=bootstrap_servers)
    try:
        service = build_default_summarization_service(
            config,
            llm_client=build_alicagpt_client_from_env(),
        )
        app = KafkaClusterDescriptionConsumerApp(
            config=config,
            consumer=AIOKafkaBatchConsumerAdapter(raw_consumer),
            producer=AioKafkaProducerAdapter(raw_producer),
            service=service,
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
