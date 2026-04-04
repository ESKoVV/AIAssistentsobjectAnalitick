from __future__ import annotations

import asyncio
import contextlib
import os

from apps.ml.sentiment import SentimentServiceConfig
from apps.orchestration.consumers import KafkaSentimentConsumerApp, build_default_sentiment_service

from .kafka_runtime import AIOKafkaBatchConsumerAdapter, create_consumer


async def _main() -> None:
    config = SentimentServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("SENTIMENT_POSTGRES_DSN or DATABASE_URL must be configured")
    bootstrap_servers = os.getenv("SENTIMENT_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("SENTIMENT_KAFKA_BOOTSTRAP_SERVERS must be configured")

    raw_consumer = await create_consumer(
        topic=config.input_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=os.getenv("SENTIMENT_CONSUMER_GROUP_ID", "sentiment-consumer-group"),
    )
    try:
        app = KafkaSentimentConsumerApp(
            config=config,
            consumer=AIOKafkaBatchConsumerAdapter(raw_consumer),
            service=build_default_sentiment_service(config),
        )
        await app.run_forever()
    finally:
        with contextlib.suppress(Exception):
            await raw_consumer.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
