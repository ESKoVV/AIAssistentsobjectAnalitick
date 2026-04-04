from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any


class KafkaMessageAdapter:
    def __init__(self, consumer: Any, message: Any) -> None:
        self._consumer = consumer
        self._message = message
        self.value = _coerce_payload(message.value)

    async def commit(self) -> None:
        from aiokafka import TopicPartition
        from aiokafka.structs import OffsetAndMetadata

        partition = TopicPartition(self._message.topic, self._message.partition)
        await self._consumer.commit(
            {
                partition: OffsetAndMetadata(
                    self._message.offset + 1,
                    "",
                ),
            },
        )


class AIOKafkaBatchConsumerAdapter:
    def __init__(self, consumer: Any) -> None:
        self._consumer = consumer

    async def getmany(self, *, max_records: int, timeout_ms: int) -> Sequence[KafkaMessageAdapter]:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        messages: list[KafkaMessageAdapter] = []
        for batch in batches.values():
            for message in batch:
                messages.append(KafkaMessageAdapter(self._consumer, message))
        return messages


async def create_consumer(
    *,
    topic: str,
    bootstrap_servers: str,
    group_id: str,
) -> Any:
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    return consumer


async def create_producer(*, bootstrap_servers: str) -> Any:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    return producer


def _coerce_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    raise TypeError(f"unsupported message payload type: {type(value)!r}")
