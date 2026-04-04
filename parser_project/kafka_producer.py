import json
import os
import time
from atexit import register

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import load_config

CONFIG = load_config()

KAFKA_PRODUCER_LINGER_MS = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50"))
KAFKA_PRODUCER_BATCH_SIZE = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", str(32 * 1024)))
KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS = float(os.getenv("KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS", "2.0"))
KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES = int(os.getenv("KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES", "100"))

producer: KafkaProducer | None = None
_messages_since_flush = 0
_last_flush_monotonic = time.monotonic()


def flush() -> None:
    flush_producer(force=True)


def flush_producer(force: bool = False) -> None:
    global _messages_since_flush, _last_flush_monotonic

    now = time.monotonic()
    should_flush = (
        force
        or _messages_since_flush >= KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES
        or (now - _last_flush_monotonic) >= KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS
    )
    if not should_flush or producer is None:
        return

    producer.flush()
    _messages_since_flush = 0
    _last_flush_monotonic = now


def send_document(topic: str, document: dict) -> None:
    global _messages_since_flush

    active_producer = _get_or_create_producer()
    active_producer.send(topic, document)
    _messages_since_flush += 1
    flush_producer()


def send_document_to_preprocessed_topic(document: dict) -> None:
    send_document(CONFIG.kafka_preprocessed_topic, document)


def close_producer() -> None:
    global producer

    flush_producer(force=True)
    if producer is not None:
        producer.close()
        producer = None


def _get_or_create_producer() -> KafkaProducer:
    global producer
    if producer is not None:
        return producer

    try:
        producer = KafkaProducer(
            bootstrap_servers=CONFIG.kafka_bootstrap_servers,
            linger_ms=KAFKA_PRODUCER_LINGER_MS,
            batch_size=KAFKA_PRODUCER_BATCH_SIZE,
            value_serializer=lambda v: json.dumps(
                v,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8"),
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            "Kafka недоступна. Проверь KAFKA_BOOTSTRAP_SERVERS "
            f"(сейчас: {CONFIG.kafka_bootstrap_servers!r}) и запущен ли broker."
        ) from exc
    return producer


register(close_producer)
