import json
import os
import time
from atexit import register

from kafka import KafkaProducer

from config import load_config

CONFIG = load_config()

KAFKA_PRODUCER_LINGER_MS = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50"))
KAFKA_PRODUCER_BATCH_SIZE = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", str(32 * 1024)))
KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS = float(os.getenv("KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS", "2.0"))
KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES = int(os.getenv("KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES", "100"))

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

_messages_since_flush = 0
_last_flush_monotonic = time.monotonic()


def flush_producer(force: bool = False) -> None:
    global _messages_since_flush, _last_flush_monotonic

    now = time.monotonic()
    should_flush = (
        force
        or _messages_since_flush >= KAFKA_PRODUCER_FLUSH_EVERY_MESSAGES
        or (now - _last_flush_monotonic) >= KAFKA_PRODUCER_FLUSH_INTERVAL_SECONDS
    )
    if not should_flush:
        return

    producer.flush()
    _messages_since_flush = 0
    _last_flush_monotonic = now


def send_document(topic: str, document: dict) -> None:
    global _messages_since_flush

    producer.send(topic, document)
    _messages_since_flush += 1
    flush_producer()


def send_document_to_preprocessed_topic(document: dict) -> None:
    send_document(CONFIG.kafka_preprocessed_topic, document)


def send_document_to_ml_topic(document: dict) -> None:
    send_document(CONFIG.kafka_ml_topic, document)


def close_producer() -> None:
    flush_producer(force=True)
    producer.close()


register(close_producer)
