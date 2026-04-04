import json
import os
from atexit import register

from kafka import KafkaProducer

from config import load_config

CONFIG = load_config()

KAFKA_PRODUCER_LINGER_MS = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50"))
KAFKA_PRODUCER_BATCH_SIZE = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", str(32 * 1024)))

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

def flush() -> None:
    producer.flush()


def send_document(topic: str, document: dict) -> None:
    producer.send(topic, document)


def send_document_to_preprocessed_topic(document: dict) -> None:
    send_document(CONFIG.kafka_preprocessed_topic, document)


def send_document_to_ml_topic(document: dict) -> None:
    send_document(CONFIG.kafka_ml_topic, document)


def close_producer() -> None:
    flush()
    producer.close()


register(close_producer)
