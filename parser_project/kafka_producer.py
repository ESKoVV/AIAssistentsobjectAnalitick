import json
from kafka import KafkaProducer

from config import load_config

CONFIG = load_config()


producer = KafkaProducer(
    bootstrap_servers=CONFIG.kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(
        v,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8"),
)


def send_document(topic: str, document: dict) -> None:
    producer.send(topic, document)
    producer.flush()
