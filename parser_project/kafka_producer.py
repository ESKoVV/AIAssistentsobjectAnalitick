import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(
        v,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8"),
)


def send_document(topic: str, document: dict) -> None:
    producer.send(topic, document)
    producer.flush()