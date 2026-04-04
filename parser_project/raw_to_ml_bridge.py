import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from config import load_config
from schema import RawDocument

CONFIG = load_config()

BRIDGE_GROUP_ID = os.getenv("KAFKA_RAW_TO_ML_GROUP_ID", f"{CONFIG.kafka_group_id}-raw-to-ml")
BRIDGE_FAILED_MESSAGES_PATH = Path(
    os.getenv("RAW_TO_ML_FAILED_MESSAGES_PATH", "raw_to_ml_failed_messages.jsonl")
)


def save_failed_message(raw_message: Any, error: str) -> None:
    BRIDGE_FAILED_MESSAGES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with BRIDGE_FAILED_MESSAGES_PATH.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                {
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                    "error": error,
                    "message": raw_message,
                },
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def build_ml_payload(document: RawDocument) -> dict[str, Any]:
    text = (document.text_raw or "").strip()

    doc_id = document.doc_id or f"{document.source_type}:{document.source_id}"

    return {
        "doc_id": doc_id,
        "text": text,
        "normalized_text": None,
        "language": None,
        "region_id": None,
        "municipality_id": None,
        "geo_confidence": None,
        "reach": document.reach,
        "engagement": {
            "likes": document.likes,
            "reposts": document.reposts,
            "comments_count": document.comments_count,
        },
        "is_official": document.is_official,
        "media_type": document.media_type,
        "created_at_utc": document.created_at.isoformat(),
        "pipeline_version": "raw-to-ml-bridge-v1",
        "raw_equivalents": {
            "text_raw": document.text_raw,
            "author_raw": document.author_raw,
            "source_type": document.source_type,
            "source_id": document.source_id,
            "parent_source_id": document.parent_source_id,
        },
    }


def main() -> None:
    consumer = KafkaConsumer(
        CONFIG.kafka_raw_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=BRIDGE_GROUP_ID,
        value_deserializer=lambda msg: json.loads(msg.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        acks="all",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
    )

    print(f"Слушаю Kafka raw topic: {CONFIG.kafka_raw_topic}")
    print(f"Публикую в Kafka ML topic: {CONFIG.kafka_ml_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value

            try:
                raw_document = RawDocument.model_validate(raw_data)
            except ValidationError as exc:
                print(f"❌ Ошибка валидации raw-сообщения offset={message.offset}: {exc}")
                save_failed_message(raw_data, f"validation_error: {exc}")
                consumer.commit()
                continue

            try:
                ml_payload = build_ml_payload(raw_document)
                producer.send(CONFIG.kafka_ml_topic, ml_payload).get(timeout=30)
                producer.flush()
                consumer.commit()
                print(f"✅ Отправлен документ в ML topic: {raw_document.source_type}:{raw_document.source_id}")
            except Exception as exc:
                print(
                    "❌ Ошибка отправки в ML topic "
                    f"offset={message.offset}, source_id={raw_document.source_id}: {exc}"
                )
                save_failed_message(raw_data, str(exc))

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
