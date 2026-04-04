import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from config import load_config
from schema import RawMessage

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


def build_ml_payload(message: RawMessage) -> dict[str, Any]:
    text = (message.text or "").strip()
    doc_id = f"{message.source_type.value}:{message.source_id}"

    return {
        "doc_id": doc_id,
        "text": text,
        "normalized_text": None,
        "language": None,
        "region_id": None,
        "municipality_id": None,
        "geo_confidence": None,
        "reach": message.reach,
        "engagement": {
            "likes": message.likes,
            "reposts": message.reposts,
            "comments_count": message.comments_count,
        },
        "is_official": message.is_official,
        "media_type": message.media_type.value if message.media_type else None,
        "created_at_utc": message.created_at_utc.isoformat(),
        "pipeline_version": "raw-to-ml-bridge-v1",
        "raw_equivalents": {
            "text_raw": message.text,
            "author_raw": message.author_id,
            "source_type": message.source_type.value,
            "source_id": message.source_id,
            "parent_source_id": message.parent_id,
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
                raw_message = RawMessage.model_validate(raw_data)
            except ValidationError as exc:
                print(f"❌ Ошибка валидации raw-сообщения offset={message.offset}: {exc}")
                save_failed_message(raw_data, f"validation_error: {exc}")
                consumer.commit()
                continue

            try:
                ml_payload = build_ml_payload(raw_message)
                producer.send(CONFIG.kafka_ml_topic, ml_payload).get(timeout=30)
                producer.flush()
                consumer.commit()
                print(f"✅ Отправлен документ в ML topic: {raw_message.source_type.value}:{raw_message.source_id}")
            except Exception as exc:
                print(
                    "❌ Ошибка отправки в ML topic "
                    f"offset={message.offset}, source_id={raw_message.source_id}: {exc}"
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
