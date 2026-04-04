import json
from datetime import UTC, datetime

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from config import load_config, validate_raw_consumer_config
from db import upsert_raw_message
from schema import RawMessage

CONFIG = load_config()


def build_dlq_message(*, raw_message: dict, error: str, offset: int, partition: int) -> dict:
    return {
        "failed_at": datetime.now(UTC).isoformat(),
        "error": error,
        "original_offset": offset,
        "original_partition": partition,
        "message": raw_message,
    }


def save_failed_message(raw_message: dict, error: str) -> None:
    CONFIG.failed_messages_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG.failed_messages_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "failed_at": datetime.now(UTC).isoformat(),
                    "error": error,
                    "message": raw_message,
                },
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def publish_to_dlq(
    dlq_producer: KafkaProducer,
    *,
    raw_message: dict,
    error: str,
    offset: int,
    partition: int,
) -> None:
    payload = build_dlq_message(
        raw_message=raw_message,
        error=error,
        offset=offset,
        partition=partition,
    )
    try:
        dlq_producer.send(CONFIG.kafka_raw_dlq_topic, payload)
        dlq_producer.flush()
    except Exception:  # noqa: BLE001
        save_failed_message(raw_message, error)


def persist_raw_payload(raw_data: dict) -> str:
    raw_message = RawMessage.model_validate(raw_data)
    upsert_raw_message(raw_message)
    return f"{raw_message.source_type.value}:{raw_message.source_id}"


def main() -> None:
    validate_raw_consumer_config(CONFIG)
    consumer = KafkaConsumer(
        CONFIG.kafka_raw_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONFIG.kafka_group_id,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )
    dlq_producer = KafkaProducer(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        acks="all",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
    )

    print(f"Слушаю Kafka topic: {CONFIG.kafka_raw_topic}")
    print(f"DLQ topic: {CONFIG.kafka_raw_dlq_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value
            try:
                persisted_id = persist_raw_payload(raw_data)
                consumer.commit()
                print(f"✅ Raw message сохранён в raw_messages: {persisted_id}")
            except ValidationError as exc:
                print(f"❌ Ошибка валидации raw message offset={message.offset}: {exc}")
                publish_to_dlq(
                    dlq_producer,
                    raw_message=raw_data,
                    error=f"validation_error: {exc}",
                    offset=message.offset,
                    partition=message.partition,
                )
            except Exception as exc:
                print(f"❌ Ошибка raw persistence offset={message.offset}: {exc}")
                publish_to_dlq(
                    dlq_producer,
                    raw_message=raw_data,
                    error=str(exc),
                    offset=message.offset,
                    partition=message.partition,
                )
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        dlq_producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
