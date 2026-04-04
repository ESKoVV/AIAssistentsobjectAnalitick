import json
from datetime import datetime, UTC

from kafka import KafkaConsumer
from pydantic import ValidationError

from config import load_config, validate_consumer_config
from db import upsert_document
from schema import NormalizedDocument

CONFIG = load_config()


def save_failed_message(raw_message: dict, error: str) -> None:
    CONFIG.failed_messages_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG.failed_messages_path.open("a", encoding="utf-8") as f:
        f.write(
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


def main() -> None:
    validate_consumer_config(CONFIG)
    consumer = KafkaConsumer(
        CONFIG.kafka_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=CONFIG.kafka_group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Слушаю Kafka topic: {CONFIG.kafka_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value
            try:
                document = NormalizedDocument.model_validate(raw_data)
                upsert_document(document)
                print(f"✅ Сохранен документ в PostgreSQL: {document.doc_id}")
            except (ValidationError, Exception) as exc:
                print(f"❌ Ошибка обработки сообщения offset={message.offset}: {exc}")
                save_failed_message(raw_data, str(exc))

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
