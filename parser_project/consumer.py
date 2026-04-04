import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pydantic import ValidationError

from config import load_config, validate_consumer_config, validate_raw_db_config
from db import upsert_raw_document
from schema import RawDocument

CONFIG = load_config()


def save_failed_message(raw_message: dict, error: str) -> None:
    CONFIG.failed_messages_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG.failed_messages_path.open("a", encoding="utf-8") as f:
        f.write(
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


def main() -> None:
    validate_consumer_config(CONFIG)
    validate_raw_db_config(CONFIG)
    consumer = KafkaConsumer(
        CONFIG.kafka_raw_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONFIG.kafka_group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Слушаю Kafka raw topic: {CONFIG.kafka_raw_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value
            topic = message.topic
            offset = message.offset
            partition = message.partition

            try:
                raw_document = RawDocument.model_validate(raw_data)
                print(
                    f"ℹ️ topic={topic} partition={partition} offset={offset} "
                    f"source_type={raw_document.source_type} source_id={raw_document.source_id} status=processing"
                )
            except ValidationError as exc:
                print(
                    f"❌ topic={topic} partition={partition} offset={offset} "
                    f"doc_id=unknown status=validation_error error={exc}"
                )
                save_failed_message(raw_data, f"Validation error: {exc}")
                continue

            try:
                upsert_raw_document(raw_document)
                consumer.commit()
                print(
                    f"✅ topic={topic} partition={partition} offset={offset} "
                    f"source_type={raw_document.source_type} source_id={raw_document.source_id} status=upserted_and_committed"
                )
            except Exception as exc:
                print(
                    f"❌ topic={topic} partition={partition} offset={offset} "
                    f"source_type={raw_document.source_type} source_id={raw_document.source_id} status=upsert_error error={exc}"
                )
                save_failed_message(raw_data, str(exc))

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
