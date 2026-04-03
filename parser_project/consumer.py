import json
import os
from datetime import datetime, UTC
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pydantic import ValidationError

from db import upsert_document
from schema import NormalizedDocument

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "documents-consumer-group")
FAILED_MESSAGES_PATH = Path(os.getenv("FAILED_MESSAGES_PATH", "failed_messages.jsonl"))


def save_failed_message(raw_message: dict, error: str) -> None:
    FAILED_MESSAGES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_MESSAGES_PATH.open("a", encoding="utf-8") as f:
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
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Слушаю Kafka topic: {KAFKA_TOPIC}")
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
