import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "documents-consumer-group")


def save_message_to_file(path: str, message: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(message, ensure_ascii=False, default=str) + "\n")


def main():
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
            data = message.value

            short_view = dict(data)
            short_view.pop("raw_payload", None)

            print("Получено сообщение из Kafka:")
            print(json.dumps(short_view, indent=2, ensure_ascii=False, default=str))
            print("-" * 80)

            save_message_to_file("consumed_documents.jsonl", data)

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()