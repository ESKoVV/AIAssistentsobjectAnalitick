import json

from kafka import KafkaConsumer, KafkaProducer

from config import load_config, validate_raw_consumer_config

CONFIG = load_config()


def main() -> None:
    validate_raw_consumer_config(CONFIG)
    consumer = KafkaConsumer(
        CONFIG.kafka_raw_dlq_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"{CONFIG.kafka_group_id}-dlq-replay",
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        acks="all",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
    )

    print(f"Слушаю DLQ topic: {CONFIG.kafka_raw_dlq_topic}")
    print(f"Replay в topic: {CONFIG.kafka_raw_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            dlq_payload = message.value
            original_message = dlq_payload.get("message")
            if not isinstance(original_message, dict):
                print(
                    "❌ Некорректное DLQ-сообщение "
                    f"offset={message.offset}, partition={message.partition}: отсутствует payload message",
                )
                continue

            try:
                producer.send(CONFIG.kafka_raw_topic, original_message).get(timeout=30)
                producer.flush()
                consumer.commit()
                print(
                    "✅ Replay выполнен "
                    f"dlq_offset={message.offset}, original_offset={dlq_payload.get('original_offset')}",
                )
            except Exception as exc:
                print(
                    "❌ Ошибка replay DLQ-сообщения "
                    f"offset={message.offset}, partition={message.partition}: {exc}",
                )
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
