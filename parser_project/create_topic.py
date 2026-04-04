import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NodeNotReadyError, NoBrokersAvailable, TopicAlreadyExistsError

from config import load_config

CONFIG = load_config()

RAW_TOPIC_PARTITIONS = int(os.getenv("KAFKA_RAW_TOPIC_PARTITIONS", "4"))
DEFAULT_TOPIC_REPLICATION_FACTOR = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "2"))
RAW_TOPIC_RETENTION_MS = 7 * 24 * 3600 * 1000
RAW_TOPIC_COMPRESSION = "lz4"


def _broker_count(bootstrap_servers: str) -> int:
    brokers = [item.strip() for item in bootstrap_servers.split(",") if item.strip()]
    return max(1, len(brokers))


def _build_topic(topic_name: str, *, broker_count: int) -> NewTopic:
    if topic_name in {CONFIG.kafka_raw_topic, CONFIG.kafka_raw_dlq_topic}:
        target_replication = 3 if broker_count >= 3 else DEFAULT_TOPIC_REPLICATION_FACTOR
        replication_factor = max(1, min(target_replication, broker_count))
        return NewTopic(
            name=topic_name,
            num_partitions=max(1, RAW_TOPIC_PARTITIONS),
            replication_factor=replication_factor,
            topic_configs={
                "retention.ms": str(RAW_TOPIC_RETENTION_MS),
                "compression.type": RAW_TOPIC_COMPRESSION,
            },
        )

    replication_factor = max(1, min(DEFAULT_TOPIC_REPLICATION_FACTOR, broker_count))
    return NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=replication_factor,
    )


def _create_admin_client(max_attempts: int = 5, retry_delay_seconds: float = 1.0) -> KafkaAdminClient:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaAdminClient(
                bootstrap_servers=CONFIG.kafka_bootstrap_servers,
                client_id="topic-creator",
            )
        except (NoBrokersAvailable, NodeNotReadyError) as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(retry_delay_seconds)

    raise RuntimeError(
        "Kafka broker недоступен для create_topic.py после повторных попыток. "
        "Проверь KAFKA_BOOTSTRAP_SERVERS "
        f"(сейчас: {CONFIG.kafka_bootstrap_servers!r}) "
        "и убедись, что контейнер kafka запущен."
    ) from last_error


def main() -> None:
    admin_client = _create_admin_client()
    broker_count = _broker_count(CONFIG.kafka_bootstrap_servers)

    topics_to_create = [
        CONFIG.kafka_raw_topic,
        CONFIG.kafka_raw_dlq_topic,
        CONFIG.kafka_preprocessed_topic,
        CONFIG.kafka_clusters_updated_topic,
        CONFIG.kafka_descriptions_updated_topic,
        CONFIG.kafka_rankings_updated_topic,
    ]
    unique_topics = list(dict.fromkeys(topics_to_create))

    print("Запуск создания Kafka topics:")
    for topic_name in unique_topics:
        print(f"- {topic_name}")

    try:
        for topic_name in unique_topics:
            topic = _build_topic(topic_name, broker_count=broker_count)
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                print(f"Топик создан: {topic_name}")
            except TopicAlreadyExistsError:
                print(f"Топик уже существует: {topic_name}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
