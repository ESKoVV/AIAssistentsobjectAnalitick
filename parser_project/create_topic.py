from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import load_config

CONFIG = load_config()

RAW_TOPIC_PARTITIONS = 4
RAW_TOPIC_RETENTION_MS = 7 * 24 * 3600 * 1000
RAW_TOPIC_COMPRESSION = "lz4"


def _broker_count(bootstrap_servers: str) -> int:
    brokers = [item.strip() for item in bootstrap_servers.split(",") if item.strip()]
    return max(1, len(brokers))


def _build_topic(topic_name: str, *, broker_count: int) -> NewTopic:
    if topic_name in {CONFIG.kafka_raw_topic, CONFIG.kafka_raw_dlq_topic}:
        replication_factor = 2 if broker_count >= 2 else 1
        return NewTopic(
            name=topic_name,
            num_partitions=RAW_TOPIC_PARTITIONS,
            replication_factor=replication_factor,
            topic_configs={
                "retention.ms": str(RAW_TOPIC_RETENTION_MS),
                "compression.type": RAW_TOPIC_COMPRESSION,
            },
        )

    replication_factor = 2 if broker_count >= 2 else 1
    return NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=replication_factor,
    )


def main() -> None:
    admin_client = KafkaAdminClient(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        client_id="topic-creator",
    )
    broker_count = _broker_count(CONFIG.kafka_bootstrap_servers)

    topics_to_create = [
        CONFIG.kafka_raw_topic,
        CONFIG.kafka_raw_dlq_topic,
        CONFIG.kafka_preprocessed_topic,
        CONFIG.kafka_ml_topic,
        CONFIG.kafka_ml_results_topic,
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
