import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from config import load_config

CONFIG = load_config()
RAW_TOPIC_PARTITIONS = int(os.getenv("KAFKA_RAW_TOPIC_PARTITIONS", "3"))
DEFAULT_TOPIC_REPLICATION_FACTOR = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "2"))


def _broker_count(bootstrap_servers: str) -> int:
    brokers = [item.strip() for item in bootstrap_servers.split(",") if item.strip()]
    return max(1, len(brokers))


def _topic_params(topic_name: str, broker_count: int) -> tuple[int, int]:
    if topic_name == CONFIG.kafka_raw_topic:
        partitions = max(3, RAW_TOPIC_PARTITIONS)
        target_replication = 3 if broker_count >= 3 else DEFAULT_TOPIC_REPLICATION_FACTOR
        replication = max(1, min(target_replication, broker_count))
        return partitions, replication

    replication = max(1, min(DEFAULT_TOPIC_REPLICATION_FACTOR, broker_count))
    return 1, replication


def main():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=CONFIG.kafka_bootstrap_servers,
            client_id="topic-creator",
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            "Kafka broker недоступен для create_topic.py. "
            "Проверь KAFKA_BOOTSTRAP_SERVERS "
            f"(сейчас: {CONFIG.kafka_bootstrap_servers!r}) "
            "и убедись, что контейнер kafka запущен."
        ) from exc
    broker_count = _broker_count(CONFIG.kafka_bootstrap_servers)

    topics_to_create = [
        CONFIG.kafka_raw_topic,
        CONFIG.kafka_preprocessed_topic,
        CONFIG.kafka_ml_topic,
        CONFIG.kafka_ml_results_topic,
    ]
    # На случай, если в конфиге указали одинаковые имена.
    unique_topics = list(dict.fromkeys(topics_to_create))

    print("Запуск создания Kafka topics:")
    for topic_name in unique_topics:
        print(f"- {topic_name}")

    try:
        for topic_name in unique_topics:
            partitions, replication = _topic_params(topic_name, broker_count)
            topic = NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replication,
            )
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                print(f"Топик создан: {topic_name}")
            except TopicAlreadyExistsError:
                print(f"Топик уже существует: {topic_name}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
