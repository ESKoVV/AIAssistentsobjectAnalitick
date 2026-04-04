from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import load_config

CONFIG = load_config()


def main():
    admin_client = KafkaAdminClient(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        client_id="topic-creator",
    )

    topics_to_create = [
        CONFIG.kafka_raw_topic,
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
            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1,
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
