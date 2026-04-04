from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import load_config

CONFIG = load_config()


def main():
    admin_client = KafkaAdminClient(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        client_id="topic-creator",
    )

    topic = NewTopic(
        name=CONFIG.kafka_topic,
        num_partitions=1,
        replication_factor=1,
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Топик создан: {CONFIG.kafka_topic}")
    except TopicAlreadyExistsError:
        print(f"Топик уже существует: {CONFIG.kafka_topic}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
