import os

from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")


def main():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic-creator",
    )

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=1,
        replication_factor=1,
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Топик создан: {KAFKA_TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Топик уже существует: {KAFKA_TOPIC}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()