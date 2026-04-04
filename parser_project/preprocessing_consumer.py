import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from kafka import KafkaConsumer
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.ml.embeddings.serde import serialize_document
from apps.preprocessing.normalization import normalize_document
from config import load_config, validate_preprocessing_consumer_config
from db import (
    fetch_recent_cleaned_documents,
    find_raw_message_id,
    update_preprocessing_projection,
    upsert_normalized_message,
)
from kafka_producer import send_document_to_preprocessed_topic
from preprocessing_pipeline import build_normalization_payload, preprocess_raw_message
from schema import RawMessage
from source_registry import load_source_registry, resolve_source_config

CONFIG = load_config()


def save_failed_message(raw_message: dict, error: str) -> None:
    CONFIG.failed_messages_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG.failed_messages_path.open("a", encoding="utf-8") as handle:
        handle.write(
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


def process_raw_payload(raw_data: dict, source_registry: dict[str, dict]) -> str | None:
    raw_message = RawMessage.model_validate(raw_data)
    source_config = resolve_source_config(source_registry, raw_message.source_type)
    preview_normalized = normalize_document(
        build_normalization_payload(raw_message),
        source_config,
    )
    recent_cleaned_documents = fetch_recent_cleaned_documents(
        exclude_doc_id=preview_normalized.doc_id,
    )
    raw_message_id = find_raw_message_id(
        source_type=raw_message.source_type,
        source_id=raw_message.source_id,
    )
    if raw_message_id is None:
        raise RuntimeError(
            "raw_messages entry not found for "
            f"{raw_message.source_type.value}:{raw_message.source_id}. "
            "Run raw persistence consumer first."
        )

    document, projection_updates = preprocess_raw_message(
        raw_message,
        source_config,
        recent_cleaned_documents=recent_cleaned_documents,
    )
    if projection_updates:
        update_preprocessing_projection(projection_updates)
    upsert_normalized_message(
        raw_message_id=raw_message_id,
        document=document,
    )
    if document.filter_status.value != "drop":
        send_document_to_preprocessed_topic(serialize_document(document))
        return document.doc_id
    return None


def main() -> None:
    validate_preprocessing_consumer_config(CONFIG)
    source_registry = load_source_registry(CONFIG.sources_config_path)
    consumer = KafkaConsumer(
        CONFIG.kafka_raw_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONFIG.kafka_preprocessing_group_id,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )

    print(f"Слушаю Kafka topic: {CONFIG.kafka_raw_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value
            try:
                published_doc_id = process_raw_payload(raw_data, source_registry)
                consumer.commit()
                if published_doc_id is not None:
                    print(
                        "✅ Обработан raw message и опубликован preprocessed document: "
                        f"{published_doc_id}",
                    )
                else:
                    source_type = raw_data.get("source_type", "unknown")
                    source_id = raw_data.get("source_id", "unknown")
                    print(
                        "✅ Обработан raw message и сохранён audit-only DROP документ: "
                        f"{source_type}:{source_id}",
                    )
            except ValidationError as exc:
                print(f"❌ Ошибка валидации raw message offset={message.offset}: {exc}")
                save_failed_message(raw_data, f"validation_error: {exc}")
                consumer.commit()
            except Exception as exc:
                print(f"❌ Ошибка обработки сообщения offset={message.offset}: {exc}")
                save_failed_message(raw_data, str(exc))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
