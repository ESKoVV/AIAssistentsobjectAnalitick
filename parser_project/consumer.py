import json
from datetime import datetime, UTC

from urllib.parse import urlparse

from kafka import KafkaConsumer
from pydantic import ValidationError

from config import load_config, validate_consumer_config
from db import upsert_raw_document
from kafka_producer import send_document_to_ml_topic
from schema import NormalizedDocument, RawDocument

CONFIG = load_config()


def save_failed_message(raw_message: dict, error: str) -> None:
    CONFIG.failed_messages_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG.failed_messages_path.open("a", encoding="utf-8") as f:
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


def _domain_from_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    return parsed.netloc or None


def _to_raw_document_legacy(document: NormalizedDocument) -> RawDocument:
    payload = document.raw_payload or {}
    source_url = payload.get("url") or payload.get("link") or payload.get("source_url")

    return RawDocument(
        doc_id=document.doc_id,
        source_type=document.source_type.value,
        source_id=document.source_id,
        parent_source_id=document.parent_id,
        text_raw=document.text,
        title_raw=payload.get("title") or payload.get("headline") or payload.get("subject"),
        author_raw=document.author_id,
        created_at_raw=document.created_at.isoformat(),
        created_at=document.created_at,
        collected_at=document.collected_at,
        source_url=source_url,
        source_domain=_domain_from_url(source_url),
        region_hint_raw=document.region_hint,
        geo_raw={"lat": document.geo_lat, "lon": document.geo_lon} if document.geo_lat or document.geo_lon else None,
        engagement_raw={
            "reach": document.reach,
            "likes": document.likes,
            "reposts": document.reposts,
            "comments_count": document.comments_count,
            "is_official": document.is_official,
            "media_type": document.media_type.value,
        },
        raw_payload=payload,
    )


def main() -> None:
    validate_consumer_config(CONFIG)
    consumer = KafkaConsumer(
        CONFIG.kafka_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=CONFIG.kafka_group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Слушаю Kafka topic: {CONFIG.kafka_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data = message.value
            try:
                try:
                    raw_document = RawDocument.model_validate(raw_data)
                except ValidationError:
                    normalized_document = NormalizedDocument.model_validate(raw_data)
                    raw_document = _to_raw_document_legacy(normalized_document)

                upsert_raw_document(raw_document)
                print(f"✅ Сохранен сырой документ в PostgreSQL: {raw_document.doc_id}")

                ml_payload = {
                    "doc_id": raw_document.doc_id,
                    "source_type": raw_document.source_type,
                    "source_id": raw_document.source_id,
                    "text_raw": raw_document.text_raw,
                    "created_at": raw_document.created_at,
                    "collected_at": raw_document.collected_at,
                    "author_raw": raw_document.author_raw,
                    "region_hint_raw": raw_document.region_hint_raw,
                    "geo_raw": raw_document.geo_raw,
                    "engagement_raw": raw_document.engagement_raw,
                    "raw_payload": raw_document.raw_payload,
                }

                try:
                    send_document_to_ml_topic(ml_payload)
                    print(
                        f"✅ Отправлен документ в ML topic {CONFIG.kafka_ml_topic}: "
                        f"{raw_document.doc_id}"
                    )
                except Exception as exc:
                    print(f"❌ Ошибка отправки в ML topic offset={message.offset}: {exc}")
                    save_failed_message(raw_data, f"ML topic publish error: {exc}")
            except (ValidationError, Exception) as exc:
                print(f"❌ Ошибка обработки сообщения offset={message.offset}: {exc}")
                save_failed_message(raw_data, str(exc))

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
