from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from apps.ml.embeddings.serde import deserialize_enriched_document


def test_process_raw_payload_persists_raw_and_normalized_messages_and_is_idempotent(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_RAW_TOPIC", "raw.documents")
    monkeypatch.setenv("KAFKA_PREPROCESSED_TOPIC", "preprocessed.documents")
    monkeypatch.setenv("KAFKA_GROUP_ID", "consumer-test")

    state: dict[str, object] = {
        "raw_messages": {},
        "normalized_messages": {},
        "published": [],
        "projection_updates": [],
    }

    def upsert_raw_message(message):  # type: ignore[no-untyped-def]
        key = (message.source_type.value, message.source_id)
        raw_messages = state["raw_messages"]
        assert isinstance(raw_messages, dict)
        raw_messages[key] = message
        return uuid5(NAMESPACE_URL, f"{key[0]}:{key[1]}")

    def fetch_recent_cleaned_documents(*, exclude_doc_id=None):  # type: ignore[no-untyped-def]
        del exclude_doc_id
        return []

    def update_preprocessing_projection(documents):  # type: ignore[no-untyped-def]
        projection_updates = state["projection_updates"]
        assert isinstance(projection_updates, list)
        projection_updates.extend(documents)

    def upsert_normalized_message(*, raw_message_id, document):  # type: ignore[no-untyped-def]
        normalized_messages = state["normalized_messages"]
        assert isinstance(normalized_messages, dict)
        normalized_messages[document.doc_id] = {
            "raw_message_id": raw_message_id,
            "document": document,
        }

    def publish(document_payload):  # type: ignore[no-untyped-def]
        published = state["published"]
        assert isinstance(published, list)
        published.append(document_payload)

    fake_db = types.SimpleNamespace(
        fetch_recent_cleaned_documents=fetch_recent_cleaned_documents,
        update_preprocessing_projection=update_preprocessing_projection,
        upsert_normalized_message=upsert_normalized_message,
        upsert_raw_message=upsert_raw_message,
    )
    fake_kafka_producer = types.SimpleNamespace(
        send_document_to_preprocessed_topic=publish,
    )
    fake_kafka = types.SimpleNamespace(KafkaConsumer=object)

    monkeypatch.setitem(sys.modules, "db", fake_db)
    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)
    monkeypatch.setitem(sys.modules, "kafka_producer", fake_kafka_producer)
    sys.modules.pop("consumer", None)

    consumer = importlib.import_module("consumer")
    raw_payload = {
        "source_type": "vk_post",
        "source_id": "-123_456",
        "author_id": "42",
        "text": "Жители пишут об аварии на теплосети",
        "media_type": "text",
        "created_at_utc": datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc).isoformat(),
        "collected_at": datetime(2026, 4, 2, 9, 5, tzinfo=timezone.utc).isoformat(),
        "raw_payload": {
            "id": 456,
            "owner_id": -123,
            "from_id": 42,
            "date": int(datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc).timestamp()),
            "text": "Жители пишут об аварии на теплосети",
            "views": {"count": 250},
            "likes": {"count": 4},
            "reposts": {"count": 1},
            "comments": {"count": 2},
        },
        "is_official": False,
        "reach": 250,
        "likes": 4,
        "reposts": 1,
        "comments_count": 2,
        "parent_id": None,
    }
    source_registry = {
        "vk_post": {
            "source": "vk",
            "entity_type": "post",
            "timezone": "+00:00",
            "default_region": "Волгоградская область",
            "default_region_id": "ru-vgg",
            "default_municipality_id": "volgograd",
        },
    }

    first_doc_id = consumer.process_raw_payload(raw_payload, source_registry)
    second_doc_id = consumer.process_raw_payload(raw_payload, source_registry)
    expected_raw_message_id = uuid5(NAMESPACE_URL, "vk_post:-123_456")

    raw_messages = state["raw_messages"]
    normalized_messages = state["normalized_messages"]
    published = state["published"]
    assert isinstance(raw_messages, dict)
    assert isinstance(normalized_messages, dict)
    assert isinstance(published, list)

    assert first_doc_id == second_doc_id == "vk_post:-123_456"
    assert len(raw_messages) == 1
    assert len(normalized_messages) == 1
    assert len(published) == 2

    saved = normalized_messages[first_doc_id]
    assert isinstance(saved, dict)
    assert saved["raw_message_id"] == expected_raw_message_id

    published_document = deserialize_enriched_document(published[-1])
    assert published_document.doc_id == first_doc_id
    assert published_document.source_id == "-123_456"
    assert published_document.filter_status.value == "pass"
