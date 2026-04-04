from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg
from kafka import KafkaConsumer, KafkaProducer

from config import load_config, validate_consumer_config, validate_db_config
from db import upsert_raw_document
from schema import RawDocument


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _info(message: str) -> None:
    print(f"[INFO] {message}")


def _fail(message: str) -> None:
    print(f"[ERROR] {message}")
    raise SystemExit(1)


def _exists(conn: psycopg.Connection[Any], table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
            );
            """,
            (table_name,),
        )
        return bool(cur.fetchone()[0])


def _count_raw_documents(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_documents;")
        return int(cur.fetchone()[0])


def _build_raw_message() -> dict[str, Any]:
    unique_source_id = f"smoke-{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    return {
        "doc_id": f"rss_article:{unique_source_id}",
        "source_type": "rss_article",
        "source_id": unique_source_id,
        "parent_source_id": None,
        "text_raw": "Smoke raw ingestion message",
        "title_raw": "Smoke check",
        "author_raw": "pipeline-check",
        "created_at_raw": now.isoformat(),
        "created_at": now.isoformat(),
        "collected_at": now.isoformat(),
        "source_url": "https://example.com/smoke",
        "source_domain": "example.com",
        "region_hint_raw": "test-region",
        "geo_raw": None,
        "engagement_raw": {"likes": 0, "comments": 0, "reposts": 0, "views": 1},
        "raw_payload": {"smoke": True},
    }


def _produce_raw_message(config, raw_message: dict[str, Any]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        acks="all",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
    )
    producer.send(config.kafka_raw_topic, raw_message).get(timeout=30)
    producer.flush()
    producer.close()


def _consume_one_raw_message(config, expected_doc_id: str) -> RawDocument:
    consumer = KafkaConsumer(
        config.kafka_raw_topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"{config.kafka_group_id}-smoke-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda msg: json.loads(msg.decode("utf-8")),
    )

    deadline = time.time() + 30
    try:
        while time.time() < deadline:
            records = consumer.poll(timeout_ms=2000)
            for _tp, batch in records.items():
                for record in batch:
                    raw_data = record.value
                    if raw_data.get("doc_id") != expected_doc_id:
                        continue
                    document = RawDocument.model_validate(raw_data)
                    consumer.commit()
                    return document
        raise TimeoutError(f"Не удалось прочитать сообщение doc_id={expected_doc_id} из Kafka")
    finally:
        consumer.close()


def main() -> None:
    config = load_config()

    try:
        validate_db_config(config)
        validate_consumer_config(config)
    except Exception as error:  # noqa: BLE001
        _fail(f"Ошибка конфигурации: {error}")

    raw_message = _build_raw_message()

    try:
        with psycopg.connect(config.database_url) as conn:
            if not _exists(conn, "raw_documents"):
                _fail("Отсутствует таблица raw_documents. Проверьте миграцию 005_create_raw_documents.sql")

            before_count = _count_raw_documents(conn)
            _info(f"raw_documents до smoke-check: {before_count}")

            _produce_raw_message(config, raw_message)
            _ok(f"Producer отправил raw сообщение doc_id={raw_message['doc_id']}")

            consumed_doc = _consume_one_raw_message(config, expected_doc_id=raw_message["doc_id"])
            _ok(f"Consumer прочитал raw сообщение doc_id={consumed_doc.doc_id}")

            upsert_raw_document(consumed_doc)
            _ok(f"Upsert в raw_documents выполнен doc_id={consumed_doc.doc_id}")

            after_count = _count_raw_documents(conn)
            _info(f"raw_documents после smoke-check: {after_count}")

            if after_count <= before_count:
                _fail("raw_documents не пополнилась после raw smoke-check")

            _ok("Raw smoke-check успешен: producer -> consumer -> raw_documents")

    except SystemExit:
        raise
    except Exception as error:  # noqa: BLE001
        _fail(f"Ошибка raw smoke-check pipeline: {error}")


if __name__ == "__main__":
    main()
