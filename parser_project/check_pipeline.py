from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from kafka import KafkaAdminClient

from config import load_config, validate_consumer_config, validate_db_config


@dataclass(frozen=True)
class PipelineStats:
    total_rows: int
    rows_by_source_type: list[tuple[str, int]]
    latest_documents: list[tuple[str, str, datetime, str]]


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _info(message: str) -> None:
    print(f"[INFO] {message}")


def _fail(step: str, error: Exception) -> None:
    print(f"[ERROR] Шаг '{step}' завершился с ошибкой: {error}")
    raise SystemExit(1) from error


def _check_postgres(database_url: str) -> psycopg.Connection[Any]:
    step = "Проверка доступности PostgreSQL"
    try:
        conn = psycopg.connect(database_url)
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        _ok("PostgreSQL доступен")
        return conn
    except Exception as error:  # noqa: BLE001
        _fail(step, error)


def _check_table_exists(conn: psycopg.Connection[Any], table_name: str) -> None:
    step = f"Проверка наличия таблицы {table_name}"
    try:
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
            exists = cur.fetchone()[0]

        if not exists:
            raise RuntimeError(f"Таблица {table_name} не найдена в схеме public")

        _ok(f"Таблица {table_name} найдена")
    except Exception as error:  # noqa: BLE001
        _fail(step, error)


def _collect_db_stats(conn: psycopg.Connection[Any]) -> PipelineStats:
    step = "Сбор статистики по normalized_documents"
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM normalized_documents;")
            total_rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT source_type, COUNT(*) AS rows_count
                FROM normalized_documents
                GROUP BY source_type
                ORDER BY rows_count DESC, source_type ASC;
                """
            )
            rows_by_source_type = cur.fetchall()

            cur.execute(
                """
                SELECT doc_id, source_type, created_at, LEFT(text, 120)
                FROM normalized_documents
                ORDER BY created_at DESC
                LIMIT 5;
                """
            )
            latest_documents = cur.fetchall()

        _ok("Статистика по таблице собрана")
        return PipelineStats(
            total_rows=total_rows,
            rows_by_source_type=rows_by_source_type,
            latest_documents=latest_documents,
        )
    except Exception as error:  # noqa: BLE001
        _fail(step, error)


def _check_kafka(bootstrap_servers: str, topic_name: str) -> None:
    step_connect = "Проверка доступности Kafka"
    step_topic = f"Проверка наличия Kafka topic {topic_name}"

    client = None
    try:
        client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        _ok("Kafka доступен")
    except Exception as error:  # noqa: BLE001
        _fail(step_connect, error)

    try:
        topics = client.list_topics()
        if topic_name not in topics:
            raise RuntimeError(f"Topic '{topic_name}' отсутствует")
        _ok(f"Kafka topic '{topic_name}' найден")
    except Exception as error:  # noqa: BLE001
        _fail(step_topic, error)
    finally:
        if client is not None:
            client.close()


def _print_report(stats: PipelineStats) -> None:
    print("\n================ Pipeline health report ================")
    _info(f"Всего записей в normalized_documents: {stats.total_rows}")

    print("\n[INFO] Записи по source_type:")
    if not stats.rows_by_source_type:
        print("  - данных пока нет")
    else:
        for source_type, count in stats.rows_by_source_type:
            print(f"  - {source_type}: {count}")

    print("\n[INFO] Последние 5 документов:")
    if not stats.latest_documents:
        print("  - документов пока нет")
    else:
        for doc_id, source_type, created_at, text_preview in stats.latest_documents:
            preview = (text_preview or "").replace("\n", " ").strip()
            print(f"  - {created_at.isoformat()} | {source_type} | {doc_id} | {preview}")

    print("========================================================")


def main() -> None:
    config = load_config()

    try:
        validate_db_config(config)
        validate_consumer_config(config)
    except Exception as error:  # noqa: BLE001
        _fail("Проверка обязательных переменных окружения", error)

    conn = _check_postgres(config.database_url)
    try:
        _check_table_exists(conn, "normalized_documents")
        stats = _collect_db_stats(conn)
    finally:
        conn.close()

    _check_kafka(config.kafka_bootstrap_servers, config.kafka_topic)
    _print_report(stats)


if __name__ == "__main__":
    main()
