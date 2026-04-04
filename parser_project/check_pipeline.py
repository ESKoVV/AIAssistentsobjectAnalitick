from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from kafka import KafkaAdminClient

from config import load_config, validate_consumer_config, validate_db_config


@dataclass(frozen=True)
class PipelineStats:
    row_counts: dict[str, int]
    rows_by_source_type: list[tuple[str, int]]
    latest_documents: list[tuple[str, str, datetime, str]]
    latest_rankings: list[tuple[str, datetime, int]]


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
    step = "Сбор статистики по production pipeline tables"
    try:
        with conn.cursor() as cur:
            row_counts: dict[str, int] = {}
            for table_name in (
                "raw_messages",
                "normalized_messages",
                "document_sentiments",
                "embeddings",
                "clusters",
                "cluster_descriptions",
                "rankings",
            ):
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
                exists = bool(cur.fetchone()[0])
                if not exists:
                    row_counts[table_name] = 0
                    continue
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                row_counts[table_name] = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT source_type, COUNT(*) AS rows_count
                FROM normalized_messages
                GROUP BY source_type
                ORDER BY rows_count DESC, source_type ASC;
                """
            )
            rows_by_source_type = cur.fetchall()

            cur.execute(
                """
                SELECT doc_id, source_type, created_at, LEFT(text, 120)
                FROM normalized_messages
                ORDER BY created_at DESC
                LIMIT 5;
                """
            )
            latest_documents = cur.fetchall()

            latest_rankings: list[tuple[str, datetime, int]] = []
            if row_counts.get("rankings", 0) > 0:
                cur.execute(
                    """
                    SELECT ranking_id, computed_at, period_hours
                    FROM rankings
                    ORDER BY computed_at DESC
                    LIMIT 5;
                    """
                )
                latest_rankings = cur.fetchall()

        _ok("Статистика по таблице собрана")
        return PipelineStats(
            row_counts=row_counts,
            rows_by_source_type=rows_by_source_type,
            latest_documents=latest_documents,
            latest_rankings=latest_rankings,
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
    print("\n[INFO] Row counts by table:")
    for table_name in (
        "raw_messages",
        "normalized_messages",
        "document_sentiments",
        "embeddings",
        "clusters",
        "cluster_descriptions",
        "rankings",
    ):
        _info(f"{table_name}: {stats.row_counts.get(table_name, 0)}")

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

    print("\n[INFO] Последние ranking snapshots:")
    if not stats.latest_rankings:
        print("  - ranking snapshots пока нет")
    else:
        for ranking_id, computed_at, period_hours in stats.latest_rankings:
            print(f"  - {computed_at.isoformat()} | period={period_hours}h | {ranking_id}")

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
        _check_table_exists(conn, "raw_messages")
        _check_table_exists(conn, "normalized_messages")
        stats = _collect_db_stats(conn)
    finally:
        conn.close()

    _check_kafka(config.kafka_bootstrap_servers, config.kafka_raw_topic)
    _check_kafka(config.kafka_bootstrap_servers, config.kafka_preprocessed_topic)
    _check_kafka(config.kafka_bootstrap_servers, config.kafka_clusters_updated_topic)
    _check_kafka(config.kafka_bootstrap_servers, config.kafka_descriptions_updated_topic)
    _check_kafka(config.kafka_bootstrap_servers, config.kafka_rankings_updated_topic)
    _print_report(stats)


if __name__ == "__main__":
    main()
