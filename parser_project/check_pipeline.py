from __future__ import annotations

from typing import Any

import psycopg

from config import load_config, validate_db_config


REQUIRED_TABLES = (
    "normalized_documents",
    "document_fingerprints",
    "ml_results",
)
REQUIRED_VIEW = "documents_with_ml"


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _info(message: str) -> None:
    print(f"[INFO] {message}")


def _fail(message: str) -> None:
    print(f"[ERROR] {message}")
    raise SystemExit(1)


def _exists(conn: psycopg.Connection[Any], object_type: str, object_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = %s
                  AND table_name = %s
            );
            """,
            (object_type, object_name),
        )
        return bool(cur.fetchone()[0])


def _check_required_objects(conn: psycopg.Connection[Any]) -> None:
    missing_tables = [table for table in REQUIRED_TABLES if not _exists(conn, "BASE TABLE", table)]
    if missing_tables:
        _fail(
            "Отсутствуют обязательные таблицы: "
            + ", ".join(missing_tables)
            + ". Проверьте миграции и этапы ingestion/ml."
        )

    if not _exists(conn, "VIEW", REQUIRED_VIEW):
        _fail(
            f"Отсутствует обязательное представление {REQUIRED_VIEW}. "
            "Проверьте SQL-миграции для витрины с ML-результатами."
        )

    _ok("Все обязательные таблицы и представление найдены")


def _count_rows(conn: psycopg.Connection[Any], table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return int(cur.fetchone()[0])


def _fetch_latest_documents_with_ml(conn: psycopg.Connection[Any], limit: int = 5) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM documents_with_ml
            ORDER BY created_at DESC NULLS LAST
            LIMIT %s;
            """,
            (limit,),
        )
        return list(cur.fetchall())


def _print_documents(rows: list[dict[str, Any]]) -> None:
    print("\n[INFO] Последние 5 записей из documents_with_ml:")
    if not rows:
        print("  - данных пока нет")
        return

    for index, row in enumerate(rows, start=1):
        rendered = ", ".join(f"{key}={value!r}" for key, value in row.items())
        print(f"  {index}. {rendered}")


def main() -> None:
    config = load_config()

    try:
        validate_db_config(config)
    except Exception as error:  # noqa: BLE001
        _fail(f"Ошибка конфигурации: {error}")

    try:
        with psycopg.connect(config.database_url) as conn:
            _ok("Подключение к PostgreSQL установлено")
            _check_required_objects(conn)

            normalized_count = _count_rows(conn, "normalized_documents")
            ml_results_count = _count_rows(conn, "ml_results")

            _info(f"Количество записей в normalized_documents: {normalized_count}")
            _info(f"Количество записей в ml_results: {ml_results_count}")

            latest_rows = _fetch_latest_documents_with_ml(conn, limit=5)
            _print_documents(latest_rows)

    except SystemExit:
        raise
    except Exception as error:  # noqa: BLE001
        _fail(f"Ошибка smoke-check pipeline: {error}")


if __name__ == "__main__":
    main()
