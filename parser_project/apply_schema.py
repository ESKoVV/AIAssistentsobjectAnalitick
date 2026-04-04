from pathlib import Path

import psycopg

from config import load_config, validate_db_config

CONFIG = load_config()
validate_db_config(CONFIG)

SQL_DIR = Path(__file__).resolve().parent / "sql"
REQUIRED_MIGRATIONS = (
    "000_create_raw_messages.sql",
    "001_create_normalized_messages.sql",
    "002_create_document_fingerprints.sql",
    "003_create_ml_results.sql",
    "004_create_documents_with_ml_view.sql",
    "007_rebuild_message_pipeline_contract.sql",
)


with psycopg.connect(CONFIG.database_url) as conn:
    with conn.cursor() as cur:
        sql_files = sorted(SQL_DIR.glob("*.sql"))
        sql_names = {sql_file.name for sql_file in sql_files}
        missing = [name for name in REQUIRED_MIGRATIONS if name not in sql_names]
        if missing:
            missing_list = ", ".join(missing)
            raise FileNotFoundError(f"Отсутствуют обязательные SQL-миграции: {missing_list}")

        for sql_file in sql_files:
            cur.execute(sql_file.read_text(encoding="utf-8"))
    conn.commit()

print("✅ SQL-схема применена: parser_project/sql/*.sql")
