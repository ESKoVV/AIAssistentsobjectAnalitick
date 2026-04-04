from pathlib import Path

import psycopg

from config import load_config, validate_db_config

CONFIG = load_config()
validate_db_config(CONFIG)

SQL_DIR = Path(__file__).resolve().parent / "sql"


with psycopg.connect(CONFIG.database_url) as conn:
    with conn.cursor() as cur:
        for sql_file in sorted(SQL_DIR.glob("*.sql")):
            cur.execute(sql_file.read_text(encoding="utf-8"))
    conn.commit()

print("✅ SQL-схема применена: parser_project/sql/001..003")
