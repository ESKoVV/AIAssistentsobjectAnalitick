import psycopg

from config import load_config, validate_db_config

CONFIG = load_config()
validate_db_config(CONFIG)

with psycopg.connect(CONFIG.database_url) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_messages;")
        print("Всего raw_messages:", cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM normalized_messages;")
        print("Всего normalized_messages:", cur.fetchone()[0])

        cur.execute("""
            SELECT source_type, text
            FROM normalized_messages
            ORDER BY created_at DESC
            LIMIT 5;
        """)

        for row in cur.fetchall():
            print(row)
