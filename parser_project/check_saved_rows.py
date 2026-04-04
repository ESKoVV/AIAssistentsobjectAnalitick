import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

with psycopg.connect(os.getenv("DATABASE_URL")) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM normalized_documents;")
        print("Всего записей:", cur.fetchone()[0])

        cur.execute("""
            SELECT source_type, text
            FROM normalized_documents
            ORDER BY created_at DESC
            LIMIT 5;
        """)

        for row in cur.fetchall():
            print(row)