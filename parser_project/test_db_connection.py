import psycopg

from config import load_config, validate_db_config

CONFIG = load_config()
validate_db_config(CONFIG)

with psycopg.connect(CONFIG.database_url) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
        print(cur.fetchone())
