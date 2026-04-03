import json
import os
from contextlib import contextmanager
from typing import Generator

import psycopg
from dotenv import load_dotenv

from schema import NormalizedDocument

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Add it to .env before running consumer.")


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_document(document: NormalizedDocument) -> None:
    query = """
    INSERT INTO normalized_documents (
        doc_id,
        source_type,
        source_id,
        parent_id,
        text,
        media_type,
        created_at,
        collected_at,
        author_id,
        is_official,
        reach,
        likes,
        reposts,
        comments_count,
        region_hint,
        geo_lat,
        geo_lon,
        raw_payload
    ) VALUES (
        %(doc_id)s,
        %(source_type)s,
        %(source_id)s,
        %(parent_id)s,
        %(text)s,
        %(media_type)s,
        %(created_at)s,
        %(collected_at)s,
        %(author_id)s,
        %(is_official)s,
        %(reach)s,
        %(likes)s,
        %(reposts)s,
        %(comments_count)s,
        %(region_hint)s,
        %(geo_lat)s,
        %(geo_lon)s,
        %(raw_payload)s::jsonb
    )
    ON CONFLICT (doc_id) DO UPDATE
    SET
        source_type = EXCLUDED.source_type,
        source_id = EXCLUDED.source_id,
        parent_id = EXCLUDED.parent_id,
        text = EXCLUDED.text,
        media_type = EXCLUDED.media_type,
        created_at = EXCLUDED.created_at,
        collected_at = EXCLUDED.collected_at,
        author_id = EXCLUDED.author_id,
        is_official = EXCLUDED.is_official,
        reach = EXCLUDED.reach,
        likes = EXCLUDED.likes,
        reposts = EXCLUDED.reposts,
        comments_count = EXCLUDED.comments_count,
        region_hint = EXCLUDED.region_hint,
        geo_lat = EXCLUDED.geo_lat,
        geo_lon = EXCLUDED.geo_lon,
        raw_payload = EXCLUDED.raw_payload;
    """

    payload = document.model_dump(mode="json")
    payload["raw_payload"] = json.dumps(payload["raw_payload"], ensure_ascii=False)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
