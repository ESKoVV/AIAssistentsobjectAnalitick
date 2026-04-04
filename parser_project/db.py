import json
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg

from config import load_config, validate_db_config
from dedup_legacy import upsert_document_fingerprint_legacy
from schema import NormalizedDocument, RawDocument

CONFIG = load_config()
validate_db_config(CONFIG)


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(CONFIG.database_url)
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
            upsert_document_fingerprint_legacy(cur, document)


def upsert_raw_document(document: RawDocument) -> None:
    query = """
    INSERT INTO raw_documents (
        doc_id,
        source_type,
        source_id,
        parent_source_id,
        text_raw,
        title_raw,
        author_raw,
        created_at_raw,
        created_at,
        collected_at,
        source_url,
        source_domain,
        region_hint_raw,
        geo_raw,
        engagement_raw,
        raw_payload
    ) VALUES (
        %(doc_id)s,
        %(source_type)s,
        %(source_id)s,
        %(parent_source_id)s,
        %(text_raw)s,
        %(title_raw)s,
        %(author_raw)s,
        %(created_at_raw)s,
        %(created_at)s,
        %(collected_at)s,
        %(source_url)s,
        %(source_domain)s,
        %(region_hint_raw)s,
        %(geo_raw)s::jsonb,
        %(engagement_raw)s::jsonb,
        %(raw_payload)s::jsonb
    )
    ON CONFLICT (doc_id) DO UPDATE
    SET
        source_type = EXCLUDED.source_type,
        source_id = EXCLUDED.source_id,
        parent_source_id = EXCLUDED.parent_source_id,
        text_raw = EXCLUDED.text_raw,
        title_raw = EXCLUDED.title_raw,
        author_raw = EXCLUDED.author_raw,
        created_at_raw = EXCLUDED.created_at_raw,
        created_at = EXCLUDED.created_at,
        collected_at = EXCLUDED.collected_at,
        source_url = EXCLUDED.source_url,
        source_domain = EXCLUDED.source_domain,
        region_hint_raw = EXCLUDED.region_hint_raw,
        geo_raw = EXCLUDED.geo_raw,
        engagement_raw = EXCLUDED.engagement_raw,
        raw_payload = EXCLUDED.raw_payload;
    """

    payload = document.model_dump(mode="json")
    payload["geo_raw"] = _serialize_jsonb(payload.get("geo_raw"))
    payload["engagement_raw"] = _serialize_jsonb(payload.get("engagement_raw"))
    payload["raw_payload"] = _serialize_jsonb(payload.get("raw_payload"))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)


def _serialize_jsonb(value: Optional[dict]) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


def save_ml_result(
    *,
    doc_id: str,
    summary: Optional[str],
    score: Optional[float],
    category: Optional[str],
    model_version: Optional[str],
    prompt_version: Optional[str],
    status: str = "processed",
    error_message: Optional[str] = None,
    raw_result: Optional[dict] = None,
) -> None:
    query = """
    INSERT INTO ml_results (
        doc_id,
        summary,
        score,
        category,
        model_version,
        prompt_version,
        status,
        error_message,
        raw_result
    ) VALUES (
        %(doc_id)s,
        %(summary)s,
        %(score)s,
        %(category)s,
        %(model_version)s,
        %(prompt_version)s,
        %(status)s,
        %(error_message)s,
        %(raw_result)s::jsonb
    )
    ON CONFLICT (doc_id) DO UPDATE
    SET
        summary = EXCLUDED.summary,
        score = EXCLUDED.score,
        category = EXCLUDED.category,
        model_version = EXCLUDED.model_version,
        prompt_version = EXCLUDED.prompt_version,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        raw_result = EXCLUDED.raw_result,
        processed_at = NOW();
    """

    payload = {
        "doc_id": doc_id,
        "summary": summary,
        "score": score,
        "category": category,
        "model_version": model_version,
        "prompt_version": prompt_version,
        "status": status,
        "error_message": error_message,
        "raw_result": _serialize_jsonb(raw_result),
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)


def save_ml_error(doc_id: str, error_message: str, raw_result: Optional[dict] = None) -> None:
    save_ml_result(
        doc_id=doc_id,
        summary=None,
        score=None,
        category=None,
        model_version=None,
        prompt_version=None,
        status="error",
        error_message=error_message,
        raw_result=raw_result,
    )
