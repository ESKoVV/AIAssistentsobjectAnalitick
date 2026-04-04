import gzip
import json
from contextlib import contextmanager
from typing import Any, Generator, Optional

import psycopg

from config import load_config, validate_db_config, validate_raw_db_config
from dedup_legacy import upsert_document_fingerprint_legacy
from schema import NormalizedDocument, RawDocument

CONFIG = load_config()

COMPRESSION_CODEC = "gzip"
TEXT_PREVIEW_LIMIT = 1000
RAW_PAYLOAD_PREVIEW_LIMIT = 2000


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    validate_db_config(CONFIG)
    conn = psycopg.connect(CONFIG.database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_raw_connection() -> Generator[psycopg.Connection, None, None]:
    validate_raw_db_config(CONFIG)
    conn = psycopg.connect(CONFIG.raw_database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def compress_text(value: str) -> bytes:
    return gzip.compress(value.encode("utf-8"))


def compress_raw_payload(value: dict[str, Any]) -> bytes:
    raw_json = json.dumps(value, ensure_ascii=False, default=str)
    return gzip.compress(raw_json.encode("utf-8"))


def decompress_text(value: bytes | None, fallback: str = "") -> str:
    if not value:
        return fallback
    return gzip.decompress(value).decode("utf-8")


def decompress_raw_payload(value: bytes | None, fallback: Optional[dict] = None) -> dict[str, Any]:
    if not value:
        return fallback or {}
    raw_json = gzip.decompress(value).decode("utf-8")
    return json.loads(raw_json)


def _serialize_jsonb(value: Optional[dict]) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str)


def _text_preview(text: str) -> str:
    clean = (text or "").strip()
    if len(clean) <= TEXT_PREVIEW_LIMIT:
        return clean
    return clean[:TEXT_PREVIEW_LIMIT]


def _raw_payload_preview(raw_payload: dict[str, Any]) -> dict[str, Any]:
    raw_json = json.dumps(raw_payload or {}, ensure_ascii=False, default=str)
    preview = raw_json[:RAW_PAYLOAD_PREVIEW_LIMIT]
    return {
        "compressed": True,
        "preview": preview,
        "truncated": len(raw_json) > RAW_PAYLOAD_PREVIEW_LIMIT,
    }


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
    INSERT INTO public.raw_messages (
        source_type,
        source_id,
        author_id,
        text,
        text_compressed,
        media_type,
        created_at_utc,
        collected_at,
        raw_payload,
        raw_payload_compressed,
        compression_codec,
        is_official,
        reach,
        likes,
        reposts,
        comments_count,
        parent_id
    ) VALUES (
        %(source_type)s,
        %(source_id)s,
        %(author_id)s,
        %(text)s,
        %(text_compressed)s,
        %(media_type)s,
        %(created_at_utc)s,
        %(collected_at)s,
        %(raw_payload)s::jsonb,
        %(raw_payload_compressed)s,
        %(compression_codec)s,
        %(is_official)s,
        %(reach)s,
        %(likes)s,
        %(reposts)s,
        %(comments_count)s,
        %(parent_id)s
    )
    ON CONFLICT (source_type, source_id) DO UPDATE
    SET
        author_id = EXCLUDED.author_id,
        text = EXCLUDED.text,
        text_compressed = EXCLUDED.text_compressed,
        media_type = EXCLUDED.media_type,
        created_at_utc = EXCLUDED.created_at_utc,
        collected_at = EXCLUDED.collected_at,
        raw_payload = EXCLUDED.raw_payload,
        raw_payload_compressed = EXCLUDED.raw_payload_compressed,
        compression_codec = EXCLUDED.compression_codec,
        is_official = EXCLUDED.is_official,
        reach = EXCLUDED.reach,
        likes = EXCLUDED.likes,
        reposts = EXCLUDED.reposts,
        comments_count = EXCLUDED.comments_count,
        parent_id = EXCLUDED.parent_id;
    """

    payload = {
        "source_type": document.source_type,
        "source_id": document.source_id,
        "author_id": document.author_raw,
        "text": _text_preview(document.text_raw),
        "text_compressed": compress_text(document.text_raw),
        "media_type": document.media_type,
        "created_at_utc": document.created_at,
        "collected_at": document.collected_at,
        "raw_payload": _serialize_jsonb(_raw_payload_preview(document.raw_payload)),
        "raw_payload_compressed": compress_raw_payload(document.raw_payload),
        "compression_codec": COMPRESSION_CODEC,
        "is_official": document.is_official,
        "reach": document.reach,
        "likes": document.likes,
        "reposts": document.reposts,
        "comments_count": document.comments_count,
        "parent_id": document.parent_source_id,
    }

    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)


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
