import json
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Generator, Iterable, Mapping, Optional, Sequence
from uuid import UUID

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.preprocessing.cleaning import CleanedDocument
from apps.preprocessing.deduplication import DeduplicatedDocument
from apps.preprocessing.enrichment import EnrichedDocument
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.normalization import MediaType as NormalizedMediaType
from apps.preprocessing.normalization import SourceType as NormalizedSourceType
from config import load_config, validate_db_config
from schema import RawMessage

CONFIG = load_config()


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


def upsert_raw_message(message: RawMessage) -> UUID:
    query = """
    INSERT INTO raw_messages (
        source_type,
        source_id,
        author_id,
        text,
        media_type,
        created_at_utc,
        collected_at,
        raw_payload,
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
        %(media_type)s,
        %(created_at_utc)s,
        %(collected_at)s,
        %(raw_payload)s::jsonb,
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
        media_type = EXCLUDED.media_type,
        created_at_utc = EXCLUDED.created_at_utc,
        collected_at = EXCLUDED.collected_at,
        raw_payload = EXCLUDED.raw_payload,
        is_official = EXCLUDED.is_official,
        reach = EXCLUDED.reach,
        likes = EXCLUDED.likes,
        reposts = EXCLUDED.reposts,
        comments_count = EXCLUDED.comments_count,
        parent_id = EXCLUDED.parent_id
    RETURNING id
    """
    payload = message.model_dump(mode="json")
    payload["raw_payload"] = _serialize_jsonb(payload.get("raw_payload"))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
            row = cur.fetchone()
    if row is None:
        raise RuntimeError("raw_messages upsert did not return id")
    return UUID(str(row[0]))


def find_raw_message_id(*, source_type: Any, source_id: str) -> UUID | None:
    query = """
    SELECT id
    FROM raw_messages
    WHERE source_type = %(source_type)s
      AND source_id = %(source_id)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "source_type": _enum_value(source_type),
                    "source_id": source_id,
                },
            )
            row = cur.fetchone()
    if row is None:
        return None
    return UUID(str(row[0]))


def fetch_recent_cleaned_documents(
    *,
    limit: int = 300,
    exclude_doc_id: str | None = None,
) -> list[CleanedDocument]:
    query = """
        SELECT doc_id, source_type, source_id, parent_id, text, media_type, created_at, collected_at,
               author_id, is_official, reach, likes, reposts, comments_count, region_hint, geo_lat,
               geo_lon, raw_payload, language, language_confidence, is_supported_language, filter_status,
               filter_reasons, quality_weight, anomaly_flags, anomaly_confidence, normalized_text,
               token_count, cleanup_flags
        FROM normalized_messages
        WHERE filter_status <> 'drop'
          AND normalized_text <> ''
    """
    params: dict[str, Any] = {"limit": max(limit, 1)}
    if exclude_doc_id:
        query += " AND doc_id <> %(exclude_doc_id)s"
        params["exclude_doc_id"] = exclude_doc_id
    query += " ORDER BY created_at DESC, doc_id DESC LIMIT %(limit)s"

    with get_connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    documents = [_row_to_cleaned_document(row) for row in rows]
    documents.reverse()
    return documents


def upsert_normalized_message(
    *,
    raw_message_id: UUID,
    document: EnrichedDocument,
) -> None:
    query = """
    INSERT INTO normalized_messages (
        raw_message_id,
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
        raw_payload,
        language,
        language_confidence,
        is_supported_language,
        filter_status,
        filter_reasons,
        quality_weight,
        anomaly_flags,
        anomaly_confidence,
        normalized_text,
        token_count,
        cleanup_flags,
        text_sha256,
        duplicate_group_id,
        near_duplicate_flag,
        duplicate_cluster_size,
        canonical_doc_id,
        region_id,
        municipality_id,
        geo_confidence,
        geo_source,
        geo_evidence,
        engagement,
        metadata_version
    ) VALUES (
        %(raw_message_id)s,
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
        %(raw_payload)s::jsonb,
        %(language)s,
        %(language_confidence)s,
        %(is_supported_language)s,
        %(filter_status)s,
        %(filter_reasons)s,
        %(quality_weight)s,
        %(anomaly_flags)s,
        %(anomaly_confidence)s,
        %(normalized_text)s,
        %(token_count)s,
        %(cleanup_flags)s,
        %(text_sha256)s,
        %(duplicate_group_id)s,
        %(near_duplicate_flag)s,
        %(duplicate_cluster_size)s,
        %(canonical_doc_id)s,
        %(region_id)s,
        %(municipality_id)s,
        %(geo_confidence)s,
        %(geo_source)s,
        %(geo_evidence)s,
        %(engagement)s,
        %(metadata_version)s
    )
    ON CONFLICT (doc_id) DO UPDATE
    SET
        raw_message_id = EXCLUDED.raw_message_id,
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
        raw_payload = EXCLUDED.raw_payload,
        language = EXCLUDED.language,
        language_confidence = EXCLUDED.language_confidence,
        is_supported_language = EXCLUDED.is_supported_language,
        filter_status = EXCLUDED.filter_status,
        filter_reasons = EXCLUDED.filter_reasons,
        quality_weight = EXCLUDED.quality_weight,
        anomaly_flags = EXCLUDED.anomaly_flags,
        anomaly_confidence = EXCLUDED.anomaly_confidence,
        normalized_text = EXCLUDED.normalized_text,
        token_count = EXCLUDED.token_count,
        cleanup_flags = EXCLUDED.cleanup_flags,
        text_sha256 = EXCLUDED.text_sha256,
        duplicate_group_id = EXCLUDED.duplicate_group_id,
        near_duplicate_flag = EXCLUDED.near_duplicate_flag,
        duplicate_cluster_size = EXCLUDED.duplicate_cluster_size,
        canonical_doc_id = EXCLUDED.canonical_doc_id,
        region_id = EXCLUDED.region_id,
        municipality_id = EXCLUDED.municipality_id,
        geo_confidence = EXCLUDED.geo_confidence,
        geo_source = EXCLUDED.geo_source,
        geo_evidence = EXCLUDED.geo_evidence,
        engagement = EXCLUDED.engagement,
        metadata_version = EXCLUDED.metadata_version,
        updated_at = NOW()
    """
    payload = _normalized_message_payload(document, raw_message_id=raw_message_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
            _upsert_document_fingerprint(cur, document)


def update_preprocessing_projection(documents: Sequence[DeduplicatedDocument]) -> None:
    if not documents:
        return

    query = """
        UPDATE normalized_messages
        SET
            filter_status = %(filter_status)s,
            quality_weight = %(quality_weight)s,
            anomaly_flags = %(anomaly_flags)s,
            anomaly_confidence = %(anomaly_confidence)s,
            text_sha256 = %(text_sha256)s,
            duplicate_group_id = %(duplicate_group_id)s,
            near_duplicate_flag = %(near_duplicate_flag)s,
            duplicate_cluster_size = %(duplicate_cluster_size)s,
            canonical_doc_id = %(canonical_doc_id)s,
            updated_at = NOW()
        WHERE doc_id = %(doc_id)s
    """
    payloads = [_projection_payload(document) for document in documents]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, payloads)
            for document in documents:
                _upsert_document_fingerprint(cur, document)


def _projection_payload(document: DeduplicatedDocument) -> dict[str, Any]:
    return {
        "doc_id": document.doc_id,
        "filter_status": _enum_value(document.filter_status),
        "quality_weight": float(document.quality_weight),
        "anomaly_flags": list(document.anomaly_flags),
        "anomaly_confidence": float(document.anomaly_confidence),
        "text_sha256": str(document.text_sha256),
        "duplicate_group_id": str(document.duplicate_group_id),
        "near_duplicate_flag": bool(document.near_duplicate_flag),
        "duplicate_cluster_size": int(document.duplicate_cluster_size),
        "canonical_doc_id": str(document.canonical_doc_id),
    }


def _normalized_message_payload(document: EnrichedDocument, *, raw_message_id: UUID) -> dict[str, Any]:
    payload = asdict(document)
    payload["raw_message_id"] = raw_message_id
    payload["source_type"] = _enum_value(payload["source_type"])
    payload["media_type"] = _enum_value(payload["media_type"])
    payload["filter_status"] = _enum_value(payload["filter_status"])
    payload["raw_payload"] = _serialize_jsonb(payload["raw_payload"])
    payload["filter_reasons"] = list(payload["filter_reasons"])
    payload["anomaly_flags"] = list(payload["anomaly_flags"])
    payload["cleanup_flags"] = list(payload["cleanup_flags"])
    payload["geo_evidence"] = list(payload["geo_evidence"])
    return payload


def _upsert_document_fingerprint(
    cur: psycopg.Cursor,
    document: DeduplicatedDocument | EnrichedDocument,
) -> None:
    normalized_text = str(document.normalized_text or document.text or "")
    normalized_title = _normalize_text(extract_title(document.raw_payload)) or None
    text_fingerprint = compute_text_fingerprint(normalized_text or document.text)
    duplicate_of = document.canonical_doc_id if document.canonical_doc_id != document.doc_id else None
    dedup_method = _resolve_dedup_method(document)
    cur.execute(
        """
        INSERT INTO document_fingerprints (
            doc_id,
            text_fingerprint,
            normalized_title,
            normalized_text,
            duplicate_of,
            cluster_id,
            dedup_method
        ) VALUES (
            %(doc_id)s,
            %(text_fingerprint)s,
            %(normalized_title)s,
            %(normalized_text)s,
            %(duplicate_of)s,
            %(cluster_id)s,
            %(dedup_method)s
        )
        ON CONFLICT (doc_id) DO UPDATE
        SET
            text_fingerprint = EXCLUDED.text_fingerprint,
            normalized_title = EXCLUDED.normalized_title,
            normalized_text = EXCLUDED.normalized_text,
            duplicate_of = EXCLUDED.duplicate_of,
            cluster_id = EXCLUDED.cluster_id,
            dedup_method = EXCLUDED.dedup_method,
            updated_at = NOW()
        """,
        {
            "doc_id": document.doc_id,
            "text_fingerprint": text_fingerprint,
            "normalized_title": normalized_title,
            "normalized_text": normalized_text,
            "duplicate_of": duplicate_of,
            "cluster_id": document.duplicate_group_id,
            "dedup_method": dedup_method,
        },
    )


def _resolve_dedup_method(document: DeduplicatedDocument | EnrichedDocument) -> str:
    if document.near_duplicate_flag:
        return "near_duplicate"
    if document.canonical_doc_id != document.doc_id:
        return "exact_duplicate"
    return "none"


def compute_text_fingerprint(text: str) -> str:
    import hashlib

    normalized_text = _normalize_text(text)
    return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(str(value).casefold().split())


def extract_title(raw_payload: Mapping[str, Any] | None) -> Optional[str]:
    payload = dict(raw_payload or {})
    for key in ("title", "headline", "subject"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _row_to_cleaned_document(row: Mapping[str, Any]) -> CleanedDocument:
    return CleanedDocument(
        doc_id=str(row["doc_id"]),
        source_type=NormalizedSourceType(str(row["source_type"])),
        source_id=str(row["source_id"]),
        parent_id=str(row["parent_id"]) if row["parent_id"] is not None else None,
        text=str(row["text"]),
        media_type=NormalizedMediaType(str(row["media_type"])),
        created_at=row["created_at"],
        collected_at=row["collected_at"],
        author_id=str(row["author_id"]),
        is_official=bool(row["is_official"]),
        reach=int(row["reach"]),
        likes=int(row["likes"]),
        reposts=int(row["reposts"]),
        comments_count=int(row["comments_count"]),
        region_hint=str(row["region_hint"]) if row["region_hint"] is not None else None,
        geo_lat=float(row["geo_lat"]) if row["geo_lat"] is not None else None,
        geo_lon=float(row["geo_lon"]) if row["geo_lon"] is not None else None,
        raw_payload=dict(row["raw_payload"]),
        language=str(row["language"]),
        language_confidence=float(row["language_confidence"]),
        is_supported_language=bool(row["is_supported_language"]),
        filter_status=FilterStatus(str(row["filter_status"])),
        filter_reasons=tuple(str(item) for item in row["filter_reasons"] or ()),
        quality_weight=float(row["quality_weight"]),
        anomaly_flags=tuple(str(item) for item in row["anomaly_flags"] or ()),
        anomaly_confidence=float(row["anomaly_confidence"]),
        normalized_text=str(row["normalized_text"]),
        token_count=int(row["token_count"]),
        cleanup_flags=tuple(str(item) for item in row["cleanup_flags"] or ()),
    )


def _serialize_jsonb(value: Optional[dict]) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str)


def _enum_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


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
        processed_at = NOW()
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
