import hashlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from difflib import SequenceMatcher
from typing import Generator, Optional, Protocol

import psycopg
from dotenv import load_dotenv

from schema import NormalizedDocument

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Add it to .env before running consumer.")


class DuplicateCandidate(Protocol):
    doc_id: str
    duplicate_of: Optional[str]
    cluster_id: Optional[str]


class DeduplicationStrategy(Protocol):
    """Strategy interface to keep room for stronger semantic deduplication."""

    def find_candidate(
        self,
        cur: psycopg.Cursor,
        *,
        doc_id: str,
        text_fingerprint: str,
        normalized_title: Optional[str],
        normalized_text: str,
    ) -> Optional[dict]:
        ...


class FingerprintSimilarityDeduplicator:
    """Rule-based deduplication using fingerprint and lexical similarity."""

    MAX_CANDIDATES = 300
    TITLE_THRESHOLD = 0.92
    TEXT_THRESHOLD = 0.97
    COMBINED_THRESHOLD = 0.94

    def find_candidate(
        self,
        cur: psycopg.Cursor,
        *,
        doc_id: str,
        text_fingerprint: str,
        normalized_title: Optional[str],
        normalized_text: str,
    ) -> Optional[dict]:
        cur.execute(
            """
            SELECT doc_id, duplicate_of, cluster_id
            FROM document_fingerprints
            WHERE text_fingerprint = %(text_fingerprint)s
              AND doc_id <> %(doc_id)s
            ORDER BY updated_at DESC
            LIMIT 1;
            """,
            {"text_fingerprint": text_fingerprint, "doc_id": doc_id},
        )
        exact_match = cur.fetchone()
        if exact_match:
            return {
                "doc_id": exact_match[0],
                "duplicate_of": exact_match[1],
                "cluster_id": exact_match[2],
                "reason": "fingerprint_match",
            }

        cur.execute(
            """
            SELECT doc_id, duplicate_of, cluster_id, normalized_title, normalized_text
            FROM document_fingerprints
            WHERE doc_id <> %(doc_id)s
            ORDER BY updated_at DESC
            LIMIT %(limit)s;
            """,
            {"doc_id": doc_id, "limit": self.MAX_CANDIDATES},
        )

        best_candidate = None
        best_score = 0.0

        for row in cur.fetchall():
            candidate_title = row[3]
            candidate_text = row[4]

            title_similarity = similarity(normalized_title, candidate_title)
            text_similarity = similarity(normalized_text, candidate_text)
            combined_similarity = max(title_similarity, text_similarity)

            is_duplicate = (
                title_similarity >= self.TITLE_THRESHOLD
                or text_similarity >= self.TEXT_THRESHOLD
                or combined_similarity >= self.COMBINED_THRESHOLD
            )

            if is_duplicate and combined_similarity > best_score:
                best_score = combined_similarity
                best_candidate = {
                    "doc_id": row[0],
                    "duplicate_of": row[1],
                    "cluster_id": row[2],
                    "reason": "similarity_match",
                }

        return best_candidate


DEDUPLICATION_STRATEGY: DeduplicationStrategy = FingerprintSimilarityDeduplicator()


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


def normalize_for_fingerprint(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = value.casefold()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[^\w\s]", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def compute_text_fingerprint(text: str) -> str:
    normalized_text = normalize_for_fingerprint(text)
    return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()


def similarity(left: Optional[str], right: Optional[str]) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(a=left, b=right).ratio()


def extract_title(document: NormalizedDocument) -> Optional[str]:
    raw_payload = document.raw_payload or {}
    for key in ("title", "headline", "subject"):
        value = raw_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _resolve_duplicate_link(candidate: Optional[dict], doc_id: str) -> tuple[Optional[str], str]:
    if not candidate:
        return None, str(uuid.uuid4())

    canonical_doc_id = candidate.get("duplicate_of") or candidate["doc_id"]
    cluster_id = candidate.get("cluster_id") or canonical_doc_id

    if canonical_doc_id == doc_id:
        return None, cluster_id

    return canonical_doc_id, cluster_id


def _upsert_document_fingerprint(cur: psycopg.Cursor, document: NormalizedDocument) -> None:
    normalized_text = normalize_for_fingerprint(document.text)
    normalized_title = normalize_for_fingerprint(extract_title(document)) or None
    text_fingerprint = compute_text_fingerprint(document.text)

    candidate = DEDUPLICATION_STRATEGY.find_candidate(
        cur,
        doc_id=document.doc_id,
        text_fingerprint=text_fingerprint,
        normalized_title=normalized_title,
        normalized_text=normalized_text,
    )
    duplicate_of, cluster_id = _resolve_duplicate_link(candidate, document.doc_id)

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
            updated_at = NOW();
        """,
        {
            "doc_id": document.doc_id,
            "text_fingerprint": text_fingerprint,
            "normalized_title": normalized_title,
            "normalized_text": normalized_text,
            "duplicate_of": duplicate_of,
            "cluster_id": cluster_id,
            "dedup_method": candidate.get("reason") if candidate else "none",
        },
    )


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
            _upsert_document_fingerprint(cur, document)
