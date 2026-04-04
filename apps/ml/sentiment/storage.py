from __future__ import annotations

import json
from typing import Protocol, Sequence

from .schema import DocumentSentiment


CREATE_DOCUMENT_SENTIMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS document_sentiments (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_messages (doc_id) ON DELETE CASCADE,
    sentiment_score DOUBLE PRECISION NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_result JSONB NOT NULL DEFAULT '{}'::jsonb
)
"""

CREATE_DOCUMENT_SENTIMENTS_INDEXES_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_document_sentiments_processed_at ON document_sentiments (processed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_document_sentiments_score ON document_sentiments (sentiment_score)",
)


class SentimentRepositoryProtocol(Protocol):
    def ensure_schema(self) -> None:
        ...

    def upsert_document_sentiments(self, sentiments: Sequence[DocumentSentiment]) -> None:
        ...


class InMemorySentimentRepository:
    def __init__(self) -> None:
        self.sentiments: dict[str, DocumentSentiment] = {}

    def ensure_schema(self) -> None:
        return None

    def upsert_document_sentiments(self, sentiments: Sequence[DocumentSentiment]) -> None:
        for sentiment in sentiments:
            self.sentiments[sentiment.doc_id] = sentiment


class PostgresSentimentRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_DOCUMENT_SENTIMENTS_TABLE_SQL)
                for statement in CREATE_DOCUMENT_SENTIMENTS_INDEXES_SQL:
                    cursor.execute(statement)
            connection.commit()

    def upsert_document_sentiments(self, sentiments: Sequence[DocumentSentiment]) -> None:
        if not sentiments:
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_DOCUMENT_SENTIMENTS_TABLE_SQL)
                cursor.executemany(
                    """
                    INSERT INTO document_sentiments (
                        doc_id,
                        sentiment_score,
                        model_name,
                        model_version,
                        processed_at,
                        raw_result
                    ) VALUES (%(doc_id)s, %(sentiment_score)s, %(model_name)s, %(model_version)s, %(processed_at)s, %(raw_result)s::jsonb)
                    ON CONFLICT (doc_id) DO UPDATE SET
                        sentiment_score = EXCLUDED.sentiment_score,
                        model_name = EXCLUDED.model_name,
                        model_version = EXCLUDED.model_version,
                        processed_at = EXCLUDED.processed_at,
                        raw_result = EXCLUDED.raw_result
                    """,
                    [
                        {
                            "doc_id": item.doc_id,
                            "sentiment_score": item.sentiment_score,
                            "model_name": item.model_name,
                            "model_version": item.model_version,
                            "processed_at": item.processed_at,
                            "raw_result": json.dumps(item.raw_result, ensure_ascii=False, default=str),
                        }
                        for item in sentiments
                    ],
                )
                for statement in CREATE_DOCUMENT_SENTIMENTS_INDEXES_SQL:
                    cursor.execute(statement)
            connection.commit()

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresSentimentRepository requires 'psycopg[binary]' to be installed",
            ) from exc
        return psycopg.connect(self._dsn)
